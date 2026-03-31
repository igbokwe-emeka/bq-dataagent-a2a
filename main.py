"""
BigQuery Data Agent A2A Proxy

Proxies A2A (Agent-to-Agent / JSON-RPC 2.0) requests from Gemini Enterprise
to the BigQuery Data Agent, propagating the user's Entra ID OAuth token so
all queries run under the user's own GCP identity.

Architecture:
    Gemini Enterprise → A2A Proxy (Cloud Run) → BigQuery Data Agent

Streaming:
    method=message/stream  → SSE stream; PROGRESS events forwarded in real time,
                             final artifact sent when agent completes
    method=message/send    → single JSON response with complete answer + charts
"""
import os
import uuid
import time
import json
import base64
import hashlib
import logging
import asyncio
import uvicorn
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import StreamingResponse
from auth import decode_token_claims
from bigquery_client import ask_bigquery, stream_bigquery, reset_user_conversation
from dotenv import load_dotenv

load_dotenv()


# ── Logging ──────────────────────────────────────────────────────────────────

class _CloudRunFormatter(logging.Formatter):
    """Emit one JSON line per record — parsed by Cloud Logging as structured logs."""
    _SEVERITY = {
        logging.DEBUG:    "DEBUG",
        logging.INFO:     "INFO",
        logging.WARNING:  "WARNING",
        logging.ERROR:    "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "severity": self._SEVERITY.get(record.levelno, "DEFAULT"),
            "logger":   record.name,
            "message":  record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_CloudRunFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    root.setLevel(getattr(logging, level, logging.INFO))


_configure_logging()
logger = logging.getLogger("oauth.proxy")

# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="BigQuery Data Agent A2A Proxy")

AGENT_NAME = os.getenv("AGENT_NAME", "bq-dataagent-a2a")
AGENT_DESCRIPTION = os.getenv(
    "AGENT_DESCRIPTION",
    "Query your BigQuery data in plain English with per-user identity propagation."
)


# ── Utility helpers ──────────────────────────────────────────────────────────

def _mask_email(value: str) -> str:
    """Partially mask an email address for safe logging."""
    if "@" in value:
        local, domain = value.split("@", 1)
        return f"{local[:3]}...@{domain}"
    return value[:6] + "..."


def _extract_user_id(claims: dict, token: str) -> tuple[str, str]:
    """
    Derive a stable user identifier from token claims.
    Returns (user_id, source_claim) where source_claim names the field used.
    """
    for claim in ("upn", "preferred_username", "unique_name", "email"):
        value = claims.get(claim)
        if isinstance(value, str) and value:
            return value, claim
    token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
    return token_hash, "token_hash"


def _format_rows(rows: list) -> str:
    if not rows:
        return "(empty result set)"
    headers = list(rows[0].keys())
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    header_row = "| " + " | ".join(headers) + " |"
    data_rows = []
    for row in rows:
        cells = []
        for h in headers:
            v = row.get(h)
            cells.append("" if v is None else str(v))
        data_rows.append("| " + " | ".join(cells) + " |")
    return "\n".join([header_row, sep] + data_rows)


def _artifact_parts(answer: str | None, sql: str | None, rows: list, charts: list) -> list:
    """Build the A2A artifact parts list from BQ result components."""
    parts = []

    if answer:
        parts.append({"kind": "text", "text": answer})

    if sql:
        # Markdown fenced SQL block — renders with syntax highlighting in GE
        parts.append({"kind": "text", "text": f"\n```sql\n{sql}\n```\n"})

    if rows:
        row_count = len(rows)
        preview = rows[:10]
        suffix = f"\n*Showing {len(preview)} of {row_count} rows*" if row_count > 10 else ""
        parts.append({
            "kind": "text",
            "text": f"\n{_format_rows(preview)}{suffix}",
        })

    for chart in charts:
        if chart.get("data"):
            # PNG rendered server-side — GE renders file/image parts inline
            parts.append({
                "kind": "file",
                "file": {
                    "name": "chart.png",
                    "mimeType": chart.get("mime_type", "image/png"),
                    "bytes": base64.b64encode(chart["data"]).decode(),
                },
            })

    logger.info("artifact: total parts=%d kinds=%s has_sql=%s rows=%d charts=%d",
                len(parts), [p["kind"] for p in parts], bool(sql), len(rows), len(charts))
    return parts or [{"kind": "text", "text": "(No answer returned from BigQuery Data Agent)"}]


def _build_a2a_response(body: dict, params: dict, parts: list) -> dict:
    context_id = params.get("message", {}).get("messageId", str(uuid.uuid4()))
    return {
        "jsonrpc": "2.0",
        "id": body.get("id"),
        "result": {
            "id": str(uuid.uuid4()),
            "contextId": context_id,
            "status": {"state": "completed"},
            "artifacts": [
                {
                    "artifactId": str(uuid.uuid4()),
                    "name": "response",
                    "parts": parts,
                }
            ],
        },
    }


def _build_response(is_json_rpc: bool, body: dict, params: dict, text: str) -> dict:
    if is_json_rpc:
        return _build_a2a_response(body, params, [{"kind": "text", "text": text}])
    return {"response": text}


# ── SSE streaming helpers ─────────────────────────────────────────────────────

async def _iter_stream_events(question: str, token: str, user_id: str):
    """Bridge sync stream_bigquery generator into async, running it in a thread."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    _DONE = object()

    def run_sync():
        try:
            for event in stream_bigquery(question, token, user_id):
                asyncio.run_coroutine_threadsafe(queue.put(event), loop)
        finally:
            asyncio.run_coroutine_threadsafe(queue.put(_DONE), loop)

    fut = loop.run_in_executor(None, run_sync)
    while True:
        item = await queue.get()
        if item is _DONE:
            break
        yield item
    await fut  # propagate any thread exception


async def _sse_stream(question: str, token: str, user_id: str, body: dict, params: dict):
    """
    Yield SSE-formatted A2A events.

    PROGRESS events → TaskStatusUpdateEvent with state=working (streamed immediately)
    All other events → accumulated, sent as a single completed artifact at the end
    """
    task_id  = str(uuid.uuid4())
    req_id   = body.get("id")
    context_id = params.get("message", {}).get("messageId", str(uuid.uuid4()))

    accumulated = {"answer": None, "sql": None, "rows": [], "charts": []}

    def _sse(payload: dict) -> str:
        return f"data: {json.dumps(payload)}\n\n"

    async for event in _iter_stream_events(question, token, user_id):
        t = event["type"]

        if t == "progress":
            # Show only the first 3 words as a brief progress indicator
            words = event["text"].split()
            preview = " ".join(words[:3]) + ("..." if len(words) > 3 else "")
            # TaskStatusUpdateEvent — taskId (not id), messageId required in message
            yield _sse({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "taskId": task_id,
                    "contextId": context_id,
                    "status": {
                        "state": "working",
                        "message": {
                            "messageId": str(uuid.uuid4()),
                            "role": "agent",
                            "parts": [{"kind": "text", "text": preview}],
                        },
                    },
                    "final": False,
                },
            })
        elif t == "answer":
            accumulated["answer"] = event["text"]
        elif t == "sql":
            accumulated["sql"] = event["sql"]
        elif t == "rows":
            accumulated["rows"].extend(event["rows"])
        elif t == "chart":
            accumulated["charts"].append(event)
        elif t == "error":
            # TaskStatusUpdateEvent — failed state, messageId required
            yield _sse({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "taskId": task_id,
                    "contextId": context_id,
                    "status": {
                        "state": "failed",
                        "message": {
                            "messageId": str(uuid.uuid4()),
                            "role": "agent",
                            "parts": [{"kind": "text", "text": f"Error: {event['error']}"}],
                        },
                    },
                    "final": True,
                },
            })
            return

    # TaskArtifactUpdateEvent — artifact singular at top level, taskId not id
    parts = _artifact_parts(
        accumulated["answer"], accumulated["sql"],
        accumulated["rows"], accumulated["charts"],
    )
    yield _sse({
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "taskId": task_id,
            "contextId": context_id,
            "artifact": {
                "artifactId": str(uuid.uuid4()),
                "name": "response",
                "parts": parts,
            },
            "final": True,
        },
    })


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/.well-known/agent.json")
async def agent_card():
    """Serve A2A agent card for Gemini Enterprise discovery."""
    cloud_run_url = (
        os.getenv("AGENT_URL", "").strip()
        or f"https://{os.getenv('K_SERVICE', AGENT_NAME)}-bt24fn2lfa-uc.a.run.app"
    )
    return {
        "name": AGENT_NAME,
        "description": AGENT_DESCRIPTION,
        "url": cloud_run_url,
        "version": "1.0.0",
        "skills": [
            {
                "id": "query_bigquery",
                "name": "Query BigQuery",
                "description": "Ask natural language questions about your BigQuery data.",
                "tags": ["bigquery", "analytics", "sql", "data"],
            }
        ],
        "capabilities": {"streaming": True, "pushNotifications": False},
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "protocolVersion": "1.0.0",
    }


@app.api_route("/", methods=["GET", "POST"])
async def root_handler(request: Request, authorization: str = Header(None)):
    """Handle root health checks and Gemini Enterprise POSTs."""
    if request.method == "GET":
        return {"status": "ok", "message": "BigQuery Data Agent A2A Proxy is running"}
    return await handle_query(request, authorization)


@app.post("/query")
async def handle_query(request: Request, authorization: str = Header(None)):
    """
    Receive A2A / JSON-RPC 2.0 queries from Gemini Enterprise, call the
    BigQuery Data Agent with the user's delegated token, and return the
    result in A2A format.

    If method=message/stream, returns an SSE stream with live PROGRESS events
    followed by the final artifact. Otherwise returns a single JSON response.
    """
    req_id = str(uuid.uuid4())[:8]
    t_start = time.monotonic()

    logger.info(
        "[%s] request_received: method=%s path=%s has_auth=%s content_type=%s",
        req_id, request.method, request.url.path,
        bool(request.headers.get("Authorization")),
        request.headers.get("content-type", ""),
    )

    try:
        body = await request.json()

        # 1. Parse JSON-RPC 2.0 envelope (A2A standard)
        is_json_rpc   = body.get("jsonrpc") == "2.0"
        rpc_method    = body.get("method", "")
        is_streaming  = rpc_method == "message/stream"
        params        = body.get("params", {})
        text          = ""

        if is_json_rpc:
            message = params.get("message", {})
            for part in message.get("parts", []):
                if part.get("kind") == "text" or "text" in part:
                    text = part.get("text", "")
                    break

        if not text:
            text = body.get("text") or body.get("query") or body.get("input", "")

        if not text:
            logger.error("[%s] query_parse_failed: no text found in keys=%s", req_id, list(body.keys()))
            error_msg = "Missing query text in request."
            if is_json_rpc:
                return {
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "error": {"code": -32602, "message": error_msg},
                }
            return {"error": error_msg}

        logger.info("[%s] query_text: is_json_rpc=%s streaming=%s text=%.80s",
                    req_id, is_json_rpc, is_streaming, text)

        # 2. Ping test
        if text.lower().strip() == "ping":
            logger.debug("[%s] ping_received", req_id)
            return _build_response(is_json_rpc, body, params, "pong")

        # 3. Validate Authorization header
        auth_header = request.headers.get("Authorization", "")

        if not (auth_header and auth_header.lower().startswith("bearer ")):
            logger.warning(
                "[%s] oauth.no_auth_header — user has not completed OAuth Connect", req_id,
            )
            msg = ("Please connect your Google account: click the **Connect** button in "
                   "Gemini Enterprise to authorise this agent.")
            return _build_response(is_json_rpc, body, params, msg)

        token  = auth_header.split(" ", 1)[1].strip()
        claims = decode_token_claims(token)
        logger.info("[%s] oauth.token_received: token_length=%d", req_id, len(token))

        token_email = claims.get("email", "")
        if "gserviceaccount.com" in token_email:
            logger.warning("[%s] oauth.service_account_detected: domain=%s",
                           req_id, token_email.split("@")[-1] if "@" in token_email else "unknown")
            msg = ("Please connect your Google account: click the **Connect** button in "
                   "Gemini Enterprise to authorise this agent.")
            return _build_response(is_json_rpc, body, params, msg)

        user_id, id_source = _extract_user_id(claims, token)
        masked = _mask_email(user_id) if "@" in user_id else user_id
        logger.info("[%s] oauth.user_identified: source_claim=%s masked_id=%s",
                    req_id, id_source, masked)

        # 4. Conversation reset command
        if text.lower().strip() in ("reset", "reset conversation", "start over", "new conversation"):
            logger.info("[%s] conversation_reset: user=%s", req_id, masked)
            reset_user_conversation(user_id)
            msg = "Conversation reset. Starting fresh — ask me anything about your BigQuery data."
            return _build_response(is_json_rpc, body, params, msg)

        # 5. Route to streaming or batch
        logger.info("[%s] bq.call_start: user=%s streaming=%s query=%.80s",
                    req_id, masked, is_streaming, text)

        if is_streaming:
            return StreamingResponse(
                _sse_stream(text, token, user_id, body, params),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        # Batch path — collect full result then respond
        t_bq   = time.monotonic()
        result = await asyncio.to_thread(ask_bigquery, text, token, user_id)
        bq_elapsed = round((time.monotonic() - t_bq) * 1000)

        if result["status"] == "success":
            logger.info(
                "[%s] bq.call_success: elapsed_ms=%d answer_len=%d has_sql=%s rows=%d charts=%d",
                req_id, bq_elapsed, len(result.get("answer") or ""),
                bool(result.get("sql")), len(result.get("rows") or []),
                len(result.get("charts") or []),
            )
            parts = _artifact_parts(
                result.get("answer"), result.get("sql"),
                result.get("rows") or [], result.get("charts") or [],
            )
            elapsed = round((time.monotonic() - t_start) * 1000)
            logger.info("[%s] request_complete: total_ms=%d", req_id, elapsed)
            if is_json_rpc:
                return _build_a2a_response(body, params, parts)
            return {"response": result.get("answer") or ""}
        else:
            error = result.get("error", "Unknown error")
            logger.error("[%s] bq.call_failed: elapsed_ms=%d error=%s", req_id, bq_elapsed, error)
            return _build_response(is_json_rpc, body, params, f"Error querying BigQuery: {error}")

    except Exception as e:
        elapsed = round((time.monotonic() - t_start) * 1000)
        logger.exception("[%s] unhandled_exception: elapsed_ms=%d error=%s", req_id, elapsed, e)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    logger.info("startup: agent=%s port=%d", AGENT_NAME, port)
    uvicorn.run(app, host="0.0.0.0", port=port)
