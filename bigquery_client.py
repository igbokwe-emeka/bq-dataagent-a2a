"""
BigQuery DataChatServiceClient wrapper for the A2A proxy.

Maintains per-user conversation state in memory so follow-up questions
retain context within the proxy's lifetime. Conversations reset on
proxy restart.

Auth model:
    The Entra ID token injected by Gemini Enterprise is exchanged for a
    Google federated access token via Workforce Identity Federation (WIF) STS.
    That token is wrapped as google.oauth2.credentials.Credentials and passed
    to DataChatServiceClient so all GDA and cloudaicompanion API calls run
    under the user's own federated identity.

    Required IAM on the project for the WIF principal set
    (principalSet://iam.googleapis.com/locations/global/workforcePools/<pool>/*):
        roles/geminidataanalytics.dataAgentUser    — create/use GDA conversations
        roles/cloudaicompanion.user                — chat() → conversations.get
        roles/bigquery.user                        — run BigQuery queries
        roles/bigquery.jobUser                     — submit BigQuery jobs
"""
import os
import uuid
import json
import logging
import requests as _requests
from typing import Any, Dict, Iterator

import google.oauth2.credentials
from google.cloud import geminidataanalytics

logger = logging.getLogger(__name__)

PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "")
LOCATION      = os.getenv("GCP_LOCATION", "global")
DATA_AGENT_ID = os.getenv("DATA_AGENT_ID", "")

# Workforce Identity Federation — pool and provider configured in GCP to trust
# the Entra ID app. The STS exchanges an Entra token for a Google access token.
WIF_POOL     = os.getenv("WIF_POOL", "ocepool")
WIF_PROVIDER = os.getenv("WIF_PROVIDER", "oce-oidc")
WIF_AUDIENCE = f"//iam.googleapis.com/locations/global/workforcePools/{WIF_POOL}/providers/{WIF_PROVIDER}"
STS_ENDPOINT = "https://sts.googleapis.com/v1/token"

# Per-user conversation IDs: user_id -> conversation_id (UUID string)
_conversation_ids: Dict[str, str] = {}


def _exchange_entra_token(entra_token: str) -> str:
    """
    Exchange an Entra ID JWT for a Google federated access token via WIF STS.

    The Entra token injected by Gemini Enterprise is a Microsoft JWT and cannot
    be used directly with Google APIs. This exchanges it for a short-lived Google
    access token scoped to cloud-platform using the configured WIF pool/provider.
    """
    logger.debug("wif.exchange_start: audience=%s", WIF_AUDIENCE)
    resp = _requests.post(STS_ENDPOINT, data={
        "grant_type":           "urn:ietf:params:oauth:grant-type:token-exchange",
        "audience":             WIF_AUDIENCE,
        "subject_token_type":   "urn:ietf:params:oauth:token-type:id_token",
        "subject_token":        entra_token,
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "scope":                "https://www.googleapis.com/auth/cloud-platform",
    }, timeout=10)

    if not resp.ok:
        logger.error("wif.exchange_failed: status=%d body=%s", resp.status_code, resp.text[:300])
        raise RuntimeError(f"WIF token exchange failed ({resp.status_code}): {resp.text}")

    google_token = resp.json().get("access_token")
    if not google_token:
        raise RuntimeError(f"WIF STS response missing access_token: {resp.text}")

    logger.info("wif.exchange_success: google_token_length=%d", len(google_token))
    return google_token


def get_agent_path() -> str:
    return f"projects/{PROJECT_ID}/locations/{LOCATION}/dataAgents/{DATA_AGENT_ID}"


def _get_or_create_conversation_id(user_id: str) -> str:
    if user_id not in _conversation_ids:
        _conversation_ids[user_id] = str(uuid.uuid4())
        logger.info(f"New conversation for {user_id[:8]}...: {_conversation_ids[user_id]}")
    return _conversation_ids[user_id]


def reset_user_conversation(user_id: str) -> str:
    """Reset the conversation for a user and return the new conversation ID."""
    new_id = str(uuid.uuid4())
    _conversation_ids[user_id] = new_id
    logger.info(f"Conversation reset for {user_id[:8]}...: {new_id}")
    return new_id


def stream_bigquery(question: str, user_token: str, user_id: str) -> Iterator[Dict[str, Any]]:
    """
    Generator that streams events from the BigQuery Data Agent as they arrive.

    Yields dicts with a 'type' key:
        {"type": "progress", "text": str}
            — PROGRESS step from the agent (surface to user in real time)
        {"type": "sql",      "sql":  str}
            — SQL generated for the query
        {"type": "rows",     "rows": list[dict]}
            — query result rows
        {"type": "chart",    "mime_type": str, "data": bytes|None, "vega_spec": dict|None}
            — chart produced by the agent (PNG image and/or Vega-Lite spec)
        {"type": "answer",   "text": str}
            — FINAL_RESPONSE natural language answer
        {"type": "error",    "error": str}
            — any exception; always the last event on failure

    THOUGHT chunks are logged at DEBUG level only — they are internal agent
    reasoning and should not be surfaced to the end user.
    """
    try:
        google_token = _exchange_entra_token(user_token)
        credentials = google.oauth2.credentials.Credentials(token=google_token)
        client = geminidataanalytics.DataChatServiceClient(credentials=credentials)

        agent_path = get_agent_path()
        parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
        conversation_id = _get_or_create_conversation_id(user_id)

        logger.info(
            "bq.client_init: user=%s agent=%s conversation=%s",
            user_id[:8], agent_path, conversation_id,
        )

        try:
            conversation = geminidataanalytics.Conversation(agents=[agent_path])
            create_req = geminidataanalytics.CreateConversationRequest(
                parent=parent,
                conversation_id=conversation_id,
                conversation=conversation,
            )
            conv_response = client.create_conversation(request=create_req)
            conversation_name = conv_response.name
            logger.info("bq.conversation_created: name=%s", conversation_name)
        except Exception as e:
            conversation_name = f"{parent}/conversations/{conversation_id}"
            logger.warning("bq.conversation_create_failed (using derived name): name=%s reason=%s", conversation_name, e)

        message = geminidataanalytics.Message(
            user_message=geminidataanalytics.UserMessage(text=question)
        )
        chat_request = geminidataanalytics.ChatRequest(
            parent=parent,
            messages=[message],
            conversation_reference=geminidataanalytics.ConversationReference(
                conversation=conversation_name,
                data_agent_context=geminidataanalytics.DataAgentContext(
                    data_agent=agent_path
                )
            ),
        )

        TextType = geminidataanalytics.TextMessage.TextType

        for response in client.chat(request=chat_request):
            sm = response.system_message
            if not sm:
                continue

            if sm.data and sm.data.generated_sql:
                yield {"type": "sql", "sql": sm.data.generated_sql}

            if sm.data and sm.data.result and sm.data.result.data:
                yield {"type": "rows", "rows": [dict(r) for r in sm.data.result.data]}

            if sm.chart and sm.chart.result:
                cr = sm.chart.result
                vega_spec = None
                if cr.vega_config:
                    # type(cr).pb(cr) gives the raw ChartResult proto; .vega_config on
                    # that is a proper Struct proto message that MessageToDict handles
                    from google.protobuf import json_format
                    vega_spec = json_format.MessageToDict(type(cr).pb(cr).vega_config)
                image_bytes = bytes(cr.image.data) if (cr.image and cr.image.data) else None
                image_mime = (cr.image.mime_type or "image/png") if cr.image else "image/png"

                # BQ agent typically returns only a Vega spec (no image bytes).
                # Render it to PNG server-side so GE can display it inline.
                if not image_bytes and vega_spec:
                    try:
                        import vl_convert as vlc
                        image_bytes = vlc.vegalite_to_png(vl_spec=json.dumps(vega_spec), scale=2)
                        image_mime = "image/png"
                        logger.info("bq.chart: rendered vega spec to PNG (%d bytes)", len(image_bytes))
                    except Exception as vega_err:
                        logger.warning("bq.chart: vega render failed: %s", vega_err)

                logger.info("bq.chart: has_vega=%s has_image=%s mime=%s", bool(vega_spec), bool(image_bytes), image_mime)
                yield {"type": "chart", "mime_type": image_mime, "data": image_bytes, "vega_spec": vega_spec}

            if sm.text and sm.text.parts:
                text = " ".join(sm.text.parts)
                if sm.text.text_type in (TextType.PROGRESS, TextType.THOUGHT):
                    # GDA emits THOUGHT chunks (not PROGRESS) — both are surfaced
                    # as progress indicators so the user sees live activity
                    logger.debug("bq.progress(%s): %s", sm.text.text_type.name, text[:120])
                    yield {"type": "progress", "text": text}
                elif sm.text.text_type == TextType.FINAL_RESPONSE:
                    yield {"type": "answer", "text": text}

            if sm.error:
                logger.warning("bq.agent_error: %s", sm.error)

    except Exception as e:
        error_str = str(e)
        if any(kw in error_str.lower() for kw in ("401", "403", "unauthenticated", "permission", "credential")):
            logger.error(
                "bq.auth_error: user=%s error=%s — token may be invalid or lack BigQuery IAM permissions",
                user_id[:8], error_str,
            )
        else:
            logger.error("bq.api_error: user=%s error=%s", user_id[:8], error_str, exc_info=True)
        yield {"type": "error", "error": error_str}


def ask_bigquery(question: str, user_token: str, user_id: str) -> Dict[str, Any]:
    """
    Batch wrapper around stream_bigquery — collects all events and returns a summary dict.
    Used for message/send (non-streaming) A2A requests.
    """
    result: Dict[str, Any] = {
        "status": "success",
        "question": question,
        "answer": None,
        "sql": None,
        "rows": [],
        "charts": [],
    }
    for event in stream_bigquery(question, user_token, user_id):
        t = event["type"]
        if t == "answer":
            result["answer"] = event["text"]
        elif t == "sql":
            result["sql"] = event["sql"]
        elif t == "rows":
            result["rows"].extend(event["rows"])
        elif t == "chart":
            result["charts"].append(event)
        elif t == "error":
            return {"status": "error", "error": event["error"], "error_code": "API_ERROR"}
    return result
