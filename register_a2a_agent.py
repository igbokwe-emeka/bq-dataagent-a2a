import os
import sys
import json
import subprocess
import urllib.request
import urllib.error
from dotenv import load_dotenv

load_dotenv()

def _env(key, default=""):
    """Load env var and strip any surrounding quotes left by .env editors."""
    return os.getenv(key, default).strip().strip('"').strip("'")

PROJECT_ID = _env("GCP_PROJECT_ID")
LOCATION   = _env("DISCOVERY_LOCATION", "us")
ENGINE_ID  = _env("GCP_ENGINE_ID")
AGENT_NAME = "bq-dataagent-a2a"

if not PROJECT_ID:
    print("[ERROR] GCP_PROJECT_ID must be set in .env")
    sys.exit(1)
if not ENGINE_ID:
    print("[ERROR] GCP_ENGINE_ID must be set in .env")
    sys.exit(1)

AGENT_DESCRIPTION  = _env("AGENT_DESCRIPTION", f"BigQuery Data Agent ({AGENT_NAME}) exposed via A2A proxy.")
AGENT_AUTHORIZATION = _env("AGENT_AUTHORIZATION", "")


def get_access_token():
    try:
        cmd = "gcloud.cmd" if sys.platform == "win32" else "gcloud"
        result = subprocess.run(
            [cmd, "auth", "print-access-token"],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        print("[ERROR] Could not obtain gcloud access token.")
        print("Please run 'gcloud auth login' in your terminal and try again.")
        sys.exit(1)


def get_cloud_run_url():
    service_name = _env("AGENT_NAME", "bqdataagenta2a").replace("_", "-").lower()
    cmd = "gcloud.cmd" if sys.platform == "win32" else "gcloud"
    try:
        result = subprocess.run(
            [cmd, "run", "services", "describe", service_name,
             "--project", PROJECT_ID, "--region", _env("GCP_REGION", "us-central1"),
             "--format=value(status.url)"],
            capture_output=True, text=True, check=True, timeout=10
        )
        url = result.stdout.strip()
        if url.startswith("https"):
            return url
    except Exception:
        pass
    print("[ERROR] Could not fetch Cloud Run URL. Have you deployed the proxy yet?")
    sys.exit(1)


def grant_public_access():
    """Grant allUsers the Cloud Run invoker role so the service is publicly accessible."""
    service_name = _env("AGENT_NAME", "bqdataagenta2a").replace("_", "-").lower()
    region = _env("GCP_REGION", "us-central1")
    cmd = "gcloud.cmd" if sys.platform == "win32" else "gcloud"
    print("Granting public access (allUsers -> roles/run.invoker)...")
    result = subprocess.run(
        [cmd, "run", "services", "add-iam-policy-binding", service_name,
         "--member=allUsers",
         "--role=roles/run.invoker",
         f"--region={region}",
         f"--project={PROJECT_ID}"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print("[OK] Public access granted.")
    else:
        print(f"[WARN] Could not grant public access: {result.stderr.strip()}")


def delete_existing_agents(token):
    """List all registered agents and delete any that match AGENT_NAME."""
    host = "discoveryengine.googleapis.com" if LOCATION == "global" else f"{LOCATION}-discoveryengine.googleapis.com"
    base_url = (
        f"https://{host}/v1alpha/projects/{PROJECT_ID}/locations/{LOCATION}"
        f"/collections/default_collection/engines/{ENGINE_ID}"
        f"/assistants/default_assistant/agents"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-goog-user-project": PROJECT_ID
    }

    try:
        req = urllib.request.Request(base_url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as response:
            agents = json.loads(response.read().decode()).get("agents", [])
    except urllib.error.HTTPError as e:
        print(f"[WARN] Could not list agents: {e.code} {e.reason}")
        return

    for agent in agents:
        if agent.get("displayName") == AGENT_NAME:
            name = agent["name"]
            print(f"Deleting existing agent: {name}")
            try:
                del_req = urllib.request.Request(
                    f"https://{host}/v1alpha/{name}",
                    headers=headers,
                    method="DELETE"
                )
                with urllib.request.urlopen(del_req):
                    print(f"[OK] Deleted agent: {name}")
            except urllib.error.HTTPError as e:
                print(f"[WARN] Could not delete agent {name}: {e.code} {e.reason}")


def register_agent():
    token = get_access_token()
    delete_existing_agents(token)

    cloud_run_url = _env("AGENT_URL")
    if not cloud_run_url or not cloud_run_url.startswith("https"):
        cloud_run_url = get_cloud_run_url()

    agent_card = {
        "name": AGENT_NAME,
        "description": AGENT_DESCRIPTION,
        "url": cloud_run_url,
        "version": "1.0.0",
        "skills": [
            {
                "id": "query_bigquery",
                "name": "Query BigQuery",
                "description": f"Ask natural language questions about your BigQuery data.",
                "tags": ["snowflake", "cortex", "ai", "analytics"]
            }
        ],
        "capabilities": {
            "streaming": True,
            "pushNotifications": False
        },
        "defaultInputModes": ["text"],
        "defaultOutputModes": ["text"],
        "protocolVersion": "1.0.0"
    }

    payload = {
        "displayName": AGENT_NAME,
        "description": AGENT_DESCRIPTION,
        "a2aAgentDefinition": {
            "jsonAgentCard": json.dumps(agent_card)
        },
        "sharingConfig": {
            "scope": "ALL_USERS"
        },
        "authorizationConfig": {
            "agentAuthorization": AGENT_AUTHORIZATION
        }
    }

    host = "discoveryengine.googleapis.com" if LOCATION == "global" else f"{LOCATION}-discoveryengine.googleapis.com"
    url = (
        f"https://{host}/v1alpha/projects/{PROJECT_ID}/locations/{LOCATION}"
        f"/collections/default_collection/engines/{ENGINE_ID}"
        f"/assistants/default_assistant/agents"
    )

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-goog-user-project": PROJECT_ID
        },
        method="POST"
    )

    print(f"Registering Agent '{AGENT_NAME}' to Gemini Enterprise App '{ENGINE_ID}'...")
    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            print("\n[OK] Registration Successful!")
            print(json.dumps(result, indent=2))
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        print(f"\n[ERROR] Error {e.code}: {e.reason}")
        print("Response Body:", body_text)
        try:
            body = json.loads(body_text)
            print("Message:", body.get("error", {}).get("message", "Unknown"))
        except Exception:
            pass
        with open("error.log", "w") as f:
            f.write(body_text)
        print("Full details written to error.log")


if __name__ == "__main__":
    grant_public_access()
    register_agent()
