


"""
deploy.py — Deploy Snowflake Cortex A2A Proxy to Cloud Run and register with Gemini Enterprise.
"""
import os
import sys
import json
import subprocess
import urllib.request
import urllib.error
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

REGION     = os.getenv("GCP_REGION",    "us-central1")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
LOCATION   = os.getenv("GCP_LOCATION",  "us")

if not PROJECT_ID:
    print("[ERROR] GCP_PROJECT_ID must be set in .env")
    sys.exit(1)


def gcloud(*args, check=True):
    cmd = "gcloud.cmd" if sys.platform == "win32" else "gcloud"
    result = subprocess.run([cmd] + list(args), capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"[ERROR] gcloud {' '.join(args[:3])} failed:\n{result.stderr.strip()}")
        sys.exit(1)
    return result.stdout.strip()


def get_access_token():
    cmd = "gcloud.cmd" if sys.platform == "win32" else "gcloud"
    result = subprocess.run([cmd, "auth", "print-access-token"], capture_output=True, text=True)
    if result.returncode != 0:
        print("[ERROR] Could not obtain gcloud access token. Run 'gcloud auth login' and try again.")
        sys.exit(1)
    return result.stdout.strip()


def _auth_url(auth_id):
    return (
        f"https://{LOCATION}-discoveryengine.googleapis.com/v1alpha"
        f"/projects/{PROJECT_ID}/locations/{LOCATION}/authorizations/{auth_id}"
    )


def authorization_exists(token, auth_id):
    """Return True if the authorization resource already exists."""
    req = urllib.request.Request(
        _auth_url(auth_id),
        headers={"Authorization": f"Bearer {token}", "x-goog-user-project": PROJECT_ID},
        method="GET"
    )
    try:
        with urllib.request.urlopen(req):
            return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        print(f"[ERROR] Unexpected error checking authorization: {e.code} {e.reason}")
        sys.exit(1)


def delete_authorization(token, auth_id):
    req = urllib.request.Request(
        _auth_url(auth_id),
        headers={"Authorization": f"Bearer {token}", "x-goog-user-project": PROJECT_ID},
        method="DELETE"
    )
    try:
        with urllib.request.urlopen(req):
            print(f"[OK] Deleted existing authorization: {auth_id}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"[WARN] Could not delete authorization {auth_id}: {e.code} {e.reason} — {body[:200]}")


def delete_registered_agents(token, agent_name):
    """Delete any registered agents with this display name so the auth resource can be freed."""
    host = f"{LOCATION}-discoveryengine.googleapis.com"
    engine_id = os.getenv("GCP_ENGINE_ID", "")
    base_url = (
        f"https://{host}/v1alpha/projects/{PROJECT_ID}/locations/{LOCATION}"
        f"/collections/default_collection/engines/{engine_id}"
        f"/assistants/default_assistant/agents"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-goog-user-project": PROJECT_ID
    }
    try:
        req = urllib.request.Request(base_url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as resp:
            agents = json.loads(resp.read().decode()).get("agents", [])
    except urllib.error.HTTPError as e:
        print(f"[WARN] Could not list agents: {e.code} {e.reason}")
        return

    for agent in agents:
        if agent.get("displayName") == agent_name:
            name = agent["name"]
            try:
                del_req = urllib.request.Request(
                    f"https://{host}/v1alpha/{name}",
                    headers=headers,
                    method="DELETE"
                )
                with urllib.request.urlopen(del_req):
                    print(f"[OK] Unregistered agent: {name}")
            except urllib.error.HTTPError as e:
                print(f"[WARN] Could not delete agent {name}: {e.code} {e.reason}")


def create_authorization(token, auth_id):
    """Create a serverSideOauth2 authorization resource for Entra ID."""
    tenant_id     = os.getenv("OAUTH_TENANT_ID", "")
    client_id     = os.getenv("OAUTH_CLIENT_ID", "")
    client_secret = os.getenv("OAUTH_CLIENT_SECRET", "")
    default_scopes = f"api://{client_id}/session:role-any openid offline_access"
    scopes = os.getenv("OAUTH_SCOPES", default_scopes).split()

    if not all([tenant_id, client_id, client_secret]):
        print("[ERROR] OAUTH_TENANT_ID, OAUTH_CLIENT_ID, and OAUTH_CLIENT_SECRET must be set in .env")
        sys.exit(1)

    # Use OAUTH_AUTH_URL from .env as base (preserves response_type, response_mode, etc.)
    # then ensure redirect_uri and prompt=consent are present
    base_auth_url = os.getenv("OAUTH_AUTH_URL") or f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
    parsed = urllib.parse.urlparse(base_auth_url)
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    redirect_uri = "https://vertexaisearch.cloud.google.com/static/oauth/oauth.html"
    params["redirect_uri"] = [redirect_uri]
    params["prompt"] = ["consent"]

    authorization_uri = urllib.parse.urlunparse(
        parsed._replace(query=urllib.parse.urlencode({k: v[0] for k, v in params.items()}))
    )

    token_uri = os.getenv("OAUTH_TOKEN_URL") or f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    payload = {
        "displayName": auth_id,
        "serverSideOauth2": {
            "clientId": client_id,
            "clientSecret": client_secret,
            "authorizationUri": authorization_uri,
            "tokenUri": token_uri,
            "scopes": scopes
        }
    }

    create_url = (
        f"https://{LOCATION}-discoveryengine.googleapis.com/v1alpha"
        f"/projects/{PROJECT_ID}/locations/{LOCATION}/authorizations"
        f"?authorizationId={auth_id}"
    )

    req = urllib.request.Request(
        create_url,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-goog-user-project": PROJECT_ID
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            name = result.get("name", "")
            print(f"[OK] Authorization created: {name}")
            return name
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"[ERROR] Could not create authorization: {e.code} {e.reason}\n{body}")
        sys.exit(1)


def manage_authorization(service_name, agent_name="snowflake-a2a"):
    """Delete and recreate the OAuth authorization resource. Returns its full resource name."""
    auth_id = f"{service_name}-oauth"
    print(f"\nManaging authorization resource '{auth_id}'...")

    token = get_access_token()

    if authorization_exists(token, auth_id):
        print(f"Authorization '{auth_id}' already exists. Unregistering bound agents first...")
        delete_registered_agents(token, agent_name)
        print("Deleting authorization...")
        delete_authorization(token, auth_id)

    return create_authorization(token, auth_id)


def main():
    agent_name   = os.getenv("AGENT_NAME", "snowflake-cortex-adk-proxy")
    service_name = agent_name.replace("_", "-").lower()

    env_vars = ",".join([
        f"AGENT_NAME={os.getenv('AGENT_NAME', '')}",
        f"AGENT_DATABASE={os.getenv('AGENT_DATABASE', '')}",
        f"AGENT_SCHEMA={os.getenv('AGENT_SCHEMA', '')}",
        f"SNOWFLAKE_ACCOUNT={os.getenv('SNOWFLAKE_ACCOUNT', '')}",
        f"SNOWFLAKE_USER={os.getenv('SNOWFLAKE_USER', '')}",
    ])

    image = f"us-central1-docker.pkg.dev/{PROJECT_ID}/cloud-run-source-deploy/{service_name}:latest"

    # Step 1: Build
    print(f"Building container image for {service_name}...")
    gcloud("builds", "submit", "--tag", image, "--project", PROJECT_ID, ".")
    print("[OK] Build successful!")

    # Step 2: Deploy
    print(f"\nDeploying {service_name} to Cloud Run in project {PROJECT_ID}...")
    gcloud(
        "run", "deploy", service_name,
        "--image", image,
        "--region", REGION,
        "--allow-unauthenticated",
        "--set-env-vars", env_vars,
        "--port", "8080",
        "--min-instances", "1",
        "--max-instances", "1",
        "--project", PROJECT_ID,
    )
    print("[OK] Deployment successful!")

    url = gcloud(
        "run", "services", "describe", service_name,
        "--region", REGION,
        "--project", PROJECT_ID,
        "--format=value(status.url)",
    )
    print(f"Service URL: {url}")

    # Step 3: Manage authorization resource
    auth_resource_name = manage_authorization(service_name)
    os.environ["AGENT_AUTHORIZATION"] = auth_resource_name

    # Step 4: Register agent
    print("\nRegistering agent with Gemini Enterprise...")
    result = subprocess.run([sys.executable, "register_a2a_agent.py"])

    if result.returncode == 0:
        print("\n[OK] Complete End-to-End Deployment and Registration Successful!")
    else:
        print("[ERROR] Could not register agent with Discovery Engine.")


if __name__ == "__main__":
    main()

