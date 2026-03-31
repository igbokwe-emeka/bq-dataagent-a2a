# BigQuery Data Agent A2A Proxy

A FastAPI proxy that bridges **Gemini Enterprise** to the **BigQuery Data Agent** using the Agent-to-Agent (A2A) protocol. Users interact with their BigQuery data in plain English through Gemini Enterprise, with their own GCP identity propagated end-to-end via Entra ID OAuth.

## Architecture

```
Gemini Enterprise
      │  JSON-RPC 2.0 (A2A)
      │  Authorization: Bearer <Entra ID token>
      ▼
A2A Proxy (Cloud Run)
      │  Credentials(token=<user token>)
      │  DataChatServiceClient
      ▼
BigQuery Data Agent
      │
      ▼
BigQuery
```

### Identity propagation

Gemini Enterprise injects the user's Entra ID OAuth token into the `Authorization` header after the user completes the **Connect** flow. The proxy:

1. Decodes the JWT (without verification) to identify the user
2. Detects service account tokens (user hasn't connected yet) and prompts them to connect
3. Wraps the user's token as `google.oauth2.credentials.Credentials`
4. Passes those credentials to `DataChatServiceClient` so all BigQuery operations run under the user's own GCP identity, respecting their IAM roles and row-level security

## Conversation state

The proxy maintains per-user conversation IDs in memory, keyed by email address. This means:

- Follow-up questions retain context within the same proxy instance
- Conversation state resets on proxy restart (new conversation is created automatically)
- Users can explicitly reset with: `reset`, `reset conversation`, or `start over`

## Project structure

```
bq-dataagent-a2a/
├── main.py                  # FastAPI app — A2A / JSON-RPC 2.0 handler
├── auth.py                  # JWT decoder (inspect Entra ID claims)
├── bigquery_client.py       # DataChatServiceClient wrapper
├── requirements.txt
├── Dockerfile
├── env.template             # Copy to .env and fill in values
├── deploy.py                # One-command deploy + register
└── register_a2a_agent.py    # Gemini Enterprise agent registration
```

## Configuration

Copy `env.template` to `.env` and fill in the values:

```bash
cp env.template .env
```

| Variable | Description |
|---|---|
| `GCP_PROJECT_ID` | GCP project ID |
| `GCP_LOCATION` | BigQuery Data Agent location (usually `global`) |
| `DATA_AGENT_ID` | BigQuery Data Agent ID (e.g. `agent_edf2178e-...`) |
| `GCP_ENGINE_ID` | Gemini Enterprise Discovery Engine ID |
| `GCP_REGION` | Cloud Run region (e.g. `us-central1`) |
| `DISCOVERY_LOCATION` | Discovery Engine location (e.g. `us`) |
| `OAUTH_TENANT_ID` | Entra ID tenant ID |
| `OAUTH_CLIENT_ID` | Entra ID app client ID |
| `OAUTH_CLIENT_SECRET` | Entra ID app client secret |
| `AGENT_NAME` | Cloud Run service name (default: `bq-dataagent-a2a`) |
| `AGENT_URL` | Cloud Run URL (set after first deploy) |

## Required IAM bindings

The Workforce Identity Federation principal set must have the following roles on the GCP project. These allow federated users (authenticated via Entra ID) to query BigQuery through the Data Agent with their own identity.

```bash
PRINCIPAL="principalSet://iam.googleapis.com/locations/global/workforcePools/YOUR_WIF_POOL/*"
PROJECT=YOUR_GCP_PROJECT_ID

gcloud projects add-iam-policy-binding $PROJECT \
  --member="$PRINCIPAL" --role="roles/geminidataanalytics.dataAgentUser"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="$PRINCIPAL" --role="roles/cloudaicompanion.user"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="$PRINCIPAL" --role="roles/bigquery.user"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="$PRINCIPAL" --role="roles/bigquery.jobUser"
```

| Role | Why it's needed |
|---|---|
| `roles/geminidataanalytics.dataAgentUser` | Create and use stateful GDA conversations |
| `roles/cloudaicompanion.user` | `chat()` internally calls `cloudaicompanion.conversations.get`; WIF users need this role or the call returns 403 |
| `roles/bigquery.user` | Run BigQuery queries on datasets the user has access to |
| `roles/bigquery.jobUser` | Submit BigQuery jobs under the user's identity |

> **Note on `cloudaicompanion.user`**: This role is not automatically included in `geminidataanalytics.dataAgentUser`. Omitting it causes `403 Permission cloudaicompanion.conversations.get denied` when `chat()` is called, even though conversation creation succeeds.

## Deploy

### Prerequisites

- `gcloud` CLI authenticated (`gcloud auth login`)
- Artifact Registry repository exists in `us-central1`
- Entra ID app registered with BigQuery OAuth scopes

### One-command deploy

```bash
python deploy.py
```

This runs four steps automatically:

1. **Build** — submits a Cloud Build job and pushes the image to Artifact Registry
2. **Deploy** — deploys to Cloud Run with `--allow-unauthenticated`
3. **Authorization** — creates (or recreates) the `serverSideOauth2` authorization resource in Discovery Engine
4. **Register** — registers the A2A agent in Gemini Enterprise

### Register only (after code-only redeploy)

```bash
python deploy.py   # full redeploy
# or, to register only after a manual gcloud run deploy:
export AGENT_URL=https://your-proxy-url.a.run.app
export AGENT_AUTHORIZATION=projects/.../authorizations/bq-dataagent-a2a-oauth
python register_a2a_agent.py
```

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check |
| `POST` | `/` | A2A query handler (used by Gemini Enterprise) |
| `POST` | `/query` | A2A query handler (alias) |
| `GET` | `/health` | Health check |
| `GET` | `/.well-known/agent.json` | A2A agent card for discovery |

