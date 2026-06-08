# _gsvermon — Gluesync Verification & Monitoring

A monitoring and proxying agent for [Gluesync](https://gluesync.com/) replication pipelines.
It subscribes to Gluesync's live WebSocket telemetry, caches entity status in a local SQLite
database, and exposes a clean REST API that CLI scripts (and future web UIs) can query without
touching `gluesync.db` directly.

> **Tooling convention:** Always use **`podman-compose`** (hyphen, the Python package) —
> not `podman compose` (space, which delegates to `docker-compose`).
> Using both tools splits container ownership and makes `ps`, `up`, and `down` unreliable.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Software Stack](#2-software-stack)
3. [Integration Flow](#3-integration-flow)
4. [Monitor Agent — Internal REST API](#4-monitor-agent--internal-rest-api)
5. [CLI Tool — `list_entities.py`](#5-cli-tool--list_entitiespy)
6. [Getting Started](#6-getting-started)
7. [Configuration Reference](#7-configuration-reference)
8. [Container Layout](#8-container-layout)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                  Gluesync Stack (gluesync-net)               │
│                                                             │
│  ┌───────────────────┐        ┌────────────────────────┐   │
│  │  gluesync-core-hub│        │  gluesync-conductor    │   │
│  │  :1717 (internal) │        │  gluesync-chronos      │   │
│  │  :1718 (host)     │        │  gluesync-grafana      │   │
│  │                   │        │  gluesync-prometheus   │   │
│  │  REST API (HTTPS) │        │  gluesync-traefik      │   │
│  │  WebSocket (WSS)  │        └────────────────────────┘   │
│  └─────────┬─────────┘                                      │
│            │                                                 │
│  ┌─────────▼──────────────────────────────────────────┐    │
│  │          gsvermon-monitor-agent  :8080              │    │
│  │                                                     │    │
│  │  ┌──────────────┐   ┌─────────────────────────┐   │    │
│  │  │  WS Worker   │──▶│  SQLite Cache            │   │    │
│  │  │  Thread      │   │  (monitor_cache.db)      │   │    │
│  │  └──────────────┘   └───────────┬─────────────┘   │    │
│  │                                 │                   │    │
│  │                      ┌──────────▼──────────┐       │    │
│  │                      │   Flask REST API     │       │    │
│  │                      │   /health /status    │       │    │
│  │                      │   /pipelines         │       │    │
│  │                      │   /entities          │       │    │
│  │                      └──────────────────────┘       │    │
│  └─────────────────────────────────────────────────────┘    │
│            │                                                 │
│  ┌─────────▼───────────┐   ┌──────────────────────────┐    │
│  │  gsvermon-mssql     │   │  Host / local scripts     │    │
│  │  (Azure SQL Edge)   │   │  list_entities.py         │    │
│  │  :1433              │   │  (--agent / --api / --db) │    │
│  └─────────────────────┘   └──────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Key design decisions

| Decision | Rationale |
|---|---|
| **Never bind `gluesync.db` directly in production** | virtiofs mounts cause SQLite `disk I/O error (522)` under concurrent access |
| **WebSocket subscription, not polling** | The Gluesync UI itself uses WSS push; we mirror the same flow |
| **SQLite as the agent's local cache** | Zero external dependency; a single thread-safe write path protects it |
| **Flask REST API in the same container** | CLI scripts query HTTP instead of SQLite, staying decoupled from the cache format |
| **Bearer-token-only WebSocket auth** | Confirmed: Gluesync rejects `?token=` query params; only header auth works |

---

## 2. Software Stack

### `_gsvermon` project (host-side)

| Component | Technology | Version |
|---|---|---|
| CLI monitoring script | Python 3 stdlib only | 3.9+ |
| Environment config | `.env` file (key=value) | — |
| Container orchestration | Podman + podman-compose | ≥4.0 |

### `monitor-agent` container

| Component | Technology | Version |
|---|---|---|
| Runtime | Python | 3.11-slim |
| WebSocket client | `websocket-client` | ≥1.8 |
| REST API framework | Flask | ≥3.0 |
| Cache database | SQLite 3 (built-in) | — |
| Base image | `python:3.11-slim` | — |

### Gluesync stack (external, read-only)

| Service | Image | Port |
|---|---|---|
| Core Hub | `molo17/gluesync-core-hub:2.2.7.1` | 1718→1717 |
| Conductor | `molo17/gluesync-conductor:0.8.2` | — |
| Chronos | `molo17/gluesync-chronos:0.1.52` | — |
| Prometheus | `molo17/prometheus:3.8.0` | 9090 |
| Grafana | `molo17/grafana:12.3.0` | 3000 |
| Traefik | `molo17/traefik:3.6.2` | 80/443 |

### Target database (gsvermon-managed)

| Service | Image | Port |
|---|---|---|
| SQL Server (Azure SQL Edge) | `mcr.microsoft.com/azure-sql-edge` | 1433 |

---

## 3. Integration Flow

### 3.1 Startup sequence

```
monitor-agent starts
       │
       ▼
  POST /authentication/login
  ──────────────────────────▶  gluesync-core-hub
  ◀──────────────────────────  { apiToken: "eyJ..." }
       │
       ▼
  GET /pipelines           (enumerate pipelines)
  GET /pipelines/{id}/entities   (enumerate entities per pipeline)
  ──────────────────────────▶  gluesync-core-hub
  ◀──────────────────────────  pipeline + entity metadata
       │
       ▼  (cache to SQLite: pipelines, entities tables)
       │
       ▼
  PUT /ui/entities-metrics-subscription
  Body: { "pipelineId": "<pid>", "entities": ["<eid1>", "<eid2>"] }
  ──────────────────────────▶  gluesync-core-hub
  ◀──────────────────────────  200 OK  (server now pushes telemetry to us)
       │
       ▼
  WebSocket Upgrade
  GET wss://gluesync-core-hub:1717/ui
  Header: Authorization: Bearer <token>
  ──────────────────────────▶  gluesync-core-hub
  ◀══════════════════════════  (persistent bi-directional stream)
```

### 3.2 Ongoing telemetry (while WS is open)

```
gluesync-core-hub
     │  push PipelineStatusMessage     → update pipelines.is_in_maintenance
     │  push EntityMetricsMessage      → update entity_status (mig/sync/rows)
     │  push EntityStatusMessage       → (fallback) same as above
     │  push LicenseStatusMessage      → log only
     │  push NotificationMessage       → log only
     ▼
  monitor_cache.db  (thread-safe via _db_lock)
     │
     ▼
  Flask REST API (any HTTP client)
```

### 3.3 Reconnect & metadata refresh

```
On WS close / error:
  ├─ force re-login (clear cached token)
  ├─ wait RECONNECT_DELAY seconds (default: 5)
  └─ restart from step 1

Every REFRESH_INTERVAL seconds (default: 300 / 5 min):
  ├─ re-fetch /pipelines and /pipelines/{id}/entities
  ├─ re-PUT /ui/entities-metrics-subscription
  └─ re-open WebSocket
```

### 3.4 Gluesync WebSocket message types

| `type` field | Trigger | Fields used |
|---|---|---|
| `PipelineStatusMessage` | Periodic / on change | `pipelinesStatus[].pipelineId`, `isInMaintenanceMode` |
| `EntityMetricsMessage` | On entity state change | `entitiesMetrics[].entityId`, `migrationActive`, `syncActive`, `migratedRows`, `totalRowCount`, `isCompleted` |
| `EntityStatusMessage` | (older versions) | Same as above, under `entities[]` key |
| `LicenseStatusMessage` | Periodic | `licenseStatus`, `expiresAt` |
| `NotificationMessage` | On notification | `unreadCount` |

> **Important:** WebSocket authentication via `?token=` query param is rejected by Gluesync Core Hub.
> Only `Authorization: Bearer <token>` in the HTTP upgrade header is accepted.

---

## 4. Monitor Agent — Internal REST API

Base URL (default): `http://localhost:8080`

All responses are JSON. No authentication required (internal network only).

---

### `GET /health`

Health check. Returns immediately from the Flask thread without touching SQLite.

**Response**
```json
{
  "status": "ok",
  "time": "2026-06-07T10:51:43.371897+00:00"
}
```

---

### `GET /status`

Full snapshot — all pipelines, all entities, and their latest cached status.

**Response schema**
```json
{
  "agent": {
    "api_url": "https://gluesync-core-hub:1717",
    "ws_url":  "wss://gluesync-core-hub:1717/ui",
    "server_time": "<ISO-8601 UTC>"
  },
  "pipelines": [
    {
      "pipeline_id":    "<8-char hex>",
      "name":           "1st pipeline",
      "maintenance_mode": false,
      "last_updated":   "<ISO-8601 UTC>",
      "entities": [
        {
          "entity_id":       "<8-char hex>",
          "group_id":        "_default",
          "source_table":    "GSLIBTST.CUSTOMERZ",
          "target_table":    "dbo.customerz",
          "source_agent_id": "<8-char hex>",
          "target_agent_id": "<8-char hex>",
          "entity_last_seen": "<ISO-8601 UTC>",
          "status": {
            "migration_active": false,
            "sync_active":      true,
            "migrated_rows":    150000,
            "total_rows":       150000,
            "is_completed":     true,
            "ws_message_type":  "EntityMetricsMessage",
            "last_updated":     "<ISO-8601 UTC>"
          }
        }
      ]
    }
  ]
}
```

**Status fields explained**

| Field | Source | Meaning |
|---|---|---|
| `migration_active` | WS `EntityMetricsMessage` | Snapshot/initial load is in progress |
| `sync_active` | WS `EntityMetricsMessage` | CDC replication is running |
| `migrated_rows` | WS `EntityMetricsMessage` | Rows copied so far in migration phase |
| `total_rows` | WS `EntityMetricsMessage` | Total source rows for migration |
| `is_completed` | WS `EntityMetricsMessage` | Migration completed (rows fully copied) |
| `ws_message_type` | Agent internal | Which WS message last updated this status |
| `last_updated` | Agent internal | UTC timestamp of last WS update |

> `status` fields are `null` until the first `EntityMetricsMessage` arrives for that entity
> (which happens when an entity is started or its state changes).

---

### `GET /pipelines`

Same as `/status` but without entity-level detail. Useful for a quick pipeline health check.

**Response schema**
```json
{
  "agent": { ... },
  "pipelines": [
    {
      "pipeline_id": "ef9c88fd",
      "name": "1st pipeline",
      "maintenance_mode": false,
      "last_updated": "...",
      "entities": [ ... ]
    }
  ]
}
```

---

### `GET /entities`

Flat list of all entities across all pipelines, with their status and parent pipeline context.

**Response schema**
```json
{
  "agent": { ... },
  "entities": [
    {
      "entity_id":       "b419b51e",
      "pipeline_id":     "ef9c88fd",
      "pipeline_name":   "1st pipeline",
      "group_id":        "_default",
      "source_table":    "GSLIBTST.CUSTOMERZ",
      "target_table":    "dbo.customerz",
      "source_agent_id": "a4186af0",
      "target_agent_id": "c82aea44",
      "entity_last_seen": "...",
      "status": { ... }
    }
  ]
}
```

---

### SQLite cache schema (internal)

Located at `/app/monitor_cache.db` inside the container.

```sql
CREATE TABLE pipelines (
    pipeline_id       TEXT PRIMARY KEY,
    name              TEXT,
    is_in_maintenance INTEGER DEFAULT 0,
    last_updated      TEXT           -- ISO-8601 UTC
);

CREATE TABLE entities (
    entity_id         TEXT PRIMARY KEY,
    pipeline_id       TEXT,
    source_table      TEXT,
    target_table      TEXT,
    source_agent_id   TEXT,
    target_agent_id   TEXT,
    group_id          TEXT,
    last_updated      TEXT
);

CREATE TABLE entity_status (
    entity_id         TEXT PRIMARY KEY,
    pipeline_id       TEXT,
    migration_active  INTEGER DEFAULT 0,
    sync_active       INTEGER DEFAULT 0,
    migrated_rows     INTEGER,
    total_rows        INTEGER,
    is_completed      INTEGER DEFAULT 0,
    ws_message_type   TEXT,
    last_updated      TEXT
);
```

---

## 5. CLI Tool — `list_entities.py`

### Synopsis

```
python3 list_entities.py [OPTIONS]
```

### Operating modes

The script has **three mutually exclusive data sources** selected by flags:

| Flag | Mode | Data source |
|---|---|---|
| *(default)* | **API mode** | Direct HTTPS calls to Gluesync Core Hub REST API |
| `--db` | **DB mode** | Direct SQLite read of `gluesync.db` (⚠️ avoid in production) |
| `--agent` | **Agent mode** | HTTP calls to the monitor-agent REST API |

### All options

```
  --agent [AGENT_URL]   Query the Gluesync Monitor Agent REST API.
                        Default URL: http://localhost:8080
                        Example: --agent http://192.168.1.50:8080

  --api-url URL         Base URL for the Gluesync Core Hub API.
                        Default: https://localhost:1718

  --user USERNAME       API login username.
                        Default: admin (or $GLUESYNC_USER)

  --password PASSWORD   API login password.
                        Default: $GLUESYNC_PASSWORD, or prompted interactively.

  --db [PATH]           Use local SQLite database instead of API/agent.
                        Default path: auto-discover gluesync.db

  --env PATH            Path to .env file for credentials.
                        Auto-discovered from: ./.env, ../_gsvermon/.env

  --json                Output in JSON format (machine-readable).

  --no-live             Skip live SQL Server row count query.

  --no-color            Disable ANSI color codes in output.
```

### Usage examples

**1. Agent mode (recommended for production)**
```bash
# Use local monitor agent (default URL: http://localhost:8080)
python3 list_entities.py --agent

# Use a remote agent
python3 list_entities.py --agent http://192.168.1.50:8080

# JSON output, skip live DB counts
python3 list_entities.py --agent --json --no-live
```

**2. API mode (direct to Gluesync Core Hub)**
```bash
# Will prompt for password if not in .env
python3 list_entities.py

# Specify credentials explicitly
python3 list_entities.py --api-url https://localhost:1718 --user admin --password P@ssw0rd

# JSON output
python3 list_entities.py --json --no-live
```

**3. Database mode (local dev / debugging only)**
```bash
# Auto-discover gluesync.db
python3 list_entities.py --db

# Specify path explicitly
python3 list_entities.py --db /path/to/gluesync.db
```

### Output columns (CLI table)

```
Entity ID  | Source Table         | Target Table     | Method   | Migration    | Sync (CDC) | Checkpoint Rows | Live Target
b419b51e   | GSLIBTST.CUSTOMERZ   | dbo.customerz    | UPSERT   | COMPLETED    | ACTIVE     | 150k/150k(100%) | 150,000 ✓
5871134e   | TBLIBTST.CHDRPF50    | dbo.CHDRPF50     | UPSERT   | INACTIVE     | ACTIVE     | N/A             | 45,231
```

| Column | Source |
|---|---|
| **Entity ID** | Gluesync internal entity UUID (truncated to 8 chars) |
| **Source Table** | `schema.table` on the source agent (e.g. IBM iSeries) |
| **Target Table** | `schema.table` on the target agent (e.g. SQL Server) |
| **Method** | Snapshot write method: `UPSERT` or `INSERT` |
| **Migration** | `ACTIVE` (in progress) / `COMPLETED` / `INACTIVE` |
| **Sync (CDC)** | `ACTIVE` (CDC running) / `INACTIVE` |
| **Checkpoint Rows** | `migrated / total (%)` from WS telemetry |
| **Live Target** | Live `COUNT(*)` from SQL Server (green ✓ = matches source) |

### JSON output structure

```bash
python3 list_entities.py --agent --json --no-live | python3 -m json.tool
```

```json
{
  "metadata": {
    "mode": "agent",
    "agent_url": "http://localhost:8080",
    "env_path": "/Users/.../._gsvermon/.env",
    "live_target_counts_retrieved": false
  },
  "pipelines": [
    {
      "id": "ef9c88fd",
      "name": "1st pipeline",
      "description": "",
      "maintenance_mode": false,
      "entities": [
        {
          "entity_id": "b419b51e",
          "group_id": "_default",
          "source": { "table": "GSLIBTST.CUSTOMERZ", "agent_id": "a4186af0" },
          "target": { "table": "dbo.customerz",      "agent_id": "c82aea44", "live_row_count": null },
          "replication": { "migration_active": false, "sync_active": false, "write_method": "UPSERT" },
          "checkpoint":  { "total_rows": null, "migrated_rows": null, "is_completed": false }
        }
      ]
    }
  ]
}
```

---

## 6. Getting Started

### Prerequisites

- macOS (or Linux) host with **Podman ≥ 4.0** and **podman-compose**
- Gluesync stack already running (produces the `gluesync-net` Podman network)
- Python 3.9+ on the host (for running `list_entities.py`)
- Optional: `pymssql` for live SQL Server row counts

### Step 1 — Clone / enter the project directory

```bash
cd /path/to/_gsvermon
```

### Step 2 — Configure `.env`

```bash
cp .env.example .env   # if example exists, otherwise edit .env directly
```

Minimum required variables:

```dotenv
# Gluesync credentials
GLUESYNC_USER=admin
GLUESYNC_PASSWORD=P@ssw0rd

# SQL Server target (for live row counts)
MSSQL_HOST=192.168.1.123
MSSQL_USER=sa
MSSQL_PASSWORD=YourSqlPassword123!
MSSQL_DATABASE=gstrgtdb
```

### Step 3 — Build and start the monitor agent

```bash
# Build the container image
podman-compose build monitor-agent

# Start the monitor agent (and mssql if needed)
podman-compose up -d monitor-agent

# Check it is running
podman ps
```

Expected output within a few seconds:
```
CONTAINER ID  IMAGE                              COMMAND         STATUS
f5879dc982b1  localhost/gsvermon_monitor-agent   python agent.py  Up X minutes  0.0.0.0:8080->8080/tcp  gsvermon-monitor-agent
```

### Step 4 — Verify connectivity

```bash
# Health check
curl http://localhost:8080/health

# Full status dump
curl http://localhost:8080/status | python3 -m json.tool

# Check agent logs
podman logs gsvermon-monitor-agent
```

Healthy log output looks like:
```
2026-06-07T10:49:29 [ws-worker] INFO - Login successful
2026-06-07T10:49:29 [ws-worker] INFO -   Pipeline 1st pipeline (ef9c88fd) – fetching entities …
2026-06-07T10:49:29 [ws-worker] INFO - Cached 1 pipelines, 2 entities
2026-06-07T10:49:29 [ws-worker] INFO - Subscribed to 2 entities in pipeline ef9c88fd
2026-06-07T10:49:29 [ws-recv] INFO  - WebSocket connection established
2026-06-07T10:49:29 [ws-recv] INFO  - License: ACTIVE (expires 2026-06-26T...)
```

### Step 5 — Run `list_entities.py`

```bash
# Install optional live-count dependency (once)
pip3 install pymssql

# Agent mode (recommended)
python3 list_entities.py --agent

# API mode (requires Gluesync Core Hub reachable on port 1718)
python3 list_entities.py

# Skip live SQL Server counts if target is offline
python3 list_entities.py --agent --no-live
```

### Stopping services

```bash
# Stop the monitor agent only
podman-compose stop monitor-agent

# Stop all gsvermon services
podman-compose down

# Or use the convenience scripts
./stop-podman.sh
```

---

## 7. Configuration Reference

### `monitor-agent` environment variables

| Variable | Default | Description |
|---|---|---|
| `GLUESYNC_API_URL` | `https://gluesync-core-hub:1717` | Base URL of the Gluesync Core Hub (internal DNS) |
| `GLUESYNC_USER` | `admin` | API login username |
| `GLUESYNC_PASSWORD` | *(required)* | API login password |
| `AGENT_PORT` | `8080` | Port the Flask REST API listens on |
| `RECONNECT_DELAY` | `5` | Seconds to wait before reconnecting WebSocket |
| `CACHE_DB_PATH` | `/app/monitor_cache.db` | Path to the SQLite cache inside the container |

### `.env` file variables (host-side)

| Variable | Used by | Description |
|---|---|---|
| `GLUESYNC_USER` | `list_entities.py` (API mode) | Gluesync admin username |
| `GLUESYNC_PASSWORD` | `list_entities.py` (API mode), `docker-compose.yml` | Gluesync admin password |
| `GLUESYNC_API_URL` | `docker-compose.yml` | Override agent's Core Hub URL |
| `MSSQL_HOST` | `list_entities.py` (live counts) | SQL Server hostname or IP |
| `MSSQL_USER` | `list_entities.py` (live counts) | SQL Server username |
| `MSSQL_PASSWORD` | `list_entities.py` (live counts), `docker-compose.yml` | SQL Server password |
| `MSSQL_DATABASE` | `list_entities.py` (live counts) | Target database name |

---

## 8. Container Layout

```
_gsvermon/
├── .env                      # Credentials and connection settings (not committed)
├── docker-compose.yml        # gsvermon Podman services
├── list_entities.py          # CLI monitoring script (3 modes: agent/api/db)
├── run-podman.sh             # Convenience: start all services
├── stop-podman.sh            # Convenience: stop all services
├── findings_and_workarounds.md  # Discovery notes: WS protocol, auth, msg types
└── monitor-agent/
    ├── Dockerfile            # python:3.11-slim, installs flask + websocket-client
    ├── requirements.txt      # flask>=3.0, websocket-client>=1.8
    └── agent.py              # The monitoring daemon (WS worker + Flask REST API)
```

### Running containers

| Container | Image | Port | Role |
|---|---|---|---|
| `gsvermon-monitor-agent` | `gsvermon_monitor-agent` | `8080` | WS listener + REST API cache |
| `gsvermon-mssql` | `azure-sql-edge` | `1433` | Replication target database |
| `gluesync-gluesync-core-hub-1` | `molo17/gluesync-core-hub:2.2.7.1` | `1718` | Gluesync engine (external) |

### Network topology

```
Host ──── port 8080 ───▶ gsvermon-monitor-agent
Host ──── port 1433 ───▶ gsvermon-mssql
Host ──── port 1718 ───▶ gluesync-core-hub

gsvermon-monitor-agent ──── gluesync-net ───▶ gluesync-core-hub (TLS/WSS)
gsvermon-mssql         ──── gluesync-net ───▶ gluesync-core-hub (replication target)
```

---

## Troubleshooting

### Agent won't start — `GLUESYNC_PASSWORD environment variable is required`
→ Ensure `GLUESYNC_PASSWORD` is set in `.env` and that `docker-compose.yml` passes it through.

### `Error 522 — disk I/O error` when using `--db` mode
→ Use `--agent` or `--api` mode instead. Direct SQLite access conflicts with the virtiofs mount
when the Gluesync container is writing to the same file.

### WebSocket closes immediately
→ Check the token is fresh. The agent auto-re-logins on error. Check `podman logs gsvermon-monitor-agent`.

### Pipeline name shows as empty in `/status`
→ The agent fetches names via REST on startup and preserves them when WS messages arrive.
If empty, the REST fetch may have failed — check the agent startup logs.

### `entity_status` fields are all `null`
→ Gluesync only pushes `EntityMetricsMessage` when an entity is started or its state changes.
Start the entity in the Gluesync UI, and the agent will receive and cache the telemetry.

### `pymssql` not found (live counts skipped)
```bash
pip3 install pymssql   # macOS: may need `brew install freetds` first
```
