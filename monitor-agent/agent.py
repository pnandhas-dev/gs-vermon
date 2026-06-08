#!/usr/bin/env python3
"""
Gluesync Monitor Agent
======================
A persistent WebSocket client that connects to the Gluesync Core Hub,
subscribes to real-time entity telemetry, caches the latest states in a
local SQLite database, and exposes a REST API for client scripts.

Configuration via environment variables:
    GLUESYNC_API_URL    Base HTTPS URL of Core Hub  (default: https://gluesync-core-hub:1717)
    GLUESYNC_USER       API username                (default: admin)
    GLUESYNC_PASSWORD   API password                (required)
    AGENT_PORT          REST API port               (default: 8080)
    RECONNECT_DELAY     Seconds between reconnects  (default: 5)
"""

import os
import json
import ssl
import time
import sqlite3
import logging
import threading
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import websocket
from flask import Flask, jsonify

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
API_URL       = os.environ.get("GLUESYNC_API_URL",  "https://gluesync-core-hub:1717")
WS_URL        = API_URL.replace("https://", "wss://").replace("http://", "ws://").rstrip("/") + "/ui"
USERNAME      = os.environ.get("GLUESYNC_USER",     "admin")
PASSWORD      = os.environ.get("GLUESYNC_PASSWORD", "")
AGENT_PORT    = int(os.environ.get("AGENT_PORT",    "8080"))
RECONNECT_DELAY = int(os.environ.get("RECONNECT_DELAY", "5"))
DB_PATH       = os.environ.get("CACHE_DB_PATH",     "/app/monitor_cache.db")

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("monitor-agent")

# ─────────────────────────────────────────────
# SSL Context (ignore self-signed certs)
# ─────────────────────────────────────────────
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

_ssl_opts = {"cert_reqs": ssl.CERT_NONE}   # for websocket-client

# ─────────────────────────────────────────────
# Local SQLite Cache
# ─────────────────────────────────────────────
_db_lock = threading.Lock()

def init_db():
    """Create the cache tables if they don't exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS pipelines (
                pipeline_id         TEXT PRIMARY KEY,
                name                TEXT,
                is_in_maintenance   INTEGER DEFAULT 0,
                last_updated        TEXT
            );

            CREATE TABLE IF NOT EXISTS entities (
                entity_id           TEXT PRIMARY KEY,
                pipeline_id         TEXT,
                source_table        TEXT,
                target_table        TEXT,
                source_agent_id     TEXT,
                target_agent_id     TEXT,
                group_id            TEXT,
                last_updated        TEXT
            );

            CREATE TABLE IF NOT EXISTS entity_status (
                entity_id           TEXT PRIMARY KEY,
                pipeline_id         TEXT,
                migration_active    INTEGER DEFAULT 0,
                sync_active         INTEGER DEFAULT 0,
                migrated_rows       INTEGER,
                total_rows          INTEGER,
                is_completed        INTEGER DEFAULT 0,
                ws_message_type     TEXT,
                last_updated        TEXT
            );

            CREATE TABLE IF NOT EXISTS agent_info (
                key     TEXT PRIMARY KEY,
                value   TEXT
            );
        """)
    log.info("SQLite cache initialized at %s", DB_PATH)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def upsert_pipeline(pipeline_id, name, is_in_maintenance):
    """Upsert pipeline. If name is None, preserve the existing name in the DB."""
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            if name is not None:
                conn.execute("""
                    INSERT INTO pipelines (pipeline_id, name, is_in_maintenance, last_updated)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(pipeline_id) DO UPDATE SET
                        name = excluded.name,
                        is_in_maintenance = excluded.is_in_maintenance,
                        last_updated = excluded.last_updated
                """, (pipeline_id, name, int(is_in_maintenance), _now_iso()))
            else:
                # Preserve existing name; only update maintenance flag
                conn.execute("""
                    INSERT INTO pipelines (pipeline_id, name, is_in_maintenance, last_updated)
                    VALUES (?, '', ?, ?)
                    ON CONFLICT(pipeline_id) DO UPDATE SET
                        is_in_maintenance = excluded.is_in_maintenance,
                        last_updated = excluded.last_updated
                """, (pipeline_id, int(is_in_maintenance), _now_iso()))


def upsert_entity(entity_id, pipeline_id, source_table, target_table,
                   source_agent_id, target_agent_id, group_id):
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO entities (entity_id, pipeline_id, source_table, target_table,
                    source_agent_id, target_agent_id, group_id, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entity_id) DO UPDATE SET
                    pipeline_id     = excluded.pipeline_id,
                    source_table    = excluded.source_table,
                    target_table    = excluded.target_table,
                    source_agent_id = excluded.source_agent_id,
                    target_agent_id = excluded.target_agent_id,
                    group_id        = excluded.group_id,
                    last_updated    = excluded.last_updated
            """, (entity_id, pipeline_id, source_table, target_table,
                  source_agent_id, target_agent_id, group_id, _now_iso()))


def upsert_entity_status(entity_id, pipeline_id, migration_active, sync_active,
                          migrated_rows, total_rows, is_completed, msg_type):
    """Upsert full entity status. migrated_rows/total_rows may be None to preserve existing."""
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            if migrated_rows is None and total_rows is None:
                # Status-only update: only touch mig/sync flags, preserve row counts
                conn.execute("""
                    INSERT INTO entity_status (entity_id, pipeline_id, migration_active,
                        sync_active, migrated_rows, total_rows, is_completed,
                        ws_message_type, last_updated)
                    VALUES (?, ?, ?, ?, NULL, NULL, ?, ?, ?)
                    ON CONFLICT(entity_id) DO UPDATE SET
                        pipeline_id      = excluded.pipeline_id,
                        migration_active = excluded.migration_active,
                        sync_active      = excluded.sync_active,
                        is_completed     = excluded.is_completed,
                        ws_message_type  = excluded.ws_message_type,
                        last_updated     = excluded.last_updated
                """, (entity_id, pipeline_id, int(migration_active), int(sync_active),
                      int(is_completed), msg_type, _now_iso()))
            else:
                conn.execute("""
                    INSERT INTO entity_status (entity_id, pipeline_id, migration_active,
                        sync_active, migrated_rows, total_rows, is_completed,
                        ws_message_type, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(entity_id) DO UPDATE SET
                        pipeline_id     = excluded.pipeline_id,
                        migration_active = excluded.migration_active,
                        sync_active     = excluded.sync_active,
                        migrated_rows   = excluded.migrated_rows,
                        total_rows      = excluded.total_rows,
                        is_completed    = excluded.is_completed,
                        ws_message_type = excluded.ws_message_type,
                        last_updated    = excluded.last_updated
                """, (entity_id, pipeline_id, int(migration_active), int(sync_active),
                      migrated_rows, total_rows, int(is_completed), msg_type, _now_iso()))


def upsert_entity_metrics(entity_id, pipeline_id, migrated_rows, total_rows, is_completed, msg_type):
    """Update row-count metrics only; preserves existing migration_active/sync_active flags."""
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                INSERT INTO entity_status (entity_id, pipeline_id, migration_active,
                    sync_active, migrated_rows, total_rows, is_completed,
                    ws_message_type, last_updated)
                VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?)
                ON CONFLICT(entity_id) DO UPDATE SET
                    migrated_rows   = excluded.migrated_rows,
                    total_rows      = excluded.total_rows,
                    is_completed    = excluded.is_completed,
                    ws_message_type = excluded.ws_message_type,
                    last_updated    = excluded.last_updated
            """, (entity_id, pipeline_id, migrated_rows, total_rows,
                  int(is_completed), msg_type, _now_iso()))


def _build_subscriptions_from_cache():
    """Read entity IDs from SQLite cache and build the subscription payload list."""
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT entity_id, pipeline_id FROM entities").fetchall()
    subs = {}
    for row in rows:
        pid = row["pipeline_id"]
        eid = row["entity_id"]
        if pid not in subs:
            subs[pid] = []
        subs[pid].append(eid)
    return [{"pipelineId": pid, "entities": eids} for pid, eids in subs.items()]




def get_all_status():
    """Return a joined view of pipelines + entities + status for the REST API."""
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            pipelines = {r["pipeline_id"]: dict(r) for r in
                         conn.execute("SELECT * FROM pipelines")}
            entities  = {r["entity_id"]:  dict(r) for r in
                         conn.execute("SELECT * FROM entities")}
            statuses  = {r["entity_id"]:  dict(r) for r in
                         conn.execute("SELECT * FROM entity_status")}

    result = {"pipelines": []}
    for pid, pipe in pipelines.items():
        pipe_out = {
            "pipeline_id":      pid,
            "name":             pipe["name"],
            "maintenance_mode": bool(pipe["is_in_maintenance"]),
            "last_updated":     pipe["last_updated"],
            "entities":         []
        }
        for eid, ent in entities.items():
            if ent["pipeline_id"] != pid:
                continue
            st = statuses.get(eid, {})
            pipe_out["entities"].append({
                "entity_id":        eid,
                "group_id":         ent.get("group_id"),
                "source_table":     ent.get("source_table"),
                "target_table":     ent.get("target_table"),
                "source_agent_id":  ent.get("source_agent_id"),
                "target_agent_id":  ent.get("target_agent_id"),
                "entity_last_seen": ent.get("last_updated"),
                "status": {
                    "migration_active": bool(st.get("migration_active", 0)),
                    "sync_active":      bool(st.get("sync_active",      0)),
                    "migrated_rows":    st.get("migrated_rows"),
                    "total_rows":       st.get("total_rows"),
                    "is_completed":     bool(st.get("is_completed",     0)),
                    "ws_message_type":  st.get("ws_message_type"),
                    "last_updated":     st.get("last_updated"),
                }
            })
        result["pipelines"].append(pipe_out)

    # Agent metadata
    result["agent"] = {
        "api_url":       API_URL,
        "ws_url":        WS_URL,
        "server_time":   _now_iso(),
    }
    return result


# ─────────────────────────────────────────────
# Gluesync REST API helpers
# ─────────────────────────────────────────────
def _api_request(path, method="GET", payload=None, token=None):
    url = API_URL.rstrip("/") + path
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body, headers=headers, method=method)
    with urlopen(req, context=_ssl_ctx, timeout=10) as r:
        return json.loads(r.read().decode("utf-8"))


def login():
    log.info("Logging in to %s as %s …", API_URL, USERNAME)
    data = _api_request("/authentication/login", method="POST",
                         payload={"username": USERNAME, "password": PASSWORD})
    token = (data.get("apiToken") or data.get("token") or
             data.get("jwt") or data.get("accessToken"))
    if not token:
        raise RuntimeError(f"Could not parse API token from login response: {data}")
    log.info("Login successful")
    return token


def fetch_and_cache_metadata(token):
    """Fetch pipelines + entities from REST API and populate the SQLite cache."""
    log.info("Fetching pipeline metadata …")
    pipelines = _api_request("/pipelines", token=token)

    all_subscriptions = []   # list of {pipelineId, entities: [eid, …]}

    for pipe in pipelines:
        pid  = pipe["id"]
        name = pipe.get("name", "")
        is_maint = pipe.get("isInMaintenanceMode", False)
        upsert_pipeline(pid, name, is_maint)

        log.info("  Pipeline %s (%s) – fetching entities …", name, pid)
        entities = _api_request(f"/pipelines/{pid}/entities", token=token)

        entity_ids = []
        for item in entities:
            ent = item.get("entity") if "entity" in item else item
            eid      = ent.get("entityId")
            group_id = ent.get("groupId", "_default")
            if not eid:
                continue

            source_table = target_table = None
            source_agent = target_agent = None
            for ae in ent.get("agentEntities", []):
                role       = ae.get("entityType", {}).get("type")
                agent_id   = ae.get("agentId")
                tbl        = ae.get("table", {})
                table_name = (f"{tbl.get('schema')}.{tbl.get('name')}"
                              if tbl else ae.get("entityName"))
                if role == "Source":
                    source_table, source_agent = table_name, agent_id
                elif role == "Target":
                    target_table, target_agent = table_name, agent_id

            upsert_entity(eid, pid, source_table, target_table,
                          source_agent, target_agent, group_id)
            entity_ids.append(eid)

        if entity_ids:
            all_subscriptions.append({"pipelineId": pid, "entities": entity_ids})

    log.info("Cached %d pipelines, %d entities", len(pipelines),
             sum(len(s["entities"]) for s in all_subscriptions))
    return all_subscriptions


def send_subscriptions(token, subscriptions):
    """PUT /ui/entities-metrics-subscription for each pipeline."""
    for sub in subscriptions:
        pid = sub["pipelineId"]
        try:
            _api_request("/ui/entities-metrics-subscription",
                          method="PUT", payload=sub, token=token)
            log.info("Subscribed to %d entities in pipeline %s",
                     len(sub["entities"]), pid)
        except Exception as e:
            log.warning("Subscription failed for pipeline %s: %s", pid, e)


# ─────────────────────────────────────────────
# WebSocket message handlers
# ─────────────────────────────────────────────
def _handle_message(raw):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        log.debug("Non-JSON WS frame: %s", raw[:80])
        return

    msg_type = msg.get("type", "")
    content  = msg.get("content", {})

    if msg_type == "PipelineStatusMessage":
        for ps in content.get("pipelinesStatus", []):
            pid = ps.get("pipelineId")
            if pid:
                # Pass name=None so we preserve whatever name was fetched via REST
                upsert_pipeline(pid, None, bool(ps.get("isInMaintenanceMode", False)))
        log.debug("PipelineStatusMessage processed")

    elif msg_type == "EntityStatusMessage":
        # Primary entity status message: isMigrationActive, isSyncActive
        for item in content.get("entitiesStatus", []):
            eid              = item.get("entityId")
            pid              = item.get("pipelineId")
            migration_active = bool(item.get("isMigrationActive", False))
            sync_active      = bool(item.get("isSyncActive",      False))
            if eid:
                # Status-only update — preserve existing row counts
                upsert_entity_status(eid, pid, migration_active, sync_active,
                                     None, None, False, msg_type)
                log.info("EntityStatus: %s mig=%s sync=%s",
                         eid, migration_active, sync_active)

    elif msg_type == "MetricsMessage":
        # Per-entity throughput + snapshot metrics, nested under pipelinesMetrics
        pipelines_metrics = content.get("pipelinesMetrics", {})
        for pid, pipe_data in pipelines_metrics.items():
            entities_metrics = pipe_data.get("entitiesMetrics", {})
            for eid, metric_list in entities_metrics.items():
                if not metric_list:
                    continue
                # Each entity can have multiple agent-pair entries; take first
                m = metric_list[0]
                snap = m.get("snapshotMetrics", {})
                total_rows    = snap.get("totalSnapshotRows")
                migrated_rows = snap.get("snapshotCount")
                progress      = snap.get("snapshotProgress", 0.0)
                is_completed  = (progress >= 1.0) if progress is not None else False
                # Preserve migration/sync flags (don't overwrite with None)
                upsert_entity_metrics(eid, pid, migrated_rows, total_rows, is_completed, msg_type)
                log.debug("MetricsMessage: %s rows=%s/%s completed=%s",
                          eid, migrated_rows, total_rows, is_completed)

    elif msg_type == "LicenseStatusMessage":
        log.info("License: %s (expires %s)",
                 content.get("licenseStatus"), content.get("expiresAt"))

    elif msg_type == "NotificationMessage":
        log.debug("Unread notifications: %s", content.get("unreadCount"))

    elif msg_type == "ConnectedExternalModulesMessage":
        log.debug("ConnectedExternalModules: %s modules",
                  len(content.get("externalModules", [])))

    else:
        log.info("Unhandled WS message type: %s (keys: %s)",
                 msg_type, list(content.keys()) if content else [])


# ─────────────────────────────────────────────
# WebSocket Worker (runs in a daemon thread)
# ─────────────────────────────────────────────
_worker_token    = None   # shared between worker and main
_worker_stop     = threading.Event()
_refresh_lock    = threading.Lock()
_last_refresh    = 0.0
REFRESH_INTERVAL = 300  # re-fetch metadata every 5 minutes


def _ws_worker_loop():
    global _worker_token, _last_refresh

    while not _worker_stop.is_set():
        try:
            # (Re-)Login if no token
            if not _worker_token:
                _worker_token = login()

            # (Re-)fetch metadata if stale
            now = time.time()
            with _refresh_lock:
                if now - _last_refresh > REFRESH_INTERVAL:
                    subs = fetch_and_cache_metadata(_worker_token)
                    _last_refresh = time.time()
                else:
                    # Re-build subscriptions from cached entities even if metadata not stale
                    subs = _build_subscriptions_from_cache()

            # WebSocket handshake
            log.info("Connecting to WebSocket: %s", WS_URL)

            _done = threading.Event()
            _subs = subs   # capture for closure
            _tok  = _worker_token

            def on_open(ws):
                log.info("WebSocket connection established")
                # IMPORTANT: subscription must be sent AFTER WS is open
                send_subscriptions(_tok, _subs)

            def on_message(ws, message):
                _handle_message(message)

            def on_error(ws, error):
                log.warning("WebSocket error: %s", error)

            def on_close(ws, code, msg):
                log.info("WebSocket closed: %s %s", code, msg)
                _done.set()

            ws = websocket.WebSocketApp(
                WS_URL,
                header=[f"Authorization: Bearer {_worker_token}"],
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            wst = threading.Thread(
                target=ws.run_forever,
                kwargs={"sslopt": _ssl_opts},
                name="ws-recv",
                daemon=True,
            )
            wst.start()

            # Wait until WS closes or stop event fires
            while not _done.is_set() and not _worker_stop.is_set():
                _done.wait(timeout=5)

            ws.close()
            wst.join(timeout=5)

        except (HTTPError, URLError) as e:
            log.error("HTTP error in WS worker: %s – retrying in %ds", e, RECONNECT_DELAY)
            _worker_token = None   # force re-login
        except Exception as e:
            log.error("Unexpected error in WS worker: %s – retrying in %ds", e, RECONNECT_DELAY)
            _worker_token = None

        if not _worker_stop.is_set():
            log.info("Reconnecting in %d seconds …", RECONNECT_DELAY)
            _worker_stop.wait(timeout=RECONNECT_DELAY)


# ─────────────────────────────────────────────
# Flask REST API
# ─────────────────────────────────────────────
app = Flask(__name__)


@app.route("/status")
@app.route("/status/")
def status():
    """Return all cached pipeline and entity statuses."""
    return jsonify(get_all_status())


@app.route("/health")
def health():
    """Health-check endpoint."""
    return jsonify({"status": "ok", "time": _now_iso()})


@app.route("/pipelines")
def pipelines():
    """Return list of cached pipelines."""
    data = get_all_status()
    return jsonify({"pipelines": data["pipelines"], "agent": data["agent"]})


@app.route("/entities")
def entities():
    """Return flat list of all cached entities with their statuses."""
    data = get_all_status()
    flat = []
    for pipe in data["pipelines"]:
        for ent in pipe["entities"]:
            flat.append({**ent, "pipeline_id": pipe["pipeline_id"],
                                "pipeline_name": pipe["name"]})
    return jsonify({"entities": flat, "agent": data["agent"]})


# ─────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if not PASSWORD:
        raise SystemExit("GLUESYNC_PASSWORD environment variable is required")

    init_db()

    # Start WS worker in background
    worker = threading.Thread(target=_ws_worker_loop, name="ws-worker", daemon=True)
    worker.start()

    log.info("Gluesync Monitor Agent REST API starting on port %d", AGENT_PORT)
    # For gunicorn, the app object is used. For direct run, use Flask dev server.
    app.run(host="0.0.0.0", port=AGENT_PORT, threaded=True)
