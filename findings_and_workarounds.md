# Gluesync API & WebSocket Protocol Findings

This document outlines the internal communication architecture of the Gluesync Core Hub, specifically focusing on how the UI and third-party clients (like our `monitor-agent`) can subscribe to real-time replication telemetry.

---

## 1. Authentication

The Gluesync Core Hub API requires a JWT token for all secured endpoints and WebSocket connections.

### Obtaining the token
```http
POST /authentication/login
Content-Type: application/json

{
  "username": "admin",
  "password": "P@ssw0rd"
}
```
**Response:**
```json
{
  "apiToken": "eyJhbGciOiJIUzI1NiIs...",
  "changeRequired": false
}
```
*Note: Depending on the Gluesync version, the token may be returned under `apiToken`, `token`, `jwt`, or `accessToken`. The `monitor-agent` checks all of these.*

---

## 2. WebSocket Subscription Flow

Subscribing to real-time entity metrics requires a strict sequence of HTTP requests. Failing to follow this exact order results in silent failures where the WebSocket connects but receives no telemetry.

### The Correct Sequence:

1. **Open the WebSocket connection**
   ```http
   GET wss://gluesync-core-hub:1717/ui
   Authorization: Bearer <token>
   ```
   > ⚠️ **CRITICAL FINDING:** The WebSocket handshake **only** accepts the `Authorization: Bearer <token>` header. It completely ignores `?token=<token>` query parameters.

2. **Wait for the connection to establish** (e.g., in the `on_open` callback).

3. **Send the Subscription Request via REST**
   While the WebSocket is open, you must make a separate REST `PUT` call to tell the server which entities to stream to your active WebSocket session.
   ```http
   PUT /ui/entities-metrics-subscription
   Authorization: Bearer <token>
   Content-Type: application/json

   {
     "pipelineId": "ef9c88fd",
     "entities": [
       "b419b51e",
       "5871134e"
     ]
   }
   ```
   > ⚠️ **CRITICAL FINDING:** If you send this `PUT` request *before* the WebSocket is open, the server accepts it (HTTP 200 OK) but **does not push any telemetry** because there is no active session attached to the subscription. It must be sent *after* the WS connection is established.

---

## 3. WebSocket Message Types & Formats

Once subscribed, the server pushes JSON messages over the WebSocket. Every message has a `type` and a `content` object.

### 3.1 `EntityStatusMessage`
**Purpose:** Reports the high-level state of entities (whether migration or CDC sync is actively running).
**Trigger:** Pushed when an entity is started, paused, or stops.

```json
{
  "type": "EntityStatusMessage",
  "content": {
    "entitiesStatus": [
      {
        "pipelineId": "ef9c88fd",
        "entityId": "b419b51e",
        "isMigrationActive": false,
        "isSyncActive": true,
        "isBusy": false,
        "snapshotWriteMethod": "UPSERT"
      }
    ]
  }
}
```
*Note: We extract `isMigrationActive` and `isSyncActive` from this message.*

### 3.2 `MetricsMessage`
**Purpose:** Reports detailed throughput, elapsed times, and snapshot progress.
**Trigger:** Pushed continuously while replication is active (approximately every 5 seconds).

```json
{
  "type": "MetricsMessage",
  "content": {
    "pipelinesMetrics": {
      "ef9c88fd": {
        "pipelineId": "ef9c88fd",
        "entitiesMetrics": {
          "b419b51e": [
            {
              "sourceAgentId": "a4186af0",
              "targetAgentId": "c82aea44",
              "lastMetrics": {
                "lastRowsCount": 82,
                "lastSizeBytes": 6310
                // ... times and throughput details
              },
              "snapshotMetrics": {
                "snapshotCount": 82,
                "totalSnapshotRows": 82,
                "snapshotProgress": 1.0,
                "startSnapshotTime": 1780826559342333300,
                "endSnapshotTime": 1780826564720364000
              }
            }
          ]
        }
      }
    }
  }
}
```
*Note: The array under `entitiesMetrics[entity_id]` represents the connections between Source and Target agents. We extract `snapshotCount`, `totalSnapshotRows`, and `snapshotProgress` from the first element of this array.*

### 3.3 `PipelineStatusMessage`
**Purpose:** Reports pipeline-level maintenance mode and state.
**Trigger:** Pushed on pipeline state changes.

```json
{
  "type": "PipelineStatusMessage",
  "content": {
    "pipelinesStatus": [
      {
        "pipelineId": "ef9c88fd",
        "isInMaintenanceMode": false
      }
    ]
  }
}
```

### 3.4 Auxiliary Messages
These messages are pushed periodically but are generally ignored by our agent.
* **`LicenseStatusMessage`**: Contains `licenseStatus` (e.g., "ACTIVE"), `plan`, and `expiresAt`.
* **`NotificationMessage`**: Contains `unreadCount` for UI notifications.
* **`ConnectedExternalModulesMessage`**: Contains a list of `externalModules` connected to the Core Hub.

---

## 4. Summary of Data Mechanics

1. **`list_entities.py` (CLI Tool)**
   By default, the CLI tool reads from the monitor agent's REST API. The `Checkpoint Rows` column displays the values of `snapshotCount` / `totalSnapshotRows` parsed from the `MetricsMessage` above. It reflects the initial migration load progress, **not** the real-time CDC volume.

2. **The Monitor Agent (`agent.py`)**
   The daemon process manages the delicate WS state machine automatically:
   - Authenticates and caches the token.
   - Opens the WSS connection.
   - Submits the `PUT` subscription immediately upon connection open.
   - Parses `EntityStatusMessage` to update UI state columns.
   - Parses `MetricsMessage` to update snapshot progress counts.
   - Resiliently loops and reconnects if the socket drops.

---

## 5. Pipeline & Entity Manipulation (REST API)

While telemetry streams over WebSockets, controlling the state of pipelines and entities is done via standard REST API calls to the Core Hub. All requests require the `Authorization: Bearer <token>` header.

### 5.1 Entity Controls
Entity state is controlled by sending a `POST` request directly to the entity's action endpoint. The URL does **not** include the `pipelineId`.

**Base path format:** `POST /entities/{entity_id}/{action}`

**Available Actions:**
- **Start:** `POST /entities/{entity_id}/start` (Begins snapshot / CDC replication)
- **Pause:** `POST /entities/{entity_id}/pause` (Suspends CDC replication)
- **Resume:** `POST /entities/{entity_id}/resume` (Resumes CDC replication from the last checkpoint)
- **Stop:** `POST /entities/{entity_id}/stop` (Gracefully stops all activity)
- **Deploy:** `POST /entities/{entity_id}/deploy` (Prepares/validates entity configuration)
- **Truncate:** `POST /entities/{entity_id}/truncate` (Truncates the target table, usually used before a fresh snapshot)

*(All action endpoints return `HTTP 200 OK` on success with no payload.)*

### 5.2 Pipeline Management
Pipelines act as containers for entities. They are managed using standard CRUD REST semantics.

- **List Pipelines:** `GET /pipelines`
- **Get Pipeline Config:** `GET /pipelines/{pipeline_id}`
- **Create Pipeline:** 
  ```http
  POST /pipelines
  Content-Type: application/json

  { "name": "Your Pipeline Name" }
  ```
- **Delete Pipeline:** `DELETE /pipelines/{pipeline_id}`
- **List Entities in Pipeline:** `GET /pipelines/{pipeline_id}/entities`
