#!/usr/bin/env python3
import os
import sys
import json
import re
import sqlite3
import argparse
import ssl
import getpass
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# ANSI color codes
COLOR_GREEN = "\033[92m"
COLOR_RED = "\033[91m"
COLOR_YELLOW = "\033[93m"
COLOR_CYAN = "\033[96m"
COLOR_BLUE = "\033[94m"
COLOR_BOLD = "\033[1m"
COLOR_RESET = "\033[0m"

def supports_color():
    """Returns True if the terminal supports color."""
    plat = sys.platform
    supported_platform = plat != 'win32' or 'ANSICON' in os.environ
    is_a_tty = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
    return supported_platform and is_a_tty

def c_str(text, color, use_color=True):
    """Formats text with ANSI color code if enabled."""
    if use_color:
        return f"{color}{text}{COLOR_RESET}"
    return text

def parse_args():
    parser = argparse.ArgumentParser(
        description="Gluesync Verification & Monitoring: List Entities, Details, and Live Replicated Status"
    )
    # Database mode arguments
    parser.add_argument(
        "--db",
        nargs="?",
        const="DEFAULT",
        help="Use local database mode instead of API mode. Optionally specify the path to gluesync.db."
    )
    # API mode arguments
    parser.add_argument(
        "--api-url",
        default="https://localhost:1718",
        help="Base URL for the Gluesync Core Hub API (default: https://localhost:1718)"
    )
    parser.add_argument(
        "--user",
        help="Username for API login (default: admin, or GLUESYNC_USER env var)"
    )
    parser.add_argument(
        "--password",
        help="Password for API login (or GLUESYNC_PASSWORD env var; prompts if not provided)"
    )
    # General arguments
    parser.add_argument(
        "--env",
        help="Path to .env file for target database credentials"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format"
    )
    parser.add_argument(
        "--no-live",
        action="store_true",
        help="Skip live target database query (useful if offline or target is down)"
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color codes in output"
    )
    parser.add_argument(
        "--agent",
        nargs="?",
        const="http://localhost:8080",
        metavar="AGENT_URL",
        help="Query the Gluesync Monitor Agent REST API instead of direct API/DB. "
             "Optionally specify the agent base URL (default: http://localhost:8080)"
    )
    return parser.parse_args()

def load_env(env_path):
    """Loads key-value pairs from .env file into os.environ."""
    if not env_path or not os.path.exists(env_path):
        return
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, val = line.split('=', 1)
                os.environ[key.strip()] = val.strip()

def find_db_path(db_arg):
    if db_arg and db_arg != "DEFAULT":
        return db_arg
    
    possible_db_paths = [
        "../_gluesync/gluesync-docker/data/core-hub/database/gluesync.db",
        "./_gluesync/gluesync-docker/data/core-hub/database/gluesync.db",
        "/Users/pakornnan/_gluesync/gluesync-docker/data/core-hub/database/gluesync.db"
    ]
    for p in possible_db_paths:
        if os.path.exists(p):
            return os.path.abspath(p)
    return None

def find_env_path(env_arg):
    if env_arg:
        return env_arg
    possible_env_paths = [
        "./.env",
        "../_gsvermon/.env",
        "/Users/pakornnan/_gsvermon/.env"
    ]
    for p in possible_env_paths:
        if os.path.exists(p):
            return os.path.abspath(p)
    return None

# =====================================================================
# API CLIENT IMPLEMENTATION
# =====================================================================

def make_api_request(url, method="GET", data_dict=None, token=None):
    headers = {}
    if data_dict is not None:
        headers['Content-Type'] = 'application/json'
        data_bytes = json.dumps(data_dict).encode('utf-8')
    else:
        data_bytes = None

    if token:
        headers['Authorization'] = f"Bearer {token}"
        
    req = Request(url, data=data_bytes, headers=headers, method=method)
    
    # Disable SSL verification for self-signed certificates used by Gluesync Core Hub
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    with urlopen(req, context=ctx, timeout=5) as response:
        return response.read().decode('utf-8')

def login_to_api(api_url, username, password):
    url = f"{api_url.rstrip('/')}/authentication/login"
    payload = {"username": username, "password": password}
    try:
        res_text = make_api_request(url, method="POST", data_dict=payload)
        res_data = json.loads(res_text)
        # Gluesync v2 returns token in a field like 'apiToken', 'token' or 'jwt'
        token = (
            res_data.get("apiToken")
            or res_data.get("token")
            or res_data.get("jwt")
            or res_data.get("authToken")
            or res_data.get("accessToken")
        )
        if not token:
            if isinstance(res_data, str):
                token = res_data
            else:
                return None, "Error: Could not parse authentication token from API response."
        return token, None
    except HTTPError as e:
        if e.code == 401:
            return None, "Error: Invalid username or password."
        return None, f"Error: Login failed with HTTP status code {e.code}."
    except URLError as e:
        return None, f"Error: Could not connect to API at '{url}': {e.reason}"
    except Exception as e:
        return None, f"Error during login: {str(e)}"

def parse_prometheus_metrics(metrics_text):
    total_counts = {}
    snapshot_counts = {}
    
    pattern = re.compile(r'^(\w+)\{([^}]+)\}\s+(\d+(?:\.\d+)?)')
    for line in metrics_text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        match = pattern.match(line)
        if match:
            metric_name, label_str, val_str = match.groups()
            if metric_name in ('gluesync_total_count', 'gluesync_total_snapshot_count'):
                labels = {}
                for lbl in re.findall(r'(\w+)="([^"]*)"', label_str):
                    labels[lbl[0]] = lbl[1]
                
                pid = labels.get('pipelineId')
                eid = labels.get('entityId')
                val = int(float(val_str))
                
                if pid and eid:
                    if metric_name == 'gluesync_total_count':
                        total_counts[(pid, eid)] = val
                    else:
                        snapshot_counts[(pid, eid)] = val
                        
    return total_counts, snapshot_counts

def get_api_metadata(api_url, token):
    try:
        # 1. Fetch Pipelines
        pipes_url = f"{api_url.rstrip('/')}/pipelines"
        pipes_data = json.loads(make_api_request(pipes_url, token=token))
        
        pipelines = {}
        for p in pipes_data:
            pipelines[p['id']] = {
                'id': p['id'],
                'name': p['name'],
                'description': p.get('description', ''),
                'isInMaintenanceMode': p.get('isInMaintenanceMode', False)
            }
            
        # 2. Fetch Entities and checkpoints via Metrics
        entities_by_id = {}
        checkpoints = {}
        statuses = {}
        
        # Query metrics
        metrics_url = f"{api_url.rstrip('/')}/metrics"
        try:
            metrics_text = make_api_request(metrics_url, token=token)
            total_counts, snapshot_counts = parse_prometheus_metrics(metrics_text)
        except Exception:
            total_counts, snapshot_counts = {}, {}

        for pipe_id in pipelines:
            ent_url = f"{api_url.rstrip('/')}/pipelines/{pipe_id}/entities"
            try:
                ent_data = json.loads(make_api_request(ent_url, token=token))
            except Exception as e:
                return None, f"Error fetching entities for pipeline {pipe_id}: {str(e)}"
                
            for item in ent_data:
                # Some API schemas nest the entity structure inside an "entity" field
                ent = item.get('entity') if 'entity' in item else item
                entity_id = ent.get('entityId')
                group_id = ent.get('groupId', '_default')
                
                source_table = None
                source_agent = None
                target_table = None
                target_agent = None
                
                agent_entities = ent.get('agentEntities', [])
                for ae in agent_entities:
                    role = ae.get('entityType', {}).get('type')
                    agent_id = ae.get('agentId')
                    table_info = ae.get('table', {})
                    table_name = f"{table_info.get('schema')}.{table_info.get('name')}" if table_info else ae.get('entityName')
                    
                    if role == 'Source':
                        source_table = table_name
                        source_agent = agent_id
                    elif role == 'Target':
                        target_table = table_name
                        target_agent = agent_id
                
                entities_by_id[entity_id] = {
                    'entity_id': entity_id,
                    'group_id': group_id,
                    'source_table': source_table,
                    'source_agent': source_agent,
                    'target_table': target_table,
                    'target_agent': target_agent
                }

                # Extract checkpoint info if available in metrics
                tot = snapshot_counts.get((pipe_id, entity_id))
                mig = total_counts.get((pipe_id, entity_id))
                
                is_mig_active = 0
                is_sync_active = 1 if not pipelines[pipe_id]['isInMaintenanceMode'] else 0
                is_completed = False
                
                if tot is not None and mig is not None:
                    is_completed = (mig >= tot)
                    if mig < tot and mig > 0:
                        is_mig_active = 1
                
                # Construct inferred statuses
                statuses[(pipe_id, entity_id)] = {
                    'pipeline_id': pipe_id,
                    'entity_id': entity_id,
                    'migration_active': is_mig_active, 
                    'sync_active': is_sync_active, 
                    'snapshot_write_method': 'UPSERT' 
                }

                if tot is not None or mig is not None:
                    cp_json = {
                        "type": "LongMigrationCheckpoint",
                        "totalRowCount": tot,
                        "checkpoint": {"rowCount": mig},
                        "isCompleted": is_completed
                    }
                    checkpoints[(pipe_id, entity_id)] = {
                        'pipeline_id': pipe_id,
                        'entity_id': entity_id,
                        'migration_checkpoint': json.dumps(cp_json)
                    }

        return {
            'pipelines': pipelines,
            'statuses': statuses,
            'checkpoints': checkpoints,
            'entities': entities_by_id
        }, None
    except Exception as e:
        return None, f"API query error: {str(e)}"


# =====================================================================
# AGENT MODE IMPLEMENTATION
# =====================================================================

def get_agent_metadata(agent_url):
    """Query the Gluesync Monitor Agent REST API and return metadata
    in the same structure as get_api_metadata / get_db_metadata."""
    try:
        url = agent_url.rstrip('/') + '/status'
        req = Request(url, method='GET')
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urlopen(req, context=ctx, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
    except URLError as e:
        return None, f"Error: Could not connect to Monitor Agent at '{agent_url}': {e.reason}"
    except Exception as e:
        return None, f"Error querying Monitor Agent: {str(e)}"

    # Convert agent format -> internal metadata format
    pipelines   = {}
    statuses    = {}
    checkpoints = {}
    entities_by_id = {}

    for pipe in data.get('pipelines', []):
        pid  = pipe.get('pipeline_id', '')
        pipelines[pid] = {
            'id':                 pid,
            'name':               pipe.get('name', ''),
            'description':        '',
            'isInMaintenanceMode': pipe.get('maintenance_mode', False),
        }

        for ent in pipe.get('entities', []):
            eid = ent.get('entity_id', '')
            if not eid:
                continue

            entities_by_id[eid] = {
                'entity_id':    eid,
                'group_id':     ent.get('group_id', '_default'),
                'source_table': ent.get('source_table'),
                'source_agent': ent.get('source_agent_id'),
                'target_table': ent.get('target_table'),
                'target_agent': ent.get('target_agent_id'),
            }

            st = ent.get('status', {})
            migrated_rows = st.get('migrated_rows')
            total_rows    = st.get('total_rows')
            is_completed  = bool(st.get('is_completed', False))
            migration_active = bool(st.get('migration_active', False))
            sync_active      = bool(st.get('sync_active', False))

            statuses[(pid, eid)] = {
                'pipeline_id':           pid,
                'entity_id':             eid,
                'migration_active':      int(migration_active),
                'sync_active':           int(sync_active),
                'snapshot_write_method': 'UPSERT',
            }

            if migrated_rows is not None or total_rows is not None:
                cp_json = {
                    'type':          'LongMigrationCheckpoint',
                    'totalRowCount': total_rows,
                    'checkpoint':    {'rowCount': migrated_rows},
                    'isCompleted':   is_completed,
                }
                checkpoints[(pid, eid)] = {
                    'pipeline_id':        pid,
                    'entity_id':          eid,
                    'migration_checkpoint': json.dumps(cp_json),
                }

    agent_meta = data.get('agent', {})
    return {
        'pipelines':   pipelines,
        'statuses':    statuses,
        'checkpoints': checkpoints,
        'entities':    entities_by_id,
        '_agent_meta': agent_meta,
    }, None

# =====================================================================
# DATABASE CLIENT IMPLEMENTATION
# =====================================================================

def get_db_metadata(db_path):
    if not db_path or not os.path.exists(db_path):
        return None, f"Error: Gluesync database not found at '{db_path}'"
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. Fetch Pipelines
        cursor.execute("SELECT id, name, description, isInMaintenanceMode FROM PIPELINES")
        pipelines = {row['id']: dict(row) for row in cursor.fetchall()}

        # 2. Fetch Entity Status
        cursor.execute("SELECT pipeline_id, entity_id, migration_active, sync_active, snapshot_write_method FROM ENTITIES_STATUS")
        statuses = {}
        for row in cursor.fetchall():
            statuses[(row['pipeline_id'], row['entity_id'])] = dict(row)

        # 3. Fetch Migration Checkpoints
        cursor.execute("SELECT pipeline_id, entity_id, migration_checkpoint FROM GLUESYNC_MIGRATION_CHECKPOINT")
        checkpoints = {}
        for row in cursor.fetchall():
            checkpoints[(row['pipeline_id'], row['entity_id'])] = dict(row)

        # 4. Fetch Entities
        cursor.execute("SELECT entity_id, agent_id, entity, group_id FROM ENTITIES")
        entities_raw = cursor.fetchall()
        
        entities_by_id = {}
        for r in entities_raw:
            entity_id = r['entity_id']
            agent_id = r['agent_id']
            group_id = r['group_id']
            try:
                data = json.loads(r['entity'])
                ae = data.get("agentEntity", {})
                role = ae.get("entityType", {}).get("type") 
                table_info = ae.get("table", {})
                table_name = f"{table_info.get('schema')}.{table_info.get('name')}" if table_info else ae.get("entityName")
            except Exception:
                role = "Unknown"
                table_name = "Unknown"

            if entity_id not in entities_by_id:
                entities_by_id[entity_id] = {
                    'entity_id': entity_id,
                    'group_id': group_id,
                    'source_table': None,
                    'source_agent': None,
                    'target_table': None,
                    'target_agent': None,
                }
            
            if role == 'Source':
                entities_by_id[entity_id]['source_table'] = table_name
                entities_by_id[entity_id]['source_agent'] = agent_id
            elif role == 'Target':
                entities_by_id[entity_id]['target_table'] = table_name
                entities_by_id[entity_id]['target_agent'] = agent_id

        conn.close()

        return {
            'pipelines': pipelines,
            'statuses': statuses,
            'checkpoints': checkpoints,
            'entities': entities_by_id
        }, None
    except Exception as e:
        return None, f"SQLite query error: {str(e)}"

# =====================================================================
# LIVE TARGET MSSQL COUNTS
# =====================================================================

def get_live_target_counts(entities, env_path):
    """Connect to SQL Server and get live count of rows for each target table."""
    counts = {}
    try:
        import pymssql
    except ImportError:
        return {}, "Warning: 'pymssql' package is not installed. Run 'python3 -m pip install --user pymssql' to enable live counts."

    # Read credentials
    load_env(env_path)
    host = os.environ.get("MSSQL_HOST", "127.0.0.1")
    user = os.environ.get("MSSQL_USER", "sa")
    password = os.environ.get("MSSQL_PASSWORD")
    database = "gstrgtdb" 

    if not password:
        return {}, "Warning: MSSQL_PASSWORD not found in environment or .env file."

    conn = None
    try:
        conn = pymssql.connect(
            server=host,
            user=user,
            password=password,
            database=database,
            timeout=5
        )
        cursor = conn.cursor()
        for entity_id, info in entities.items():
            tbl = info['target_table']
            if tbl:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
                    count = cursor.fetchone()[0]
                    counts[entity_id] = count
                except Exception as tbl_err:
                    counts[entity_id] = f"Error ({tbl_err.__class__.__name__})"
        conn.close()
        return counts, None
    except Exception as conn_err:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return {}, f"Warning: Failed to connect to SQL Server database at {host}: {str(conn_err)}"

def format_percentage(current, total):
    if not total:
        return "0.0%"
    pct = (current / total) * 100.0
    return f"{pct:.1f}%"

def main():
    args = parse_args()
    use_color = supports_color() and not args.no_color
    env_path = find_env_path(args.env)
    
    # Load .env variables first to see if credentials are defined
    load_env(env_path)

    metadata = None
    err = None
    is_agent_mode = args.agent is not None
    is_db_mode    = args.db is not None
    is_api_mode   = not is_agent_mode and not is_db_mode

    if is_agent_mode:
        # Agent Mode: query the Gluesync Monitor Agent REST API
        agent_url = args.agent
        if not args.json:
            print(f"Querying Monitor Agent at {agent_url} ...")
        metadata, err = get_agent_metadata(agent_url)
    elif is_api_mode:
        # API Mode
        api_url = args.api_url
        username = args.user or os.environ.get("GLUESYNC_USER", "admin")
        password = args.password or os.environ.get("GLUESYNC_PASSWORD")
        
        if not password and not args.json:
            print(f"Connecting to Gluesync Core Hub API at {api_url}...")
            print(f"Logging in as: '{username}'")
            password = getpass.getpass("Enter Gluesync password: ")
        elif not password:
            print(f"Error: GLUESYNC_PASSWORD must be provided in env or .env file when using --json option.", file=sys.stderr)
            sys.exit(1)
            
        token, login_err = login_to_api(api_url, username, password)
        if login_err:
            print(c_str(login_err, COLOR_RED, use_color), file=sys.stderr)
            sys.exit(1)
            
        metadata, err = get_api_metadata(api_url, token)
    else:
        # Database Mode
        db_path = find_db_path(args.db)
        if not db_path:
            print(c_str("Error: Could not locate gluesync.db. Please supply it via --db <path>", COLOR_RED, use_color), file=sys.stderr)
            sys.exit(1)
        metadata, err = get_db_metadata(db_path)

    if err:
        print(c_str(err, COLOR_RED, use_color), file=sys.stderr)
        sys.exit(1)

    entities = metadata['entities']
    
    # Query live SQL Server counts if requested
    live_counts = {}
    live_warn = None
    if not args.no_live:
        live_counts, live_warn = get_live_target_counts(entities, env_path)

    # Format output
    if args.json:
        # Construct JSON output structure
        json_output = {
            "metadata": {
                "mode": "agent" if is_agent_mode else ("api" if is_api_mode else "database"),
                "env_path": env_path,
                "live_target_counts_retrieved": not args.no_live and not live_warn
            },
            "pipelines": []
        }
        if is_agent_mode:
            json_output["metadata"]["agent_url"] = args.agent
        elif is_db_mode:
            json_output["metadata"]["database_path"] = find_db_path(args.db)
        else:
            json_output["metadata"]["api_url"] = args.api_url
            
        for pipe_id, pipe in metadata['pipelines'].items():
            pipe_data = {
                "id": pipe_id,
                "name": pipe['name'],
                "description": pipe['description'],
                "maintenance_mode": bool(pipe['isInMaintenanceMode']),
                "entities": []
            }
            
            for entity_id, info in entities.items():
                status_key = (pipe_id, entity_id)
                status = metadata['statuses'].get(status_key, {
                    'pipeline_id': pipe_id,
                    'entity_id': entity_id,
                    'migration_active': 0,
                    'sync_active': 0,
                    'snapshot_write_method': 'UPSERT'
                })
                cp = metadata['checkpoints'].get(status_key)
                cp_data = json.loads(cp['migration_checkpoint']) if cp else None
                
                total_rows = cp_data.get("totalRowCount") if cp_data else None
                migrated_rows = cp_data.get("checkpoint", {}).get("rowCount") if cp_data else None
                is_completed = cp_data.get("isCompleted") if cp_data else False
                
                live_count = live_counts.get(entity_id)
                
                ent_data = {
                    "entity_id": entity_id,
                    "group_id": info['group_id'],
                    "source": {
                        "table": info['source_table'],
                        "agent_id": info['source_agent']
                    },
                    "target": {
                        "table": info['target_table'],
                        "agent_id": info['target_agent'],
                        "live_row_count": live_count
                    },
                    "replication": {
                        "migration_active": bool(status['migration_active']) if not is_api_mode else None,
                        "sync_active": bool(status['sync_active']) if not is_api_mode else None,
                        "write_method": status['snapshot_write_method']
                    },
                    "checkpoint": {
                        "total_rows": total_rows,
                        "migrated_rows": migrated_rows,
                        "is_completed": is_completed
                    }
                }
                pipe_data["entities"].append(ent_data)
            json_output["pipelines"].append(pipe_data)
        
        print(json.dumps(json_output, indent=2))
        if live_warn and not args.no_live:
            print(f"/* {live_warn} */", file=sys.stderr)
        return

    # CLI Output Style
    print("=" * 110)
    print(c_str(f" {COLOR_BOLD}GLUESYNC VERIFICATION & MONITORING DASHBOARD{COLOR_RESET}", COLOR_CYAN, use_color))
    if is_agent_mode:
        print(f" Mode: Monitor Agent ({args.agent})")
    elif is_api_mode:
        print(f" Mode: API Client ({args.api_url})")
    else:
        print(f" Mode: Direct Database ({find_db_path(args.db)})")
    if env_path:
        print(f" Env file: {env_path}")
    print("=" * 110)

    for pipe_id, pipe in metadata['pipelines'].items():
        maint_status = c_str("ON", COLOR_RED, use_color) if pipe['isInMaintenanceMode'] else c_str("OFF", COLOR_GREEN, use_color)
        print(f"\n{c_str('Pipeline:', COLOR_BOLD, use_color)} {pipe['name']} ({c_str(pipe_id, COLOR_BLUE, use_color)})")
        print(f"Maintenance Mode: {maint_status}")
        print("-" * 110)
        
        # Table Headers
        header_format = "{:<10} | {:<20} | {:<16} | {:<8} | {:<12} | {:<10} | {:<15} | {:<11}"
        row_format = "{:<10} | {:<20} | {:<16} | {:<8} | {} | {} | {:<15} | {}"
        
        print(c_str(header_format.format(
            "Entity ID", "Source Table", "Target Table", "Method", "Migration", "Sync (CDC)", "Checkpoint Rows", "Live Target"
        ), COLOR_BOLD, use_color))
        print("-" * 110)
        
        has_entities = False
        for entity_id, info in entities.items():
            status_key = (pipe_id, entity_id)
            has_entities = True
            status = metadata['statuses'].get(status_key, {
                'pipeline_id': pipe_id,
                'entity_id': entity_id,
                'migration_active': 0,
                'sync_active': 0,
                'snapshot_write_method': 'UPSERT'
            })
            cp = metadata['checkpoints'].get(status_key)
            cp_data = json.loads(cp['migration_checkpoint']) if cp else None
            
            # Migration status strings
            mig_val = "ACTIVE" if status['migration_active'] else ("COMPLETED" if (cp_data and cp_data.get("isCompleted")) else "INACTIVE")
            mig_padded = mig_val.ljust(12)
            if mig_val == "ACTIVE":
                mig_str = c_str(mig_padded, COLOR_YELLOW, use_color)
            elif mig_val == "COMPLETED":
                mig_str = c_str(mig_padded, COLOR_GREEN, use_color)
            else:
                mig_str = mig_padded
            
            # Sync status strings
            sync_val = "ACTIVE" if status['sync_active'] else "INACTIVE"
            sync_padded = sync_val.ljust(10)
            if sync_val == "ACTIVE":
                sync_str = c_str(sync_padded, COLOR_GREEN, use_color)
            else:
                sync_str = c_str(sync_padded, COLOR_RED, use_color)
            
            # Row counts
            total_rows = cp_data.get("totalRowCount") if cp_data else None
            migrated_rows = cp_data.get("checkpoint", {}).get("rowCount") if cp_data else None
            
            if total_rows is not None and migrated_rows is not None:
                pct = format_percentage(migrated_rows, total_rows)
                cp_rows_str = f"{migrated_rows:,}/{total_rows:,} ({pct})"
            else:
                cp_rows_str = "N/A"
            
            live_count = live_counts.get(entity_id)
            if live_count is not None:
                if isinstance(live_count, int):
                    base_str = f"{live_count:,}"
                    # Check match
                    if total_rows is not None and live_count == total_rows:
                        base_str += " ✓"
                        live_count_str = c_str(base_str.ljust(11), COLOR_GREEN, use_color)
                    elif total_rows is not None:
                        base_str += " ✗"
                        live_count_str = c_str(base_str.ljust(11), COLOR_RED, use_color)
                    else:
                        live_count_str = base_str.ljust(11)
                else:
                    live_count_str = c_str("Error".ljust(11), COLOR_RED, use_color)
            else:
                live_count_str = "N/A".ljust(11)
            
            # Print Row
            print(row_format.format(
                entity_id,
                info['source_table'] or "N/A",
                info['target_table'] or "N/A",
                status['snapshot_write_method'],
                mig_str,
                sync_str,
                cp_rows_str,
                live_count_str
            ))
        if not has_entities:
            print(" No entities mapped to this pipeline.")
        print("-" * 110)

    if is_api_mode:
        print(c_str("Note: Detailed internal database state flags (Migration, Sync) are inferred in API mode based on metrics.\n      For raw internal DB flags, run: `./list_entities.py --db`", COLOR_BLUE, use_color))
    if live_warn:
        print(f"\n{c_str('[WARNING]', COLOR_YELLOW, use_color)} {live_warn}")
    print()

if __name__ == "__main__":
    main()
