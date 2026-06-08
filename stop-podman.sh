#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure Podman is available
if ! command -v podman >/dev/null 2>&1; then
  echo "Error: podman is not installed or not in PATH." >&2
  exit 1
fi

# Determine compose command: prefer 'podman compose' wrapper, fallback to 'podman-compose'
COMPOSE_CMD=""
if podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD="podman compose"
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD="podman-compose"
else
  echo "Error: Neither 'podman compose' nor 'podman-compose' is available. Please install podman-compose or configure a compose provider." >&2
  exit 1
fi

# Project Environment Adjustments:
# 1. Project Naming: Podman does not allow project names starting with underscores (_).
#    Since the directory starts with "_" (e.g. /Users/pakornnan/_gsvermon), we explicitly force it to 'gsvermon'.
export COMPOSE_PROJECT_NAME=gsvermon

# 2. Socket Connection: Map local Unix socket to DOCKER_HOST if host has it running.
PODMAN_SOCKET=$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)
if [[ -n "$PODMAN_SOCKET" ]]; then
  export DOCKER_HOST="unix://$PODMAN_SOCKET"
fi

echo "Running: $COMPOSE_CMD -f $script_dir/docker-compose.yml down --remove-orphans"
$COMPOSE_CMD -f "$script_dir/docker-compose.yml" down --remove-orphans

echo "MSSQL container stopped."
