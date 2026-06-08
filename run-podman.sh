#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure Podman is available
if ! command -v podman >/dev/null 2>&1; then
  echo "Error: podman is not installed or not in PATH." >&2
  exit 1
fi

# Ensure Podman machine is running (specifically for macOS)
if podman machine list 2>/dev/null | grep -q "podman-machine-default"; then
  if ! podman system info >/dev/null 2>&1; then
    echo "Podman machine is not running. Starting it..."
    podman machine start
  fi
fi

# Determine compose command: prefer 'podman-compose' over 'podman compose' to prevent label splits
COMPOSE_CMD=""
if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD="podman-compose"
elif podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD="podman compose"
else
  echo "Error: Neither 'podman-compose' nor 'podman compose' is available. Please install podman-compose." >&2
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

# Validate .env values
if [[ -f "$script_dir/.env" ]]; then
  # Sourced in a subshell to check variables without leaking them unnecessarily
  if ! grep -q "^MSSQL_PASSWORD=[^[:space:]]" "$script_dir/.env"; then
    echo "Error: MSSQL_PASSWORD is not set or is empty in .env. Please check the file." >&2
    exit 1
  fi
else
  echo "Error: .env file not found in $script_dir." >&2
  exit 1
fi

echo "Running: $COMPOSE_CMD -f $script_dir/docker-compose.yml up -d --remove-orphans"
$COMPOSE_CMD -f "$script_dir/docker-compose.yml" up -d --remove-orphans

echo "MSSQL container started (detached)."
