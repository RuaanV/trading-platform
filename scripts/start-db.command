#!/bin/bash
# Starts the local Postgres container and creates the portfolio tables.
# Double-click in Finder (opens Terminal) or run: ./scripts/start-db.command
export PATH="$HOME/.orbstack/bin:/usr/local/bin:/opt/homebrew/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1
LOG=logs/db-init.log
{
  echo "=== $(date) ==="
  if ! command -v docker >/dev/null; then echo "ERROR: docker CLI not found"; echo "EXIT=127"; exit 127; fi
  if ! docker info >/dev/null 2>&1; then
    echo "Docker engine not running - starting OrbStack..."
    open -a OrbStack 2>/dev/null || open -a Docker
    for i in $(seq 1 90); do docker info >/dev/null 2>&1 && break; sleep 2; done
    docker info >/dev/null 2>&1 || { echo "ERROR: Docker did not start within 3 minutes"; echo "EXIT=1"; exit 1; }
  fi
  existing=$(docker ps -a --filter name=^trading-postgres$ --format '{{.Names}} {{.Status}} {{.Label "com.docker.compose.project"}}')
  echo "Existing container: ${existing:-none}"
  if [ -n "$existing" ] && ! docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' trading-postgres | grep -q .; then
    echo "A non-compose trading-postgres container already exists; starting it instead of replacing it."
    docker start trading-postgres
    until docker exec trading-postgres pg_isready >/dev/null 2>&1; do sleep 1; done
    make init-portfolios; rc=$?
  else
    make db-init; rc=$?
  fi
  docker ps --filter name=trading-postgres --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
  echo "EXIT=$rc"
} 2>&1 | tee "$LOG"
