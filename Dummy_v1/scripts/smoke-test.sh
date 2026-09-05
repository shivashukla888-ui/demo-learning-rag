#!/usr/bin/env sh
set -eu

API_KEY="${AEGIS_API_KEY:-hackathon-local-change-me}"
curl -fsS http://localhost:8000/health >/dev/null
curl -fsS http://localhost:8080/actuator/health >/dev/null
curl -fsS -H "X-Aegis-Key: ${API_KEY}" http://localhost:8080/v1/management >/dev/null
curl -fsS http://localhost:3000/ >/dev/null
printf '%s\n' "Trade Surveillance Navigator stack is healthy."
