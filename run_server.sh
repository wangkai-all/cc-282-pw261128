#!/usr/bin/env bash
# 启动质量处置平台（默认 http://0.0.0.0:8000）
set -euo pipefail
cd "$(dirname "$0")"

export QUALITY_DB="${QUALITY_DB:-data/quality.db}"
export PORT="${PORT:-8000}"

mkdir -p "$(dirname "$QUALITY_DB")"
exec python3 -m app.server
