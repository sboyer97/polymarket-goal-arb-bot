#!/bin/bash
# Run live_system on server (use python3; venv optional)
cd "$(dirname "$0")"
if [ -d "venv" ] && [ -x "venv/bin/python3" ]; then
  source venv/bin/activate
fi
exec python3 live_system.py "$@"
