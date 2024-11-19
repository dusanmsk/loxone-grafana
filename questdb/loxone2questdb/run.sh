#!/bin/sh

if [ "$LOXONE2QUESTDB_DEBUGGING" = "1" ]; then
    echo "Debugging enabled"
    set -x
    # --wait-for-client
    python3 -m debugpy --listen 0.0.0.0:5678 /app/loxone2questdb.py
else
    python3 /app/loxone2questdb.py
fi
