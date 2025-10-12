#!/bin/bash
# Keep container running so FastAPI can execute iquery commands
# SciDB server can run in background; use iquery for commands

echo "SciDB container started. You can now run iquery commands."
tail -f /dev/null
