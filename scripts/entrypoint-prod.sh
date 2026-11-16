#!/bin/bash
# Production entrypoint: the webserver serves the UI, and the daemon runs the
# sensor and the run queue. Without the daemon, no new data is ever picked up.
# If either process exits, the container exits and Docker restarts it.
set -euo pipefail

cp /app/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

dagster-daemon run -m comext_pipeline &
dagster-webserver -m comext_pipeline --host 0.0.0.0 --port 3000 &

wait -n
exit $?
