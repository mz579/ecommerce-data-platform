#!/usr/bin/env bash
# Submit (or restore) the Phase 9.3 job on the standalone Flink session cluster.
#
# Run inside WSL (the Docker engine lives there), from the repo root or anywhere:
#   wsl -d Ubuntu24.04 -- bash -lc \
#     'cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && bash scripts/deploy_flink_job.sh'
#
# The jar is mounted read-only at /opt/flink/jobs by docker-compose.yml. Build it on
# the host first with scripts/build_flink_job.sh; the container has no javac.
#
# Usage:
#   bash scripts/deploy_flink_job.sh [--restore-from <checkpoint-path>] [job args...]
#
# Examples:
#   bash scripts/deploy_flink_job.sh
#   bash scripts/deploy_flink_job.sh --group-id ecommerce-rt-flink-v1-drill
#   bash scripts/deploy_flink_job.sh --restore-from file:/opt/flink/checkpoints/<id>/chk-12
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JM="${FLINK_JM_CONTAINER:-ecommerce-flink-jobmanager}"
JAR_CONTAINER="/opt/flink/jobs/ecommerce-rt-job.jar"
JAR_HOST="$ROOT/build/flink-jobs/ecommerce-rt-job.jar"

RESTORE_FROM=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --restore-from)
            RESTORE_FROM="${2:?--restore-from requires a checkpoint path}"
            shift 2
            ;;
        --help|-h)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            break
            ;;
    esac
done

if [[ ! -f "$JAR_HOST" ]]; then
    echo "[error] job jar not found: $JAR_HOST" >&2
    echo "        run: bash scripts/build_flink_job.sh" >&2
    exit 2
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$JM"; then
    echo "[error] Flink JobManager container $JM is not running" >&2
    exit 2
fi

CMD=(flink run -d)
if [[ -n "$RESTORE_FROM" ]]; then
    CMD+=(-s "$RESTORE_FROM")
fi
CMD+=("$JAR_CONTAINER" "$@")

echo "== submitting =="
echo "docker exec $JM ${CMD[*]}"
set +e
OUTPUT=$(docker exec "$JM" "${CMD[@]}" 2>&1)
RC=$?
set -e
echo "$OUTPUT" | grep -v '^WARNING' || true
if [[ $RC -ne 0 ]]; then
    echo "[error] flink run failed (rc=$RC)" >&2
    exit $RC
fi

JOB_ID=$(echo "$OUTPUT" | grep -oE 'JobID [0-9a-f]{32}' | awk '{print $2}' | head -1)
if [[ -z "$JOB_ID" ]]; then
    echo "[error] could not parse JobID from flink run output" >&2
    exit 1
fi
echo
echo "JOB_ID=$JOB_ID"
if [[ -n "$RESTORE_FROM" ]]; then
    echo "RESTORE_FROM=$RESTORE_FROM"
fi
