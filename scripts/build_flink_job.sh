#!/usr/bin/env bash
# Build the Phase 9.3 Flink DataStream job with javac --release 17 and run the Java
# self-test before packaging.
#
# The Flink containers only have a JRE, so the jar MUST be produced here (host JDK)
# before it appears under /opt/flink/jobs.
#
# Environment overrides:
#   JAVAC           javac executable (default: javac)
#   JAVA            java executable used for the self-test (default: java)
#   FLINK_DIST_JAR  path to flink-dist-1.20.1.jar (default: existing 9.1 copy or lib/)
#
# Outputs (all git-ignored under build/):
#   build/flink-job-classes/           compiled main classes
#   build/flink-job-test-classes/      compiled self-test
#   build/flink-jobs/ecommerce-rt-job.jar
#   build/flink-job-selftest.log
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_MAIN="$ROOT/streaming/flink-job/src/main/java"
SRC_TEST="$ROOT/streaming/flink-job/src/test/java"
LIB_DIR="$ROOT/docker/flink/lib"
CLASSES="$ROOT/build/flink-job-classes"
TEST_CLASSES="$ROOT/build/flink-job-test-classes"
JAR_OUT="$ROOT/build/flink-jobs/ecommerce-rt-job.jar"
SELFTEST_LOG="$ROOT/build/flink-job-selftest.log"

KAFKA_JAR="$LIB_DIR/flink-sql-connector-kafka-3.3.0-1.20.jar"
MYSQL_JAR="$LIB_DIR/mysql-connector-j-8.4.0.jar"

JAVAC="${JAVAC:-javac}"
JAVA="${JAVA:-java}"
JAR="${JAR:-jar}"

FLINK_DIST_JAR="${FLINK_DIST_JAR:-}"
if [[ -z "$FLINK_DIST_JAR" ]]; then
    for candidate in \
        "$LIB_DIR/flink-dist-1.20.1.jar" \
        "/c/Zproject/ZClaude/streaming-9.1/flink-dist-1.20.1.jar" \
        "$HOME/Zproject/ZClaude/streaming-9.1/flink-dist-1.20.1.jar"; do
        if [[ -f "$candidate" ]]; then
            FLINK_DIST_JAR="$candidate"
            break
        fi
    done
fi

for required in "$FLINK_DIST_JAR" "$KAFKA_JAR" "$MYSQL_JAR"; do
    if [[ ! -f "$required" ]]; then
        echo "[error] missing build dependency: $required" >&2
        echo "        run: bash scripts/download_flink_jars.sh" >&2
        exit 2
    fi
done

CP="$FLINK_DIST_JAR:$KAFKA_JAR:$MYSQL_JAR"

echo "== javac =="
"$JAVAC" -version
echo "== classpath =="
echo "  $FLINK_DIST_JAR"
echo "  $KAFKA_JAR"
echo "  $MYSQL_JAR"

rm -rf "$CLASSES" "$TEST_CLASSES"
mkdir -p "$CLASSES" "$TEST_CLASSES" "$(dirname "$JAR_OUT")"

echo
echo "== compiling main sources (--release 17) =="
# shellcheck disable=SC2046
"$JAVAC" --release 17 -encoding UTF-8 -Xlint:all,-deprecation,-serial -d "$CLASSES" -cp "$CP" \
    $(find "$SRC_MAIN" -name '*.java' | sort)

echo
echo "== compiling self-test =="
# shellcheck disable=SC2046
"$JAVAC" --release 17 -encoding UTF-8 -Xlint:all,-deprecation,-serial -d "$TEST_CLASSES" -cp "$CP:$CLASSES" \
    $(find "$SRC_TEST" -name '*.java' | sort)

echo
echo "== running self-test =="
set +e
"$JAVA" -cp "$CP:$CLASSES:$TEST_CLASSES" com.ecommerce.rt.SelfTest 2>&1 | tee "$SELFTEST_LOG"
SELFTEST_RC=${PIPESTATUS[0]}
set -e
if [[ "$SELFTEST_RC" -ne 0 ]]; then
    echo "[error] self-test failed (rc=$SELFTEST_RC), see $SELFTEST_LOG" >&2
    exit "$SELFTEST_RC"
fi

echo
echo "== packaging job jar =="
"$JAR" --create --file "$JAR_OUT" --main-class com.ecommerce.rt.RtJob -C "$CLASSES" .
"$JAR" --list --file "$JAR_OUT" | grep -q 'com/ecommerce/rt/RtJob.class'
echo "[ok] $JAR_OUT ($(stat -c %s "$JAR_OUT") bytes)"
echo
echo "BUILD_FLINK_JOB_OK"
