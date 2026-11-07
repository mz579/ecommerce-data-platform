#!/usr/bin/env bash
# Download the fixed-version Flink user jars into docker/flink/lib/ (git-ignored).
#
# The Flink image only ships a JRE, so the job jar is built on the host; these two
# connector jars are copied into /opt/flink/lib by docker/flink/Dockerfile.
#
# Versions are pinned and verified by SHA-256 on first download:
#   - flink-sql-connector-kafka 3.3.0-1.20 (the suggested 3.2.0-1.20 does not exist;
#     3.2.0 stops at -1.19, 3.3.0 is the first 1.20 build)
#   - mysql-connector-j 8.4.0
#
# Usage:
#   bash scripts/download_flink_jars.sh [--force]
#   MAVEN_BASE_URL=https://maven.aliyun.com/repository/central bash scripts/download_flink_jars.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB_DIR="$ROOT/docker/flink/lib"
MAVEN_BASE_URL="${MAVEN_BASE_URL:-https://repo1.maven.org/maven2}"
FORCE=0
[[ "${1:-}" == "--force" ]] && FORCE=1

KAFKA_VERSION="3.3.0-1.20"
KAFKA_JAR="flink-sql-connector-kafka-${KAFKA_VERSION}.jar"
KAFKA_SHA256="1086f3eee73d727e234860fcd03adafc0d76f2fc70a25d39c36427693fff749d"
KAFKA_URL="${MAVEN_BASE_URL}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VERSION}/${KAFKA_JAR}"
MYSQL_VERSION="8.4.0"
MYSQL_JAR="mysql-connector-j-${MYSQL_VERSION}.jar"
MYSQL_SHA256="d77962877d010777cff997015da90ee689f0f4bb76848340e1488f2b83332af5"
MYSQL_URL="${MAVEN_BASE_URL}/com/mysql/mysql-connector-j/${MYSQL_VERSION}/${MYSQL_JAR}"

mkdir -p "$LIB_DIR"

verify() {
    local file="$1" expected="$2" actual
    actual="$(sha256sum "$file" | awk '{print $1}')"
    if [[ "$actual" != "$expected" ]]; then
        echo "[error] sha256 mismatch for $file" >&2
        echo "        expected $expected" >&2
        echo "        actual   $actual" >&2
        rm -f "$file"
        exit 3
    fi
    echo "[sha256] OK $actual"
}

download() {
    local url="$1" dest="$2" expected="$3"
    if [[ -f "$dest" && "$FORCE" -eq 0 ]]; then
        echo "[skip] $dest already exists ($(stat -c %s "$dest") bytes)"
        verify "$dest" "$expected"
        return
    fi
    echo "[download] $url"
    curl -fL --retry 3 --connect-timeout 20 --max-time 300 -o "$dest.tmp" "$url"
    verify "$dest.tmp" "$expected"
    mv "$dest.tmp" "$dest"
    echo "[ok] $dest ($(stat -c %s "$dest") bytes)"
}

download "$KAFKA_URL" "$LIB_DIR/$KAFKA_JAR" "$KAFKA_SHA256"
download "$MYSQL_URL" "$LIB_DIR/$MYSQL_JAR" "$MYSQL_SHA256"

echo
echo "FLINK_JARS_OK"
ls -l "$LIB_DIR"
