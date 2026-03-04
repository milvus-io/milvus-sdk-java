#!/usr/bin/env bash
#
# Run all Milvus Java SDK benchmarks for multiple SDK versions.
#
# Usage:
#   ./run_benchmarks.sh [options]
#
# Options:
#   -v, --versions  Comma-separated SDK versions (default: 2.6.1,2.6.14)
#   -u, --uri       Milvus server URI (default: http://localhost:19530)
#   -t, --token     Authentication token (default: root:Milvus)
#   -h, --help      Show this help message
#
# Examples:
#   ./run_benchmarks.sh
#   ./run_benchmarks.sh -v 2.6.14
#   ./run_benchmarks.sh -v 2.6.1,2.6.13,2.6.14
#   ./run_benchmarks.sh -v 2.6.1,2.6.14 -u http://myhost:19530
#   ./run_benchmarks.sh -v 2.6.14 -u http://myhost:19530 -t root:Milvus

set -euo pipefail

# --- Defaults ---
VERSIONS="2.6.1,2.6.14"
URI=""
TOKEN=""

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--versions)
            VERSIONS="$2"
            shift 2
            ;;
        -u|--uri)
            URI="$2"
            shift 2
            ;;
        -t|--token)
            TOKEN="$2"
            shift 2
            ;;
        -h|--help)
            head -20 "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run with -h for usage."
            exit 1
            ;;
    esac
done

# Convert comma-separated versions to space-separated
SDK_VERSIONS="${VERSIONS//,/ }"

BENCHMARKS=(
    "io.milvus.benchmark.PoolBenchmark"
    "io.milvus.benchmark.SearchBenchmark"
)

# --- Check prerequisites ---
if ! command -v mvn &>/dev/null; then
    echo "Error: 'mvn' command not found. Please install Maven 3.x or add it to your PATH."
    exit 1
fi

echo "Maven found: $(mvn --version | head -1)"
echo "SDK versions: $SDK_VERSIONS"
echo ""

# Build exec.args if URI/TOKEN provided
EXEC_ARGS=""
if [ -n "$URI" ]; then
    EXEC_ARGS="$URI"
    if [ -n "$TOKEN" ]; then
        EXEC_ARGS="$URI $TOKEN"
    fi
fi

# --- Run benchmarks ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

for version in $SDK_VERSIONS; do
    echo "============================================================"
    echo " SDK Version: $version"
    echo "============================================================"
    echo ""

    for benchmark in "${BENCHMARKS[@]}"; do
        class_name="${benchmark##*.}"
        echo "------------------------------------------------------------"
        echo " Running: $class_name (SDK $version)"
        echo "------------------------------------------------------------"

        MVN_ARGS=(clean compile exec:java
            "-Drevision=$version"
            "-Dexec.mainClass=$benchmark"
        )
        if [ -n "$EXEC_ARGS" ]; then
            MVN_ARGS+=("-Dexec.args=$EXEC_ARGS")
        fi

        if ! mvn "${MVN_ARGS[@]}"; then
            echo ""
            echo "WARNING: $class_name failed with SDK $version, continuing..."
            echo ""
        fi

        echo ""
    done
done

echo "============================================================"
echo " All benchmarks complete."
echo " Results are in: $SCRIPT_DIR/results/"
echo "============================================================"
ls -lt results/*.md 2>/dev/null | head -20
