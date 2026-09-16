#!/usr/bin/env bash
# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Compile and run the Milvus SDK Java examples (V2) in batch.

set -uo pipefail

usage() {
  cat <<'EOF'
Usage: run_examples.sh [OPTIONS] [FILTER ...]

Compile and run the V2 examples under examples/src/main/java/io/milvus/v2.
By default every example with a main() method is run, except a hard-coded
skip list of examples that need extra services (MinIO, CDC, TLS, external
tables, etc.) or are not meant to be executed standalone.

The example set can be narrowed with FILTER arguments: an example runs only
when its class name contains at least one filter string (case-insensitive),
e.g. "run_examples.sh Vector Search".

OPTIONS:
  -l, --list       List the eligible examples without running them
  -n, --limit N    Run at most the first N eligible examples
  -h, --help       Show this help and exit

The repository root is derived from this script's location and can be
overridden with MILVUS_SDK_JAVA_DIR. A JDK 11+ installation is auto-detected
when the default java is older (the TensorFlow-based examples need Java 11).
EOF
}

list_only=0
limit=0
filters=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -l | --list)
      list_only=1
      shift
      ;;
    -n | --limit)
      if [[ $# -lt 2 ]]; then
        echo "Option $1 requires an argument." >&2
        exit 2
      fi
      limit="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      filters+=("$1")
      shift
      ;;
  esac
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=${MILVUS_SDK_JAVA_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}
EXAMPLES_DIR="$REPO_DIR/examples"
JAVA_SOURCE_ROOT="$EXAMPLES_DIR/src/main/java"
SOURCE_DIR="$JAVA_SOURCE_ROOT/io/milvus/v2"

if [[ ! -f "$EXAMPLES_DIR/pom.xml" ]]; then
    echo "Cannot find the examples Maven project at $EXAMPLES_DIR." >&2
    echo "Set MILVUS_SDK_JAVA_DIR to the milvus-sdk-java repository path." >&2
    exit 1
fi

cd "$EXAMPLES_DIR"

if ! command -v mvn >/dev/null 2>&1; then
    echo "Maven is required to build and run the examples." >&2
    exit 1
fi

java_major_version() {
    local version
    version=$("$1" -version 2>&1 | awk -F '"' '/version/ {print $2; exit}')
    if [[ "$version" == 1.* ]]; then
        version=${version#1.}
    fi
    echo "${version%%.*}"
}

java_bin=$(command -v java)
java_major=$(java_major_version "$java_bin")
if ((java_major < 11)); then
    java_11_home=""
    for candidate in \
        "${JAVA11_HOME:-}" \
        /usr/lib/jvm/java-11-openjdk-amd64 \
        /usr/lib/jvm/java-11-openjdk \
        /usr/lib/jvm/openjdk-11; do
        if [[ -n "$candidate" && -x "$candidate/bin/java" ]]; then
            java_11_home=$candidate
            break
        fi
    done

    if [[ -z "$java_11_home" ]]; then
        echo "JDK 11 or newer is required because the examples depend on TensorFlow classes built for Java 11." >&2
        echo "Set JAVA_HOME or JAVA11_HOME to a JDK 11+ installation and try again." >&2
        exit 1
    fi

    export JAVA_HOME=$java_11_home
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "Using JDK at $JAVA_HOME"
fi

echo "Compiling examples..."
if ! mvn compile; then
    echo "Failed to compile examples." >&2
    exit 1
fi

files=()
while IFS= read -r file; do
    if ! grep -Eq 'public[[:space:]]+static[[:space:]]+void[[:space:]]+main[[:space:]]*\(' "$file"; then
        continue
    fi

    filename=$(basename "$file")
    case "$filename" in
        CDCExample.java|TLSExample.java|VolumeFileManagerExample.java|VolumeManagerExample.java|\
        ClientPoolDemo.java|ClientPoolExample.java|ExternalTableExample.java|OptimizeExample.java|\
        AddFieldExample.java)
            continue
            ;;
    esac

    class_name=${file#"$JAVA_SOURCE_ROOT/"}
    class_name=${class_name%.java}
    class_name=${class_name//\//.}

    if ((${#filters[@]} > 0)); then
        match=0
        for f in "${filters[@]}"; do
            if printf '%s\n' "$class_name" | grep -qiF "$f"; then
                match=1
                break
            fi
        done
        ((match == 1)) || continue
    fi

    files+=("$class_name")
done < <(find "$SOURCE_DIR" -path "$SOURCE_DIR/bulkwriter" -prune \
    -o -type f -name '*.java' -print | sort)

if ((limit > 0 && ${#files[@]} > limit)); then
    files=("${files[@]:0:$limit}")
fi

if ((list_only)); then
    printf 'Eligible examples (%d):\n' "${#files[@]}"
    printf '  %s\n' "${files[@]}"
    exit 0
fi

failed=()
run_count=0

for class_name in "${files[@]}"; do
    run_count=$((run_count + 1))
    printf '\nRunning %s\n' "$class_name"
    if ! mvn exec:java -Dexec.mainClass="$class_name"; then
        failed+=("$class_name")
    fi
done

printf '\nRan %d examples.\n' "$run_count"
if ((${#failed[@]} > 0)); then
    echo "Failed examples:" >&2
    printf '  %s\n' "${failed[@]}" >&2
    exit 1
fi

echo "All examples completed successfully."
