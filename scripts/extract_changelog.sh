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

# Extract the changelog section for a given version from a Markdown changelog.
# Works with common changelog heading styles, e.g. `## X.Y.Z (date)`,
# `## [X.Y.Z] - date`, `# vX.Y.Z (date)`, or `## <project> X.Y.Z (date)`.
# A version token is matched with numeric boundaries, so `3.0.1` never matches
# inside `3.0.10`, and pre-release suffixes such as `3.0.3-rc1` never match
# when looking for `3.0.3`.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: extract_changelog.sh [OPTIONS] VERSION

Print the changelog section for VERSION (e.g. 3.0.1) from a Markdown
changelog. The section starts at the matching version heading and ends at the
next heading of the same or a shallower level; the heading line itself is
omitted unless --include-heading is given. Leading and trailing blank lines
are trimmed. Exit status is 0 when the version is found, 1 when it is not.

OPTIONS:
  -f, --file FILE        Changelog file (default: CHANGELOG.md)
  -i, --include-heading  Include the matching version heading in the output
  -h, --help             Show this help and exit
EOF
}

file="CHANGELOG.md"
include_heading=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f | --file)
      if [[ $# -lt 2 ]]; then
        echo "Option $1 requires an argument." >&2
        exit 2
      fi
      file="$2"
      shift 2
      ;;
    -i | --include-heading)
      include_heading=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi
version="$1"

if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Invalid version: ${version}. Expected X.Y.Z." >&2
  exit 2
fi

if [[ ! -f "${file}" ]]; then
  echo "Changelog file not found: ${file}" >&2
  exit 2
fi

output="$(
  awk -v version="${version}" -v include_heading="${include_heading}" '
    function heading_level(line,    lvl) {
      lvl = 0
      while (substr(line, lvl + 1, 1) == "#") lvl++
      return lvl
    }
    BEGIN {
      ver = version
      gsub(/\./, "\\.", ver)
      pattern = "(^|[^0-9A-Za-z._-])" ver "([^0-9A-Za-z._-]|$)"
      found = 0
    }
    {
      if (substr($0, 1, 1) == "#") {
        lvl = heading_level($0)
        text = $0
        sub(/^#+[[:space:]]*/, "", text)
        sub(/^v/, "", text)
        if (!found) {
          if (text ~ pattern) {
            found = 1
            found_level = lvl
            if (include_heading == "1") print
          }
        } else if (lvl <= found_level) {
          exit
        } else {
          print
        }
        next
      }
      if (found) print
    }
    END { if (!found) exit 1 }
  ' "${file}"
)" || {
  echo "No changelog entry found for version ${version} in ${file}." >&2
  exit 1
}

printf '%s\n' "${output}" \
  | sed -e '/./,$!d' \
  | sed -e :a -e '/^[[:space:]]*$/{$d;N;ba}'
