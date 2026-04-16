#!/usr/bin/env bash

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

set -e

DIR="$1"
CONFIG_IN="cmd/$DIR/builder-config.yaml"
CONFIG_OUT="cmd/$DIR/builder-config-replaced.yaml"

cp "$CONFIG_IN" "$CONFIG_OUT"

tmp_replaces=$(mktemp)
trap 'rm -f "$tmp_replaces"' EXIT

relpath_from_builder() {
    local mod_path="$1"
    local rhs="$2"

    local abs_rhs
    abs_rhs=$(python3 - "$mod_path" "$rhs" <<'PY'
import os
import sys

mod_path, rhs = sys.argv[1], sys.argv[2]
print(os.path.realpath(os.path.join(mod_path, rhs)))
PY
)

    python3 - "cmd/$DIR" "$abs_rhs" <<'PY'
import os
import sys

base, target = sys.argv[1], sys.argv[2]
print(os.path.relpath(target, start=base))
PY
}

local_mods=$(find . -type f -name "go.mod" -exec dirname {} \; | sort)
for mod_path in $local_mods; do
    mod=${mod_path#"."} # remove initial dot
    echo "github.com/open-telemetry/opentelemetry-collector-contrib$mod => ../..$mod" >> "$tmp_replaces"

    awk '
        /^replace[[:space:]]*\(/ { in_replace = 1; next }
        in_replace && /^\)/ { in_replace = 0; next }
        /^replace[[:space:]]+/ && !/\(/ {
            line = $0
            sub(/^replace[[:space:]]+/, "", line)
            print line
            next
        }
        in_replace { print }
    ' "$mod_path/go.mod" |
        sed -E 's/[[:space:]]*\/\/.*$//' |
        while IFS= read -r replace_line; do
            replace_line=$(echo "$replace_line" | xargs)
            [[ -z "$replace_line" ]] && continue

            lhs=$(echo "${replace_line%%=>*}" | xargs)
            rhs=${replace_line#*=> }
            if [[ "$rhs" == .* || "$rhs" == ../* ]]; then
                if [[ "$lhs" == github.com/Sawmills/* ]]; then
                    echo "$lhs => $(relpath_from_builder "$mod_path" "$rhs")" >> "$tmp_replaces"
                fi
                continue
            fi
            if [[ "$rhs" != github.com/Sawmills/* ]]; then
                continue
            fi

            echo "$replace_line" >> "$tmp_replaces"
        done
done

sort -u "$tmp_replaces" | sed 's/^/  - /' >> "$CONFIG_OUT"

echo "Wrote replace statements to $CONFIG_OUT"
