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

            rhs=${replace_line#*=> }
            if [[ "$rhs" == .* || "$rhs" == ../* ]]; then
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
