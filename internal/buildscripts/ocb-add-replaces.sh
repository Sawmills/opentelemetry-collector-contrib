#!/usr/bin/env bash

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$1"
CONFIG_IN="cmd/$DIR/builder-config.yaml"
CONFIG_OUT="cmd/$DIR/builder-config-replaced.yaml"

cp "$CONFIG_IN" "$CONFIG_OUT"

tmp_replaces=$(mktemp)
trap 'rm -f "$tmp_replaces"' EXIT

python3 - "$DIR" >"$tmp_replaces" <<'PY'
import os
import sys
from pathlib import Path


def is_repo_local_module(lhs: str) -> bool:
    return (
        lhs.startswith("github.com/Sawmills/")
        or lhs == "github.com/open-telemetry/opentelemetry-collector-contrib"
        or lhs.startswith("github.com/open-telemetry/opentelemetry-collector-contrib/")
    )


def iter_replace_lines(go_mod: Path):
    in_replace = False

    for raw_line in go_mod.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("//", 1)[0].strip()
        if not line:
            continue

        if line.startswith("replace ("):
            in_replace = True
            continue

        if in_replace and line == ")":
            in_replace = False
            continue

        if line.startswith("replace ") and "(" not in line:
            yield line[len("replace ") :].strip()
            continue

        if in_replace:
            yield line


dir_name = sys.argv[1]
repo = Path(".").resolve()
builder_dir = (repo / "cmd" / dir_name).resolve()
replaces: set[str] = set()

for go_mod in sorted(repo.rglob("go.mod")):
    mod_path = go_mod.parent.resolve()
    suffix = "" if mod_path == repo else "/" + mod_path.relative_to(repo).as_posix()
    replaces.add(
        "github.com/open-telemetry/opentelemetry-collector-contrib"
        f"{suffix} => {os.path.relpath(mod_path, start=builder_dir)}"
    )

    for replace_line in iter_replace_lines(go_mod):
        if "=>" not in replace_line:
            continue

        lhs, rhs = [part.strip() for part in replace_line.split("=>", 1)]
        lhs_module = lhs.split()[0]
        if rhs.startswith(".") and is_repo_local_module(lhs_module):
            target = (go_mod.parent / rhs).resolve()
            replaces.add(f"{lhs_module} => {os.path.relpath(target, start=builder_dir)}")
            continue

        if rhs.startswith("github.com/Sawmills/"):
            replaces.add(f"{lhs_module} => {rhs}")

for replace in sorted(replaces):
    print(f"  - {replace}")
PY

cat "$tmp_replaces" >> "$CONFIG_OUT"

echo "Wrote replace statements to $CONFIG_OUT"
