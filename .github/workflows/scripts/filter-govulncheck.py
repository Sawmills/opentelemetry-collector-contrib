#!/usr/bin/env python3

import json
import subprocess
import sys
from typing import Any


IGNORED_OSVS = {
    "GO-2026-4883",
    "GO-2026-4887",
    "GO-2026-4923",
}


def parse_json_stream(payload: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    entries: list[dict[str, Any]] = []
    index = 0
    length = len(payload)
    while index < length:
        while index < length and payload[index].isspace():
            index += 1
        if index >= length:
            break
        item, index = decoder.raw_decode(payload, index)
        entries.append(item)
    return entries


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: filter-govulncheck.py <govulncheck> [args...]", file=sys.stderr)
        return 2

    govulncheck_cmd = sys.argv[1:]
    json_result = run([govulncheck_cmd[0], "-format", "json", *govulncheck_cmd[1:]])

    try:
        entries = parse_json_stream(json_result.stdout)
    except json.JSONDecodeError:
        sys.stdout.write(json_result.stdout)
        sys.stderr.write(json_result.stderr)
        return json_result.returncode

    findings = [entry["finding"]["osv"] for entry in entries if "finding" in entry]
    remaining_findings = [finding for finding in findings if finding not in IGNORED_OSVS]

    if remaining_findings:
        text_result = run(govulncheck_cmd)
        sys.stdout.write(text_result.stdout)
        sys.stderr.write(text_result.stderr)
        return text_result.returncode

    if findings:
        ignored = ", ".join(sorted(set(findings)))
        print(
            f"govulncheck findings limited to ignored no-fix advisories: {ignored}",
            file=sys.stderr,
        )
        return 0

    return json_result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
