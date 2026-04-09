#!/usr/bin/env python3

import json
import subprocess
import sys
from typing import Any, Optional


IGNORED_OSVS = {
    "GO-2026-4514",
    "GO-2026-4883",
    "GO-2026-4887",
    "GO-2026-4923",
}


def finding_osv_id(entry: dict[str, Any]) -> Optional[str]:
    finding = entry.get("finding")
    if not isinstance(finding, dict):
        return None

    osv = finding.get("osv")
    if isinstance(osv, str):
        return osv
    if isinstance(osv, dict):
        osv_id = osv.get("id")
        if isinstance(osv_id, str):
            return osv_id

    return None


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

    findings = [osv_id for entry in entries if (osv_id := finding_osv_id(entry)) is not None]
    remaining_findings = [finding for finding in findings if finding not in IGNORED_OSVS]

    if json_result.returncode != 0 and not findings:
        sys.stdout.write(json_result.stdout)
        sys.stderr.write(json_result.stderr)
        return json_result.returncode

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
