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

# The Go vulnerability database currently maps these Docker Engine daemon-only
# advisories to every package and symbol in the legacy monolithic module.
DOCKER_DAEMON_ONLY_OSVS = {
    "GO-2026-5617",
    "GO-2026-5668",
}

DOCKER_DAEMON_PACKAGES = {
    "github.com/docker/docker/daemon",
    "github.com/moby/moby/daemon",
    "github.com/moby/moby/v2/daemon",
}

GOVULNCHECK_VULNERABILITIES_FOUND = 3


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


def finding_has_symbol(entry: dict[str, Any]) -> bool:
    finding = entry.get("finding")
    if not isinstance(finding, dict):
        return False

    trace = finding.get("trace")
    if not isinstance(trace, list):
        return False

    return any(isinstance(frame, dict) and "function" in frame for frame in trace)


def finding_is_ignored(entry: dict[str, Any]) -> bool:
    osv_id = finding_osv_id(entry)
    if osv_id in IGNORED_OSVS:
        return True
    if osv_id not in DOCKER_DAEMON_ONLY_OSVS:
        return False

    finding = entry.get("finding")
    if not isinstance(finding, dict):
        return False
    trace = finding.get("trace")
    if not isinstance(trace, list) or not trace:
        return False
    vulnerable_frame = trace[0]
    if not isinstance(vulnerable_frame, dict):
        return False
    vulnerable_package = vulnerable_frame.get("package")
    if not isinstance(vulnerable_package, str):
        return False

    return not any(
        vulnerable_package == daemon_package
        or vulnerable_package.startswith(f"{daemon_package}/")
        for daemon_package in DOCKER_DAEMON_PACKAGES
    )


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
    symbol_findings = [
        osv_id
        for entry in entries
        if finding_has_symbol(entry) and (osv_id := finding_osv_id(entry)) is not None
    ]
    remaining_symbol_findings = [
        osv_id
        for entry in entries
        if finding_has_symbol(entry)
        and not finding_is_ignored(entry)
        and (osv_id := finding_osv_id(entry)) is not None
    ]

    if json_result.returncode not in (0, GOVULNCHECK_VULNERABILITIES_FOUND):
        sys.stdout.write(json_result.stdout)
        sys.stderr.write(json_result.stderr)
        return json_result.returncode

    if json_result.returncode != 0 and not findings:
        sys.stdout.write(json_result.stdout)
        sys.stderr.write(json_result.stderr)
        return json_result.returncode

    if remaining_symbol_findings:
        text_result = run(govulncheck_cmd)
        sys.stdout.write(text_result.stdout)
        sys.stderr.write(text_result.stderr)
        return text_result.returncode

    if symbol_findings:
        ignored = ", ".join(sorted(set(symbol_findings)))
        print(
            f"govulncheck findings limited to ignored no-fix advisories: {ignored}",
            file=sys.stderr,
        )
        return 0

    return json_result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
