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

DOCKER_DAEMON_PACKAGES = {
    "github.com/docker/docker/daemon",
    "github.com/moby/moby/daemon",
    "github.com/moby/moby/v2/daemon",
}

PACKAGE_SCOPED_OSVS = {
    # The Go vulnerability database currently maps these Docker Engine
    # daemon-only advisories to the legacy monolithic module.
    "GO-2026-5617": DOCKER_DAEMON_PACKAGES,
    "GO-2026-5668": DOCKER_DAEMON_PACKAGES,
    "GO-2026-5746": DOCKER_DAEMON_PACKAGES,
    # openpgp is unmaintained with no fixed x/crypto release.
    "GO-2026-5932": {"golang.org/x/crypto/openpgp"},
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


def ignored_osv_ids(entries: list[dict[str, Any]]) -> set[str]:
    ignored = set(IGNORED_OSVS)
    for osv_id, affected_packages in PACKAGE_SCOPED_OSVS.items():
        reported_packages = set()
        for entry in entries:
            if finding_osv_id(entry) != osv_id:
                continue
            finding = entry.get("finding")
            if not isinstance(finding, dict):
                continue
            trace = finding.get("trace")
            if not isinstance(trace, list) or not trace:
                continue
            vulnerable_frame = trace[0]
            if not isinstance(vulnerable_frame, dict):
                continue
            vulnerable_package = vulnerable_frame.get("package")
            if isinstance(vulnerable_package, str):
                reported_packages.add(vulnerable_package)

        affected_package_reported = any(
            reported_package == affected_package
            or reported_package.startswith(f"{affected_package}/")
            for reported_package in reported_packages
            for affected_package in affected_packages
        )
        if not affected_package_reported:
            ignored.add(osv_id)

    return ignored


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

    ignored_osvs = ignored_osv_ids(entries)
    findings = [osv_id for entry in entries if (osv_id := finding_osv_id(entry)) is not None]
    remaining_findings = [finding for finding in findings if finding not in ignored_osvs]
    remaining_symbol_findings = [
        osv_id
        for entry in entries
        if finding_has_symbol(entry)
        and finding_osv_id(entry) not in ignored_osvs
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

    if remaining_findings:
        return json_result.returncode

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
