#!/usr/bin/env python3

import importlib.util
import io
import json
import pathlib
import subprocess
import sys
import unittest
import unittest.mock
from typing import Optional


SCRIPT_PATH = pathlib.Path(__file__).with_name("filter-govulncheck.py")
SPEC = importlib.util.spec_from_file_location("filter_govulncheck", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
FILTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FILTER)


def finding(osv_id: str, package: Optional[str], *, symbol: bool = True) -> str:
    frame = {"module": "example.com/module"}
    if package is not None:
        frame["package"] = package
    if symbol:
        frame["function"] = "Vulnerable"
    return json.dumps({"finding": {"osv": osv_id, "trace": [frame]}})


class FilterGovulncheckTest(unittest.TestCase):
    def run_filter(
        self,
        results: list[subprocess.CompletedProcess[str]],
    ) -> tuple[int, int]:
        with (
            unittest.mock.patch.object(FILTER, "run", side_effect=results) as run,
            unittest.mock.patch.object(
                sys,
                "argv",
                [str(SCRIPT_PATH), "govulncheck", "./..."],
            ),
            unittest.mock.patch.object(sys, "stdout", io.StringIO()),
            unittest.mock.patch.object(sys, "stderr", io.StringIO()),
        ):
            return FILTER.main(), run.call_count

    def test_ignored_client_finding_passes(self) -> None:
        result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding("GO-2026-5617", "github.com/docker/docker/client"),
            "",
        )
        self.assertEqual((0, 1), self.run_filter([result]))

    def test_same_advisory_daemon_finding_fails(self) -> None:
        json_result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding("GO-2026-5617", "github.com/docker/docker/daemon"),
            "",
        )
        text_result = subprocess.CompletedProcess([], 3, "vulnerable", "")
        self.assertEqual((3, 2), self.run_filter([json_result, text_result]))

    def test_nested_moby_daemon_finding_fails(self) -> None:
        json_result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding(
                "GO-2026-5668",
                "github.com/moby/moby/v2/daemon/internal",
            ),
            "",
        )
        text_result = subprocess.CompletedProcess([], 3, "vulnerable", "")
        self.assertEqual((3, 2), self.run_filter([json_result, text_result]))

    def test_nonignored_symbol_finding_fails(self) -> None:
        json_result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding("GO-2099-0001", "example.com/vulnerable"),
            "",
        )
        text_result = subprocess.CompletedProcess([], 3, "vulnerable", "")
        self.assertEqual((3, 2), self.run_filter([json_result, text_result]))

    def test_scanner_error_with_ignored_finding_fails(self) -> None:
        result = subprocess.CompletedProcess(
            [],
            1,
            finding("GO-2026-5617", "github.com/docker/docker/client"),
            "scanner failed",
        )
        self.assertEqual((1, 1), self.run_filter([result]))

    def test_informational_module_finding_preserves_clean_exit(self) -> None:
        result = subprocess.CompletedProcess(
            [],
            0,
            finding("GO-2099-0002", "example.com/module", symbol=False),
            "",
        )
        self.assertEqual((0, 1), self.run_filter([result]))

    def test_informational_module_finding_does_not_revive_ignored_symbol(self) -> None:
        result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding("GO-2026-5617", "github.com/docker/docker/client")
            + finding("GO-2099-0002", "example.com/module", symbol=False),
            "",
        )
        self.assertEqual((3, 1), self.run_filter([result]))

    def test_ignored_module_only_advisory_passes(self) -> None:
        result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding("GO-2026-5746", None, symbol=False),
            "",
        )
        self.assertEqual((0, 1), self.run_filter([result]))

    def test_actual_openpgp_package_is_not_ignored(self) -> None:
        json_result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding(
                "GO-2026-5932",
                "golang.org/x/crypto/openpgp/packet",
            ),
            "",
        )
        text_result = subprocess.CompletedProcess([], 3, "vulnerable", "")
        self.assertEqual((3, 2), self.run_filter([json_result, text_result]))

    def test_prometheus_library_false_positive_passes(self) -> None:
        result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding(
                "GO-2026-5710",
                "github.com/prometheus/prometheus/storage/remote/azuread",
            ),
            "",
        )
        self.assertEqual((0, 1), self.run_filter([result]))

    def test_prometheus_web_server_finding_fails(self) -> None:
        json_result = subprocess.CompletedProcess(
            [],
            FILTER.GOVULNCHECK_VULNERABILITIES_FOUND,
            finding(
                "GO-2026-5662",
                "github.com/prometheus/prometheus/web/api/v1",
            ),
            "",
        )
        text_result = subprocess.CompletedProcess([], 3, "vulnerable", "")
        self.assertEqual((3, 2), self.run_filter([json_result, text_result]))


if __name__ == "__main__":
    unittest.main()
