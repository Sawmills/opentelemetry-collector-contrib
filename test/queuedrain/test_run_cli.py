#!/usr/bin/env python3
#
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
#

import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]
RUN_SH = ROOT_DIR / "test" / "queuedrain" / "run.sh"


class RunCliTest(unittest.TestCase):
    def test_replica_flags_reject_non_numeric_values(self):
        env = os.environ.copy()
        env["PATH"] = "/usr/bin:/bin"

        cases = [
            ("--lb-replicas", "many"),
            ("--active-lb-replicas-config", "not-a-number"),
            ("--red-active-lb-replicas", "-1"),
            ("--green-active-lb-replicas", "1.5"),
        ]
        for flag, value in cases:
            with self.subTest(flag=flag):
                result = subprocess.run(
                    ["bash", str(RUN_SH), flag, value],
                    cwd=ROOT_DIR,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "replica values must be non-negative integers",
                    result.stderr,
                )

    def test_render_only_keeps_phase_specific_num_consumers_isolated(self):
        env = os.environ.copy()
        env["PATH"] = "/usr/bin:/bin"

        with tempfile.TemporaryDirectory() as artifact_root:
            result = subprocess.run(
                [
                    "bash",
                    str(RUN_SH),
                    "--render-only",
                    "--phase",
                    "both",
                    "--artifacts",
                    artifact_root,
                    "--num-consumers",
                    "30",
                    "--red-num-consumers",
                    "120",
                    "--green-active-lb-replicas",
                    "10",
                ],
                cwd=ROOT_DIR,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(
                result.returncode,
                0,
                msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
            )

            red_vars = (
                pathlib.Path(artifact_root)
                / "red"
                / "rendered"
                / "render-vars.txt"
            ).read_text()
            green_vars = (
                pathlib.Path(artifact_root)
                / "green"
                / "rendered"
                / "render-vars.txt"
            ).read_text()
            red_config = (
                pathlib.Path(artifact_root) / "red" / "rendered" / "lb-config.yaml"
            ).read_text()
            green_config = (
                pathlib.Path(artifact_root)
                / "green"
                / "rendered"
                / "lb-config.yaml"
            ).read_text()

            self.assertIn("num_consumers=120\n", red_vars)
            self.assertIn("num_consumers=30\n", green_vars)
            self.assertNotIn("active_load_balancer_replicas", red_config)
            self.assertIn("active_load_balancer_replicas: 10", green_config)


if __name__ == "__main__":
    unittest.main()
