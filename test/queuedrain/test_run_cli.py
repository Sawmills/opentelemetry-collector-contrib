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

    def test_red_num_consumers_does_not_leak_to_green_render(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    "bash",
                    str(RUN_SH),
                    "--render-only",
                    "--artifacts",
                    tmp,
                    "--red-num-consumers",
                    "120",
                    "--num-consumers",
                    "30",
                    "--green-active-lb-replicas",
                    "4",
                ],
                cwd=ROOT_DIR,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            red_vars = pathlib.Path(tmp, "red", "rendered", "render-vars.txt").read_text()
            green_vars = pathlib.Path(tmp, "green", "rendered", "render-vars.txt").read_text()
            green_config = pathlib.Path(tmp, "green", "rendered", "lb-config.yaml").read_text()

            self.assertIn("num_consumers=120", red_vars)
            self.assertIn("num_consumers=30", green_vars)
            self.assertIn("num_consumers: 30", green_config)
            self.assertIn("active_load_balancer_replicas: 4", green_config)


if __name__ == "__main__":
    unittest.main()
