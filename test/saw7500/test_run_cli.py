#!/usr/bin/env python3
#
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
#

import os
import pathlib
import subprocess
import unittest


ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]
RUN_SH = ROOT_DIR / "test" / "saw7500" / "run.sh"


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


if __name__ == "__main__":
    unittest.main()
