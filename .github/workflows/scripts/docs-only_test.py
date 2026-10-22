"""Exercise the docs-only CLI against real Git histories."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CLASSIFIER = Path(__file__).with_name("docs-only.py")


class DocsOnlyTest(unittest.TestCase):
    def test_docs_only_diff(self):
        with tempfile.TemporaryDirectory(prefix="ci-docs-") as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

            git("init", "--quiet")
            git("config", "user.name", "ci-fixture")
            git("config", "user.email", "ci@example.invalid")
            (root / "app.go").write_text("runtime\n")
            git("add", ".")
            git("commit", "--quiet", "-m", "base")
            base = git("rev-parse", "HEAD")
            (root / "README.md").write_text("docs\n")
            git("add", ".")
            git("commit", "--quiet", "-m", "docs")
            result = subprocess.run([sys.executable, str(CLASSIFIER), base, "HEAD"],
                                    cwd=root, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "true")

            def classify(start, end="HEAD"):
                return subprocess.run([sys.executable, str(CLASSIFIER), start, end],
                                      cwd=root, text=True, capture_output=True)

            self.assertEqual(classify("HEAD").stdout.strip(), "false")
            self.assertEqual(classify("").stdout.strip(), "false")
            self.assertNotEqual(classify("invalid-base").returncode, 0)
            for path, expected in (
                ("docs/guide.md", "true"),
                ("exporter/example/README.md", "true"),
                ("docs/line\nname.md", "true"),
                ("docs/AGENTS.md", "false"),
                ("processor/example/prompt.md", "false"),
                ("app.go", "false"),
                (".github/workflows/build-and-test.yml", "false"),
            ):
                before = git("rev-parse", "HEAD")
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("updated\n")
                git("add", ".")
                git("commit", "--quiet", "-m", "change")
                changed = classify(before)
                self.assertEqual(changed.returncode, 0, changed.stderr)
                self.assertEqual(changed.stdout.strip(), expected, path)
            before = git("rev-parse", "HEAD")
            git("mv", "app.go", "docs/renamed.md")
            git("commit", "--quiet", "-m", "rename code to docs")
            self.assertEqual(classify(before).stdout.strip(), "false")
            before = git("rev-parse", "HEAD")
            git("rm", "--quiet", "README.md")
            git("commit", "--quiet", "-m", "delete docs")
            self.assertEqual(classify(before).stdout.strip(), "true")
            self.assertEqual(classify(base).stdout.strip(), "false")


if __name__ == "__main__":
    unittest.main()
