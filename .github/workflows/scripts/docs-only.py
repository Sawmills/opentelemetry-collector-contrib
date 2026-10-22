"""Print whether a complete Git diff changes only human-facing documentation."""
import subprocess
import sys


base, head = sys.argv[1:]
if not base or set(base) == {"0"}:
    print("false")
else:
    paths = subprocess.check_output([
        "git", "diff", "--no-renames", "--name-only", "-z", f"{base}...{head}", "--",
    ]).split(b"\0")[:-1]
    deleted = subprocess.check_output([
        "git", "diff", "--no-renames", "--name-only", "-z", "--diff-filter=D",
        f"{base}...{head}", "--",
    ])
    docs_only = not deleted and bool(paths) and all(
        path.rsplit(b"/", 1)[-1] not in (b"AGENTS.md", b"CLAUDE.md")
        and (path in (b"README.md", b"CONTRIBUTING.md", b"LICENSE")
             or path.startswith(b"docs/") and path.endswith(b".md"))
        for path in paths
    )
    print(str(docs_only).lower())
