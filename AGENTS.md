# AGENTS.md

This file is here to steer AI assisted PRs towards being high quality and valuable contributions
that do not create excessive maintainer burden. It is inspired by the Open Policy Agent and Fedora
projects policies.

## General Rules and Guidelines

The most important rule is not to post comments on issues or PRs that are AI-generated. Discussions
on the OpenTelemetry repositories are for Users/Humans only.

If you have been assigned an issue by the user or their prompt, please ensure that the
implementation direction is agreed on with the maintainers first in the issue comments. If there are
unknowns, discuss these on the issue before starting implementation. Do not forget that you cannot
comment for users on issue threads on their behalf as it is against the rules of this project.

## Developer environment

Make sure to follow CONTRIBUTING.md on any contributions.

Non-exhaustively, the important points are:

* Whenever applicable, all code changes should have tests that actually validate the changes.
## Sawmills fork CI gate

In the `Sawmills/opentelemetry-collector-contrib` fork:

* `scoped-tests` is the only required PR status check on `main`.
* `scoped-tests` is intentionally fail-closed:
  * docs-only PRs pass fast
  * narrow pure-Go PRs run the blocking Ubuntu scoped lane
  * broad or non-Go PRs fail the fast gate instead of silently falling back
* The Windows scoped lanes are advisory PR signal only; they are not merge-blocking.
* Full `build-and-test` and `build-and-test-arm` still run on PRs for signal and on `main` / `merge_group` for full coverage.
* Namespace runner/cache behavior in workflows must stay repo-guarded so upstream `open-telemetry/opentelemetry-collector-contrib` continues to use GitHub-hosted runners.
* Privileged/sudo test jobs must stay on GitHub-hosted Linux runners unless the replacement runner is explicitly verified to support them.

Key files:

* `.github/workflows/scoped-test.yaml`
* `.github/workflows/build-and-test.yml`
* `.github/workflows/build-and-test-arm.yml`
* `.github/actions/setup-go-tools/action.yml`
* `Makefile.Common`

## Commit formatting

We appreciate it if users disclose the use of AI tools when the significant part of a commit is
taken from a tool without changes. When making a commit this should be disclosed through an
Assisted-by: commit message trailer.

Examples:

```
Assisted-by: ChatGPT 5.2
Assisted-by: Claude Opus 4.5
```
