// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package buildscripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func scopedWorkflowStepScript(t *testing.T, name string) string {
	t.Helper()
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "scoped-test.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	_, step, ok := strings.Cut(string(workflow), "      - name: "+name+"\n")
	if !ok {
		t.Fatal("full lint gate step is missing")
	}
	_, body, ok := strings.Cut(step, "        run: |\n")
	if !ok {
		t.Fatal("full lint gate script is missing")
	}
	var script strings.Builder
	for line := range strings.SplitSeq(body, "\n") {
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "          ") {
			break
		}
		script.WriteString(strings.TrimPrefix(line, "          "))
		script.WriteByte('\n')
	}
	return script.String()
}

func TestScopedLintGate(t *testing.T) {
	script := scopedWorkflowStepScript(t, "Wait for full lint")

	for _, tc := range []struct {
		name, state, exitCode string
		wantPass              bool
	}{
		{"success", "success", "0", true},
		{"failure", "failure", "0", false},
		{"cancelled", "cancelled", "0", false},
		{"neutral", "neutral", "0", false},
		{"missing", "pending", "0", false},
		{"unfinished", "in_progress", "0", false},
		{"api_error", "success", "1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := t.TempDir()
			for name, content := range map[string]string{
				"gh": `#!/bin/sh
if [ "$2" = "repos/Sawmills/test/actions/workflows/build-and-test.yml/runs?head_sha=test-head&event=pull_request" ]; then
  echo 123
elif [ "$2" = "repos/Sawmills/test/check-suites/123/check-runs?check_name=lint&filter=latest&per_page=100" ]; then
  printf '%s\n' "$TEST_STATE"
else
  echo "unexpected API request: $*" >&2
  exit 2
fi
exit "$TEST_EXIT_CODE"
`,
				"sleep": "#!/bin/sh\nexit 0\n",
			} {
				// #nosec G306 -- Private temporary command stubs must be executable.
				if err := os.WriteFile(filepath.Join(bin, name), []byte(content), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			// #nosec G204 -- Run the checked-in workflow step against local command stubs.
			cmd := exec.Command("bash", "-c", script)
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"GH_REPO=Sawmills/test", "PR_HEAD=test-head", "TEST_STATE="+tc.state, "TEST_EXIT_CODE="+tc.exitCode)
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.wantPass {
				t.Fatalf("pass=%v, want %v: %v\n%s", err == nil, tc.wantPass, err, output)
			}
		})
	}
}

func TestScopedLintConfigClassification(t *testing.T) {
	for _, tc := range []struct {
		name, files, mode string
	}{
		{"config_only", ".golangci.yml", "scoped"},
		{"config_and_docs", ".golangci.yml\nREADME.md", "scoped"},
		{"config_and_go", ".golangci.yml\nexporter/example/main.go", "scoped"},
		{"config_and_metadata", ".golangci.yml\nexporter/example/metadata.yaml", "full_fallback"},
		{"config_and_schema", ".golangci.yml\nexporter/example/config.schema.yaml", "full_fallback"},
		{"config_and_workflow", ".golangci.yml\n.github/workflows/scoped-test.yaml", "full_fallback"},
		{"makefile_only", "Makefile.Common", "full_fallback"},
		{"makefile_and_go", "Makefile.Common\ninternal/buildscripts/dependent_modules_test.go", "full_fallback"},
		{"config_and_makefile", ".golangci.yml\nMakefile.Common", "full_fallback"},
		{"config_and_unknown", ".golangci.yml\nDockerfile", "full_fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertContains(t, runScopedChanges(t, tc.files, ""), "mode="+tc.mode+"\n")
		})
	}
}

func TestScopedInstructionStubClassification(t *testing.T) {
	for _, tc := range []struct {
		name, files, deleted, mode string
	}{
		{"root_deletion_with_go", "CLAUDE.md\nexporter/example/main.go", "CLAUDE.md", "scoped"},
		{"root_deletion_with_lint_config", ".golangci.yml\nCLAUDE.md", "CLAUDE.md", "scoped"},
		{"root_nondeletion_with_go", "CLAUDE.md\nexporter/example/main.go", "", "full_fallback"},
		{"nested_deletion_with_go", "exporter/CLAUDE.md\nexporter/example/main.go", "exporter/CLAUDE.md", "full_fallback"},
		{"root_and_readme_deletions_with_go", "CLAUDE.md\nREADME.md\nexporter/example/main.go", "CLAUDE.md\nREADME.md", "scoped"},
		{"root_and_go_deletions", "CLAUDE.md\nexporter/example/old.go", "CLAUDE.md\nexporter/example/old.go", "full_fallback"},
		{"root_deletion_and_other_file", "CLAUDE.md\nDockerfile\nexporter/example/main.go", "CLAUDE.md", "full_fallback"},
		{"docs_only_root_deletion", "CLAUDE.md", "CLAUDE.md", "docs_only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertContains(t, runScopedChanges(t, tc.files, tc.deleted), "mode="+tc.mode+"\n")
		})
	}
}

func runScopedChanges(t *testing.T, files, deleted string) string {
	t.Helper()
	bin := t.TempDir()
	stub := `#!/bin/sh
case "$*" in
*merge-base*) echo base;;
*--diff-filter=ACDMRTUXB*) printf '%s\n' "$TEST_FILES";;
*" -- CLAUDE.md") printf '%s\n' "$TEST_DELETED_FILES" | awk '$0 == "CLAUDE.md" { print }';;
*--diff-filter=D*) printf '%s\n' "$TEST_DELETED_FILES";;
esac
`
	// #nosec G306 -- Private temporary command stubs must be executable.
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(bin, "output")
	// #nosec G204 -- Run the checked-in classifier against a local Git stub.
	cmd := exec.Command("bash", "-c", scopedWorkflowStepScript(t, "Get changes"))
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"PR_HEAD=test-head", "PR_BASE=main", "TEST_FILES="+files, "TEST_DELETED_FILES="+deleted, "GITHUB_OUTPUT="+outputPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("classifier failed: %v\n%s", err, output)
	}
	output, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}
