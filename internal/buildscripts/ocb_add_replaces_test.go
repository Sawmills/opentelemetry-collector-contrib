package buildscripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOCBAddReplacesPreservesRelativePaths(t *testing.T) {
	t.Parallel()

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	scriptPath := filepath.Join(repoRoot, "internal", "buildscripts", "ocb-add-replaces.sh")

	tempRepo := t.TempDir()
	mustMkdirAll(t, filepath.Join(tempRepo, "cmd", "demo"))
	mustMkdirAll(t, filepath.Join(tempRepo, "cmd", "telemetrygen"))
	mustMkdirAll(t, filepath.Join(tempRepo, "exporter", "loadbalancingexporter"))
	mustMkdirAll(t, filepath.Join(tempRepo, "internal", "common"))
	mustMkdirAll(t, filepath.Join(tempRepo, "sawmills-helper"))

	mustWriteFile(t, filepath.Join(tempRepo, "cmd", "demo", "builder-config.yaml"), "dist:\n  name: demo\n")
	mustWriteFile(t, filepath.Join(tempRepo, "go.mod"), "module github.com/open-telemetry/opentelemetry-collector-contrib\n\ngo 1.24.0\n")
	mustWriteFile(t, filepath.Join(tempRepo, "cmd", "telemetrygen", "go.mod"), "module github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen\n\ngo 1.24.0\n")
	mustWriteFile(t, filepath.Join(tempRepo, "exporter", "loadbalancingexporter", "go.mod"), strings.Join([]string{
		"module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter",
		"",
		"go 1.24.0",
		"",
		"replace (",
		"\tgithub.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common",
		"\tgithub.com/Sawmills/helper => ../../sawmills-helper",
		")",
		"",
	}, "\n"))

	cmd := exec.Command("bash", scriptPath, "demo")
	cmd.Dir = tempRepo
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script failed: %v\n%s", err, output)
	}

	builderConfig, err := os.ReadFile(filepath.Join(tempRepo, "cmd", "demo", "builder-config-replaced.yaml"))
	if err != nil {
		t.Fatalf("read builder config: %v", err)
	}

	content := string(builderConfig)
	assertContains(t, content, "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen => ../telemetrygen")
	assertContains(t, content, "github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common")
	assertContains(t, content, "github.com/Sawmills/helper => ../../sawmills-helper")
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()

	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertContains(t *testing.T, content, want string) {
	t.Helper()

	if !strings.Contains(content, want) {
		t.Fatalf("missing %q in output:\n%s", want, content)
	}
}
