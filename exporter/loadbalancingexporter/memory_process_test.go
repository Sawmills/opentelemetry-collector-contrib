// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"bytes"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// HeapAlloc describes the whole process. Give each memory scenario a fresh
// process so earlier tests and -count repetitions cannot affect its baseline.
func runMemoryTestInSubprocess(t *testing.T) bool {
	t.Helper()
	const childEnv = "SAWMILLS_MEMORY_TEST_CHILD"
	if os.Getenv(childEnv) == t.Name() {
		return false
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1", "-test.timeout=180s", "-test.v") // #nosec G204 -- Runs only this test binary and the current test.
	cmd.Env = append(os.Environ(), childEnv+"="+t.Name())
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exitCode := -1
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	// Keep bounded, redacted diagnostics before checking child success.
	t.Logf("memory test child exit=%d\nstdout: %s\nstderr: %s", exitCode,
		redactedMemoryTestOutput(stdout.String()), redactedMemoryTestOutput(stderr.String()))
	require.NoError(t, err, "isolated memory scenario failed")
	return true
}

func redactedMemoryTestOutput(output string) string {
	lines := strings.Split(output, "\n")
	for i, line := range lines {
		lower := strings.ToLower(line)
		for _, sensitive := range []string{"token", "key", "secret", "password", "credential", "authorization", "bearer"} {
			if strings.Contains(lower, sensitive) {
				lines[i] = "[REDACTED]"
				break
			}
		}
	}
	redacted := strings.Join(lines, "\n")
	if len(redacted) <= 4096 {
		return redacted
	}
	return redacted[:2048] + "\n[middle omitted]\n" + redacted[len(redacted)-2048:]
}
