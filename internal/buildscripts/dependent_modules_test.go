// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package buildscripts

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestDependentModuleSelection(t *testing.T) {
	for _, tc := range []struct{ name, source, want string }{
		{"root_does_not_match_children", "internal/buildscripts/main.go", "rootconsumer"},
		{"exact_requirements", "component/main.go", "block\nquoted\nsingle"},
		{"multiple_sources", "component/main.go block/main.go", "quoted\nsingle"},
		{"empty_sources", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			files := map[string]string{
				"go.mod":              "module example.com/root\n",
				"component/go.mod":    "module example.com/root/component\n",
				"block/go.mod":        "module example.com/root/block\nrequire (\n example.com/root/component v1.0.0 // indirect\n)\n",
				"single/go.mod":       "module example.com/root/single\nrequire example.com/root/component v1.0.0\n",
				"quoted/go.mod":       "module example.com/root/quoted\nrequire \"example.com/root/component\" v1.0.0\n",
				"prefix/go.mod":       "module example.com/root/prefix\nrequire example.com/root/component/child v1.0.0\n",
				"replacement/go.mod":  "module example.com/root/replacement\nreplace example.com/root/component => ../component\n",
				"comment/go.mod":      "module example.com/root/comment\n// require example.com/root/component v1.0.0\n",
				"rootconsumer/go.mod": "module example.com/root/rootconsumer\nrequire example.com/root v1.0.0\n",
			}
			for name, body := range files {
				path := filepath.Join(dir, name)
				mustMkdirAll(t, filepath.Dir(path))
				mustWriteFile(t, path, body)
			}
			selected, err := DependentModules(t.Context(), dir, strings.Fields(tc.source))
			if err != nil {
				t.Fatal(err)
			}
			got := strings.Join(selected, "\n")
			if got != tc.want {
				t.Fatalf("selected %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDependentModuleSelectionFailsClosed(t *testing.T) {
	for _, tc := range []struct{ name, mod, source string }{
		{"invalid_module", "module example.com/root\nrequire (", "main.go"},
		{"missing_directive", "go 1.26.0\n", "main.go"},
		{"outside_repository", "module example.com/root\n", "../main.go"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			mustWriteFile(t, filepath.Join(dir, "go.mod"), tc.mod)
			if _, err := DependentModules(t.Context(), dir, []string{tc.source}); err == nil {
				t.Fatal("expected selection error")
			}
		})
	}
}
