// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package buildscripts // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/buildscripts"

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os/exec"
	"path/filepath"
	"slices"
)

type moduleDescription struct {
	Module  struct{ Path string }
	Require []struct{ Path string }
}

// DependentModules returns modules that require a changed source module.
// It excludes the changed modules, which the workflow tests separately.
func DependentModules(ctx context.Context, root string, sources []string) ([]string, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	modules := make(map[string]moduleDescription)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && entry.Name() == ".git" {
			return filepath.SkipDir
		}
		if entry.IsDir() || entry.Name() != "go.mod" {
			return nil
		}
		// Use Go's parser so comments, quoted paths, and replacement-only entries
		// cannot turn into false dependency matches.
		cmd := exec.CommandContext(ctx, "go", "mod", "edit", "-json", path)
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		var module moduleDescription
		if decodeErr := json.Unmarshal(output, &module); decodeErr != nil {
			return fmt.Errorf("decode %s: %w", path, decodeErr)
		}
		if module.Module.Path == "" {
			return fmt.Errorf("missing module directive in %s", path)
		}
		dir, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		modules[dir] = module
		return nil
	})
	if err != nil {
		return nil, err
	}
	changedDirs := make(map[string]bool)
	changedPaths := make(map[string]bool)
	for _, source := range sources {
		if !filepath.IsLocal(source) {
			return nil, fmt.Errorf("changed source is outside repository: %s", source)
		}
		dir := filepath.Dir(filepath.Clean(source))
		for {
			if module, ok := modules[dir]; ok {
				changedDirs[dir] = true
				changedPaths[module.Module.Path] = true
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				return nil, fmt.Errorf("no module for changed source %s", source)
			}
			dir = parent
		}
	}
	var dependents []string
	for dir, module := range modules {
		if changedDirs[dir] {
			continue
		}
		for _, requirement := range module.Require {
			if changedPaths[requirement.Path] {
				dependents = append(dependents, filepath.ToSlash(dir))
				break
			}
		}
	}
	slices.Sort(dependents)
	return dependents, nil
}
