package main

import (
	"fmt"
	"os"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/buildscripts"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <chloggen-config-path>\n", os.Args[0])
		os.Exit(2)
	}

	path := os.Args[1]
	content, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
		os.Exit(1)
	}

	deduped, err := buildscripts.DedupeChloggenComponentsConfig(content)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dedupe %s: %v\n", path, err)
		os.Exit(1)
	}

	if err := os.WriteFile(path, deduped, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
		os.Exit(1)
	}
}
