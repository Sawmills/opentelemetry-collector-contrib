// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/buildscripts"
)

func main() {
	modules, err := buildscripts.DependentModules(context.Background(), ".", strings.Fields(os.Getenv("CHANGED_GOLANG_SOURCES")))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, module := range modules {
		fmt.Println(module)
	}
}
