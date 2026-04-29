// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandardRegistriesIncludeSawmillsFactories(t *testing.T) {
	expected := []string{
		"DdStatusRemapper",
		"Contains",
		"EndsWith",
		"StartsWith",
		"IsInRange",
		"split_metric_by_attributes",
		"FromContext",
	}

	converters := StandardConverters[any]()
	functions := StandardFuncs[any]()
	for _, name := range expected {
		t.Run(name, func(t *testing.T) {
			converter, ok := converters[name]
			require.True(t, ok)
			require.Equal(t, name, converter.Name())

			function, ok := functions[name]
			require.True(t, ok)
			require.Equal(t, name, function.Name())
		})
	}
}
