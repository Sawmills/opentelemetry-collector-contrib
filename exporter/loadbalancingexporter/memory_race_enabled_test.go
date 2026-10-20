// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build race

package loadbalancingexporter

// memoryTestRaceEnabled widens only the recovery drain assertion under race.
// Keep scenario deadlines, queue budgets and delivery checks unchanged.
const memoryTestRaceEnabled = true
