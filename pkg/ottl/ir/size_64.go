// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build amd64 || arm64 || ppc64le || s390x || riscv64

package ir

import "unsafe"

// Compile-time size assertion: Value must stay 24 bytes on 64-bit targets.
var _ [24]byte = [unsafe.Sizeof(Value{})]byte{}
