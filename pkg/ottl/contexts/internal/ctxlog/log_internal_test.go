// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ctxlog

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type compileTimeTestContext struct {
	log                   plog.LogRecord
	cache                 pcommon.Map
	cachedBodyString      string
	cachedBodyStringValid bool
}

func (c *compileTimeTestContext) GetLogRecord() plog.LogRecord {
	return c.log
}

func (c *compileTimeTestContext) GetCache() pcommon.Map {
	return c.cache
}

func (c *compileTimeTestContext) GetCachedBodyString() (string, bool) {
	return c.cachedBodyString, c.cachedBodyStringValid
}

func (c *compileTimeTestContext) SetCachedBodyString(bodyString string) {
	c.cachedBodyString = bodyString
	c.cachedBodyStringValid = true
}

func (c *compileTimeTestContext) InvalidateCachedBodyString() {
	c.cachedBodyString = ""
	c.cachedBodyStringValid = false
}

var _ interface {
	Get(context.Context, *compileTimeTestContext) (any, error)
	GetPcommonValue(context.Context, *compileTimeTestContext) (pcommon.Value, bool, error)
	Set(context.Context, *compileTimeTestContext, any) error
	GetStringLike(context.Context, *compileTimeTestContext) (*string, bool, error)
} = bodyGetSetter[*compileTimeTestContext]{}

func TestBodyGetPcommonValueInvalidatesMutableBodyCache(t *testing.T) {
	ctx := &compileTimeTestContext{log: plog.NewLogRecord(), cache: pcommon.NewMap()}
	ctx.log.Body().SetEmptyMap().PutStr("key", "value")
	ctx.SetCachedBodyString("cached")

	value, found, err := (bodyGetSetter[*compileTimeTestContext]{}).GetPcommonValue(t.Context(), ctx)

	if err != nil {
		t.Fatal(err)
	}
	if !found || value.Type() != pcommon.ValueTypeMap {
		t.Fatalf("got value=%v found=%v", value.Type(), found)
	}
	if _, ok := ctx.GetCachedBodyString(); ok {
		t.Fatal("mutable body access left the cached body string valid")
	}
}

func TestBodyGetPcommonValuePreservesScalarBodyCache(t *testing.T) {
	ctx := &compileTimeTestContext{log: plog.NewLogRecord(), cache: pcommon.NewMap()}
	ctx.log.Body().SetStr("body")
	ctx.SetCachedBodyString("cached")

	value, found, err := (bodyGetSetter[*compileTimeTestContext]{}).GetPcommonValue(t.Context(), ctx)

	if err != nil {
		t.Fatal(err)
	}
	if !found || value.Type() != pcommon.ValueTypeStr {
		t.Fatalf("got value=%v found=%v", value.Type(), found)
	}
	if cached, ok := ctx.GetCachedBodyString(); !ok || cached != "cached" {
		t.Fatalf("got cached=%q valid=%v", cached, ok)
	}
}
