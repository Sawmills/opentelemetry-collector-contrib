// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ctxprofilecommon // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/internal/ctxprofilecommon"

import (
	"context"
	"fmt"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/internal/ctxutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

type ProfileAttributable interface {
	AttributeIndices() pcommon.Int32Slice
}

type attributeSource[K any] = func(ctx K) (pprofile.ProfilesDictionary, ProfileAttributable)

func AccessAttributes[K any](source attributeSource[K]) ottl.StandardGetSetter[K] {
	return ottl.StandardGetSetter[K]{
		Getter: func(_ context.Context, tCtx K) (any, error) {
			dict, attributable := source(tCtx)
			return pprofile.FromAttributeIndices(dict.AttributeTable(), attributable, dict), nil
		},
		Setter: func(_ context.Context, tCtx K, val any) error {
			m, err := ctxutil.GetMap(val)
			if err != nil {
				return err
			}

			dict, attributable := source(tCtx)
			attributable.AttributeIndices().FromRaw([]int32{})
			for k, v := range m.All() {
				kvu := pprofile.NewKeyValueAndUnit()
				keyIdx, err := pprofile.SetString(dict.StringTable(), k)
				if err != nil {
					return err
				}
				kvu.SetKeyStrindex(keyIdx)
				v.CopyTo(kvu.Value())
				idx, err := pprofile.SetAttribute(dict.AttributeTable(), kvu)
				if err != nil {
					return err
				}
				attributable.AttributeIndices().Append(idx)
			}
			return nil
		},
	}
}

func AccessAttributesKey[K any](key []ottl.Key[K], source attributeSource[K]) ottl.GetSetter[K] {
	getSetter := ottl.StandardGetSetter[K]{
		Getter: func(ctx context.Context, tCtx K) (any, error) {
			dict, attributable := source(tCtx)
			return ctxutil.GetMapValue[K](ctx, tCtx, pprofile.FromAttributeIndices(dict.AttributeTable(), attributable, dict), key)
		},
		Setter: func(ctx context.Context, tCtx K, val any) error {
			newKey, err := ctxutil.GetMapKeyName(ctx, tCtx, key[0])
			if err != nil {
				return err
			}

			dict, attributable := source(tCtx)
			v := getAttributeValue(dict, attributable.AttributeIndices(), *newKey)
			err = ctxutil.SetIndexableValue[K](ctx, tCtx, v, val, key[1:])
			if err != nil {
				return err
			}

			kvu := pprofile.NewKeyValueAndUnit()
			keyIdx, err := pprofile.SetString(dict.StringTable(), *newKey)
			if err != nil {
				return err
			}
			kvu.SetKeyStrindex(keyIdx)
			v.CopyTo(kvu.Value())
			idx, err := pprofile.SetAttribute(dict.AttributeTable(), kvu)
			if err != nil {
				return err
			}

			for k, i := range attributable.AttributeIndices().All() {
				if i == idx {
					return nil
				}

				attr := dict.AttributeTable().At(int(i))
				if attr.KeyStrindex() == keyIdx {
					attributable.AttributeIndices().SetAt(k, idx)
					return nil
				}
			}

			attributable.AttributeIndices().Append(idx)
			return nil
		},
	}
	if literalKey, ok := literalStringKeyFromKeys(key); ok {
		return ottl.StandardVMGetSetter[K]{
			StandardGetSetter: getSetter,
			VMGetterFunc: func(_ context.Context, tCtx K) (ir.Value, error) {
				dict, attributable := source(tCtx)
				val, ok := GetAttributeValue(dict, attributable, literalKey)
				if !ok {
					return ir.Value{}, fmt.Errorf("attribute %q not found", literalKey)
				}
				return ctxutil.ValueToVM(val)
			},
			VMAttrKeyValue: literalKey,
			VMAttrKeySet:   ok,
			VMAttrSetterFunc: func(tCtx K, key string, val ir.Value) error {
				dict, attributable := source(tCtx)
				return SetAttributeValueFromVM(dict, attributable, key, val)
			},
		}
	}
	return getSetter
}

func GetAttributeValue(dict pprofile.ProfilesDictionary, attributable ProfileAttributable, key string) (pcommon.Value, bool) {
	strTable := dict.StringTable()
	kvuTable := dict.AttributeTable()

	for _, tableIndex := range attributable.AttributeIndices().All() {
		attr := kvuTable.At(int(tableIndex))
		attrKey := strTable.At(int(attr.KeyStrindex()))
		if attrKey == key {
			v := pcommon.NewValueEmpty()
			attr.Value().CopyTo(v)
			return v, true
		}
	}

	return pcommon.NewValueEmpty(), false
}

func getAttributeValue(dict pprofile.ProfilesDictionary, indices pcommon.Int32Slice, key string) pcommon.Value {
	strTable := dict.StringTable()
	kvuTable := dict.AttributeTable()

	for _, tableIndex := range indices.All() {
		attr := kvuTable.At(int(tableIndex))
		attrKey := strTable.At(int(attr.KeyStrindex()))
		if attrKey == key {
			// Copy the value because OTTL expects to do inplace updates for the values.
			v := pcommon.NewValueEmpty()
			attr.Value().CopyTo(v)
			return v
		}
	}

	return pcommon.NewValueEmpty()
}

// SetAttributeValueFromVM sets an attribute value from a VM value for a given key.
func SetAttributeValueFromVM(dict pprofile.ProfilesDictionary, attributable ProfileAttributable, key string, val ir.Value) error {
	kvu := pprofile.NewKeyValueAndUnit()
	keyIdx, err := pprofile.SetString(dict.StringTable(), key)
	if err != nil {
		return err
	}
	kvu.SetKeyStrindex(keyIdx)
	if err := setValueFromVM(kvu.Value(), val); err != nil {
		return err
	}
	idx, err := pprofile.SetAttribute(dict.AttributeTable(), kvu)
	if err != nil {
		return err
	}

	indices := attributable.AttributeIndices()
	for i, tableIdx := range indices.All() {
		attr := dict.AttributeTable().At(int(tableIdx))
		if attr.KeyStrindex() == keyIdx {
			indices.SetAt(i, idx)
			return nil
		}
	}
	indices.Append(idx)
	return nil
}

func setValueFromVM(val pcommon.Value, vmVal ir.Value) error {
	switch vmVal.Type {
	case ir.TypeInt:
		val.SetInt(int64(vmVal.Num))
		return nil
	case ir.TypeFloat:
		val.SetDouble(math.Float64frombits(vmVal.Num))
		return nil
	case ir.TypeBool:
		val.SetBool(vmVal.Num != 0)
		return nil
	case ir.TypeString:
		s, ok := vmVal.String()
		if !ok {
			return fmt.Errorf("invalid string value")
		}
		val.SetStr(s)
		return nil
	default:
		return fmt.Errorf("unsupported VM value type: %v", vmVal.Type)
	}
}

func literalStringKeyFromKeys[K any](keys []ottl.Key[K]) (string, bool) {
	if len(keys) != 1 {
		return "", false
	}
	literal, ok := keys[0].(ottl.LiteralStringKey)
	if !ok {
		return "", false
	}
	return literal.LiteralString()
}
