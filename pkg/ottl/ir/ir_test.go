package ir

import "testing"

func TestStringValueStoresDataPointer(t *testing.T) {
	s := "hello"
	v := StringValue(s)

	got, ok := v.String()
	if !ok {
		t.Fatalf("String() not ok")
	}
	if got != s {
		t.Fatalf("got %q want %q", got, s)
	}

	// mutate original variable to ensure we didn't store stack header
	s = "zzz"
	got2, ok := v.String()
	if !ok || got2 != "hello" {
		t.Fatalf("String after stack change got %q ok %v", got2, ok)
	}

	if !StringsEqual(v, v) {
		t.Fatalf("StringsEqual should be true for same value")
	}

	empty := StringValue("")
	if str, ok := empty.String(); !ok || str != "" {
		t.Fatalf("empty string got %q ok %v", str, ok)
	}
}
