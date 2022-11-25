package main

import "testing"

func TestGetShouldAcceptStrings(t *testing.T) {
	var expect = "was here"
	_ = Put("rob", "was here")
	var got, ok = Get("rob")

	if ok != nil {
		t.Error(ok)
	}

	if got != expect {
		t.Errorf("Want: %s; Got: %s", expect, got)
	}
}
