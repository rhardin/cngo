package main

import (
	"testing"
)

func TestStore(t *testing.T) {
	t.Run("Get Should Accept Strings", func(t *testing.T) {
		expect := "was here"
		_ = Put("rob", "was here")
		got, ok := Get("rob")

		if ok != nil {
			t.Error(ok)
		}

		if got != expect {
			t.Errorf("Want: %s; Got: %s", expect, got)
		}
	})

	t.Run("Get Should Error on Bad Keys", func(t *testing.T) {
		_, ok := Get("bad key")

		if ok != ErrorNoSuchKey {
			t.Error(ok)
		}
	})

	t.Run("Put Is Idempotent", func(t *testing.T) {
		expect := "was here"
		_ = Put("rob", expect)
		ok := Put("rob", expect)

		if ok != nil {
			t.Error(ok)
		}

		got, _ := Get("rob")

		if got != expect {
			t.Errorf("Want: %s; Got: %s", expect, got)
		}
	})

	t.Run("Delete Should Remove Key and Value", func(t *testing.T) {
		Put("delete me", "42")
		Delete("delete me")
		_, ok := Get("delete me")

		if ok != ErrorNoSuchKey {
			t.Error(ok)
		}
	})
}
