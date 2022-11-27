package main

import (
	"testing"

	"github.com/rhardin/cngo/store"
)

func TestStore(t *testing.T) {
	t.Run("Get Should Accept Strings", func(t *testing.T) {
		expect := "was here"
		_ = kvs.Put("rob", "was here")
		got, ok := kvs.Get("rob")

		if ok != nil {
			t.Error(ok)
		}

		if got != expect {
			t.Errorf("Want: %s; Got: %s", expect, got)
		}
	})

	t.Run("Get Should Error on Bad Keys", func(t *testing.T) {
		_, ok := kvs.Get("bad key")

		if ok != store.ErrorNoSuchKey {
			t.Error(ok)
		}
	})

	t.Run("Put Is Idempotent", func(t *testing.T) {
		expect := "was here"
		_ = kvs.Put("rob", expect)
		ok := kvs.Put("rob", expect)

		if ok != nil {
			t.Error(ok)
		}

		got, _ := kvs.Get("rob")

		if got != expect {
			t.Errorf("Want: %s; Got: %s", expect, got)
		}
	})

	t.Run("Delete Should Remove Key and Value", func(t *testing.T) {
		kvs.Put("delete me", "42")
		kvs.Delete("delete me")
		_, ok := kvs.Get("delete me")

		if ok != store.ErrorNoSuchKey {
			t.Error(ok)
		}
	})
}
