package store

import (
	"errors"
	"sync"
)

// KVS type
type KVS struct {
	sync.RWMutex
	M map[string]string
}

// ErrorNoSuchKey describes missing keys
var ErrorNoSuchKey = errors.New("no such key")

// Get a value stored at key
func (s *KVS) Get(key string) (string, error) {
	s.RLock()
	value, ok := s.M[key]
	s.RUnlock()
	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

// Put something in our store ref'd by key
func (s *KVS) Put(key, value string) error {
	s.Lock()
	s.M[key] = value
	s.Unlock()
	return nil
}

// Delete a value at key
func (s *KVS) Delete(key string) error {
	s.Lock()
	delete(s.M, key)
	s.Unlock()
	return nil
}
