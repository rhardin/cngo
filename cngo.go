// cngo - simple key-value store toy
package main

import (
	"errors"
)

var store = make(map[string]string)

// ErrorNoSuchKey describes missing keys
var ErrorNoSuchKey = errors.New("no such key")

// Get a value stored at key
func Get(key string) (string, error) {
	value, ok := store[key]

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

// Put something in our store ref'd by key
func Put(key, value string) error {
	store[key] = value

	return nil
}

// Delete a value at key
func Delete(key string) error {
	delete(store, key)

	return nil
}

func main() {
	server()
}
