// cngo - simple key-value store toy
package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

// ErrorNoSuchKey describes missing keys
var ErrorNoSuchKey = errors.New("no such key")

// Get a value stored at key
func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.m[key]
	store.RUnlock()
	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

// Put something in our store ref'd by key
func Put(key, value string) error {
	store.Lock()
	store.m[key] = value
	store.Unlock()
	return nil
}

// Delete a value at key
func Delete(key string) error {
	store.Lock()
	delete(store.m, key)
	store.Unlock()
	return nil
}

func initTransactionLogger() error {
	var err error

	logger, err := Logger.MakeFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}

func main() {
	initTransactionLogger()

	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}
