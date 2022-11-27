// cngo - simple key-value store toy
package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/rhardin/cngo/logger"
	"github.com/rhardin/cngo/store"
)

var tLog logger.TransactionLogger

var kvs = store.KVS{M: make(map[string]string)}

func initTransactionLogger() error {
	var err error

	log, err := logger.MakeFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := log.ReadEvents()
	e, ok := logger.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case logger.EventDelete:
				err = kvs.Delete(e.Key)
			case logger.EventPut:
				err = kvs.Put(e.Key, e.Value)
			}
		}
	}

	log.Run()

	return err
}

// KeyValuePutHandler exoects to be called from http PUT at
// "/v1/key/{key}" resource.
func KeyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = kvs.Put(key, string(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tLog.WritePut(key, string(val))

	w.WriteHeader(http.StatusCreated)
}

// KeyValueGetHandler expects to be called from http GET at
// "/v1/key/{key}" resource.
func KeyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := kvs.Get(key)
	if errors.Is(err, store.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(val))
}

// KeyValueDeleteHandler expects to be called from http DELETE at
// "/v1/key/{key}" resource.
func KeyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := kvs.Delete(key)
	if errors.Is(err, store.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tLog.WriteDelete(key)

	w.WriteHeader(http.StatusOK)
}

func main() {
	initTransactionLogger()

	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", KeyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", KeyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", KeyValueDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}
