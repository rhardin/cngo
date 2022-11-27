// cngo - simple key-value toy
package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var transact *FileTransactionLogger

var kvs = KVS{M: make(map[string]string)}

func initTransactionLogger() error {
	var err error

	transact, err := MakeFileTransactionLogger("transact.log")
	if err != nil {
		return fmt.Errorf("failed to create event  %w", err)
	}

	events, errors := transact.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = kvs.Delete(e.Key)
			case EventPut:
				err = kvs.Put(e.Key, e.Value)
			}
		}
	}

	transact.Run()

	return err
}

// KeyValuePutHandler exoects to be called from http PUT at
// "/v1/key/{key}" resource.
func KeyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	err = kvs.Put(key, string(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("tlog.lastsequence is %d", transact.lastSequence)
	transact.WritePut(key, string(val))
	log.Printf("PUT key=%s value=%s\n", key, val)

	w.WriteHeader(http.StatusCreated)
}

// KeyValueGetHandler expects to be called from http GET at
// "/v1/key/{key}" resource.
func KeyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := kvs.Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
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
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transact.WriteDelete(key)

	w.WriteHeader(http.StatusOK)
}

func main() {
	err := initTransactionLogger()
	if err != nil {
		panic(err)
	}

	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", KeyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", KeyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", KeyValueDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}
