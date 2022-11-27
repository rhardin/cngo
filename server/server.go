// Package server - https server for kvs
package server

import (
	"errors"
	"io"
	"net/http"

	"github.com/gorilla/mux"
)

// keyValuePutHandler exoects to be called from http PUT at
// "/v1/key/{key}" resource.
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	Logger.WritePut(key, string(val))

	w.WriteHeader(http.StatusCreated)
}

// keyValueGetHandler expects to be called from http GET at
// "/v1/key/{key}" resource.
func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := Get(key)
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

// keyValueDeleteHandler expects to be called from http DELETE at
// "/v1/key/{key}" resource.
func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := Delete(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	Logger.WriteDelete(key)

	w.WriteHeader(http.StatusOK)
}
