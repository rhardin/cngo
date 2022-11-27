// cngo - simple key-value store toy
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
)

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

// TransactionLogger interface for our state store
type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()
}

// EventType kind
type EventType byte

// kinds of events to serialize
const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

// Event persistence data type
type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

var logger TransactionLogger

// FileTransactionLogger data type for event streams and state
type FileTransactionLogger struct {
	events       chan<- Event // write only channel for sending events
	errors       <-chan error // read only channel for sending errors
	lastSequence uint64       // the last used num
	file         *os.File
}

func initTransactionLogger() error {
	var err error

	logger, err := MakeFileTransactionLogger("transaction.log")
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

// MakeFileTransactionLogger constructor-ish a FNL
func MakeFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &FileTransactionLogger{file: file}, nil
}

// Run does the file transaction logging
func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(l.file, "%d\t%d\t%s\t%s\n", l.lastSequence, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

// ReadEvents gets the transaction log and reads it into channels
func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			// Sanity check: are the sequence numbers ascending order?
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			l.lastSequence = e.Sequence
			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}

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

// WritePut send put events
func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

// WriteDelete send delete events
func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

// Err send errors on channel
func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

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

	logger.WritePut(key, string(val))

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

	logger.WriteDelete(key)

	w.WriteHeader(http.StatusOK)
}

func main() {
	initTransactionLogger()

	r := mux.NewRouter()

	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}
