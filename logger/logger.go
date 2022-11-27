// Package logger - logging and persistence for cngo kvs
package logger

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
)

// Event persistence data type
type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

// EventType kind
type EventType byte

// kinds of events to serialize
const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

// TransactionLogger interface for our state store
type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()
}

// FileTransactionLogger data type for event streams and state
type FileTransactionLogger struct {
	events       chan<- Event // write only channel for sending events
	errors       <-chan error // read only channel for sending errors
	lastSequence uint64       // the last used num
	file         *os.File
}

// PostgresTransactionLogger data type for event streams and state backed by postgres
type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

// PostgresDBParams helper structure for parms
type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

// WritePut for postgres
func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

// WriteDelete for postgres
func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

// Err for postgres
func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

// ReadEvents reads the events from the log table in postgres
func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	return nil, nil
}

// Run runs the the thing
func (l *PostgresTransactionLogger) Run() {
	return
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	return true, nil
}

func (l *PostgresTransactionLogger) createTable() error {
	return nil
}

// MakeFileTransactionLogger constructor-ish a FNL
func MakeFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &FileTransactionLogger{file: file}, nil
}

// MakePostgresTransactionLogger constructor func
func MakePostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s", config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
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
