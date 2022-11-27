// Package logger - logging and persistence for cngo kvs
package logger

import (
	"bufio"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
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
	wg           *sync.WaitGroup
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

// ReadEvents reads the transaction log in the postgres db
func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outError)

		query := `select sequence, event_type, key, value from Transactions order by sequence`

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}

		defer rows.Close()

		e := Event{}

		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read error: %w", err)
		}

	}()
	return outEvent, outError
}

// Run runs the the thing
func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		query := `insert into Transactions (event_type, key, value) values ($1, $2, $3)`

		for e := range events {
			_, err := l.db.Exec(query, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
			}
		}
	}()
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	return true, nil
}

func (l *PostgresTransactionLogger) createTable() error {
	return nil
}

// MakeFileTransactionLogger constructor-ish a FNL
func MakeFileTransactionLogger(filename string) (*FileTransactionLogger, error) {
	var err error
	var l FileTransactionLogger = FileTransactionLogger{wg: &sync.WaitGroup{}}
	l.file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &l, nil
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

	// Start retrieving events from the events channel and writing them
	// to the transaction log
	go func() {
		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- fmt.Errorf("cannot write to log file: %w", err)
			}

			l.wg.Done()
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

			fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value)

			// Sanity check: are the sequence numbers ascending order?
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			uv, err := url.QueryUnescape(e.Value)
			if err != nil {
				outError <- fmt.Errorf("vaalue decoding failure: %w", err)
				return
			}

			e.Value = uv
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

// Wait for io
func (l *FileTransactionLogger) Wait() {
	l.wg.Wait()
}

// WritePut send put events
func (l *FileTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

// WriteDelete send delete events
func (l *FileTransactionLogger) WriteDelete(key string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventDelete, Key: key}
}

// Close the connection to io
func (l *FileTransactionLogger) Close() error {
	l.wg.Wait()

	if l.events != nil {
		close(l.events) // Terminates Run loop and goroutine
	}

	return l.file.Close()
}

// Err send errors on channel
func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}
