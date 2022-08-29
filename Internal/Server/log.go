package server

import (
	"errors"
	"sync"
)

/*
	To append a record to the log, you just append to the slice. Each time we read
	a record given an index, we use that index to look up the record in the slice.
	If the offset given by the client doesn’t exist, we return an error saying that
	the offset doesn’t exist.
*/

type Log struct {
	mu   *sync.Mutex
	recs []Record
}

type Record struct {
	Val    []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var (
	ErrorOffsetNotFound = errors.New("the offset index is invalid")
)

func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(rec Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	rec.Offset = uint64(len(l.recs))
	l.recs = append(l.recs, rec)

	return rec.Offset, nil
}

func (l *Log) Read(offset uint64) (*Record, error) {

	l.mu.Lock()
	defer l.mu.Unlock()

	if offset >= uint64(len(l.recs)) {
		return nil, ErrorOffsetNotFound
	}

	return &l.recs[offset], nil
}
