package log

// The following terms to mean these things:
// • Record—the data stored in our log.
// • Store—the file we store records in.
// • Index—the file we store index entries in.
// • Segment—the abstraction that ties a store and an index together.
// • Log—the abstraction that ties all the segments together.

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

// This is the file that stores all our logs
type Store struct {
	store *os.File
	mu    sync.Mutex
	buf   *bufio.Writer
	size  uint64
}

func newStore(f *os.File) (*Store, error) {
	// Get the Information about the file using os.Stat(filename)
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// Get the file size from the file information object `fi`.
	size := uint64(fi.Size())

	return &Store{
		store: f,
		mu:    sync.Mutex{},
		buf:   bufio.NewWriter(f),
		size:  size,
	}, nil
}

func (s *Store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the current last postion
	pos = s.size
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		// Lets see what the default default values are returned!!
		return
	}

	w, err := s.buf.Write(p)
	if err != nil {
		return
	}

	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}
