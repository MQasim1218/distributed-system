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
	//  enc defines the encoding that we
	// persist record sizes and index entries in
	enc = binary.BigEndian
)

const (
	//  lenWidth defines the number of bytes used to store the record’s length.
	lenWidth = 8
)

// The store struct is a simple wrapper around a file with two APIs to append
// and read bytes to and from the file.
type Store struct {
	store *os.File
	mu    sync.RWMutex
	buf   *bufio.Writer
	size  uint64
}

// The newStore(*os.File) function creates a store for the given file.
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
		mu:    sync.RWMutex{},
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

	// Write writes the contents of p into the buffer.
	// Returns the number of bytes written.
	w, err := s.buf.Write(p)
	if err != nil {
		return
	}

	w += lenWidth
	// Move the pointer to the new EOF position.
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *Store) Read(pos uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// buf.Flush() flushes the writer buffer, in case we’re about to
	// try to read a record that the buffer hasn’t flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// find out how many bytes we have to read to get the whole record.
	size := make([]byte, lenWidth)
	if _, err := s.store.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Then we return the number of bytes written,
	b := make([]byte, enc.Uint64(size))
	if _, err := s.store.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.store.ReadAt(p, off)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.store.Close()
}
