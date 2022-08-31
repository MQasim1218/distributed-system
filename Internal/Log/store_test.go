package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStorageAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// In this test, we create a store with a temporary
	// file and call two test helpers
	// to test appending and reading from the store.
	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// Then we create the store again
	// and test reading from it again to verify
	// that our service will recover its state
	// after a restart.
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testReadAt(t *testing.T, s *Store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)

		require.Equal(t, lenWidth, n)
		off += int64(n)

		sz := enc.Uint64(b)
		b = make([]byte, sz)

		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(sz), n)
		off += int64(n)
	}
}

func testRead(t *testing.T, s *Store) {
	t.Helper()
	var pos uint64

	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testAppend(t *testing.T, s *Store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, befSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, aftSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, aftSize > befSize)
}

func openFile(filename string) (file *os.File, size int64, err error) {
	file, err = os.OpenFile(
		filename,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	f_info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}

	return file, f_info.Size(), nil

}
