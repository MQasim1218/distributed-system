package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

/*
	the service needs to know the offset to set on the
	next record appended to the log. The service learns the next record’s offset
	by looking at the last entry of the index, a simple process of reading the last
	12 bytes of the file. However, we mess up this process when we grow the files
	so we can memory-map them. (The reason we resize them now is that, once
	they’re memory-mapped, we can’t resize them, so it’s now or never.) We grow
	the files by appending empty space at the end of them, so the last entry is no
	longer at the end of the file—instead, there’s some unknown amount of space
	between this entry and the file’s end. This space prevents the service from
	restarting properly. That’s why we shut down the service by truncating the
	index files to remove the empty space and put the last entry at the end of the
	file once again. This graceful shutdown returns the service to a state where
	it can restart properly and efficiently.
*/

// The following terms to mean these things:
// • Record—the data stored in our log.
// • Store—the file we store records in.
// • Index—the file we store index entries in.
// • Segment—the abstraction that ties a store and an index together.
// • Log—the abstraction that ties all the segments together.

var (
	offWidth uint64 = 4
	posWidth uint64 = 4
	entWidth uint64 = offWidth + posWidth
)

// ndex defines our index file, which comprises a
// persisted file and a memory-mapped file.
type index struct {
	*os.File
	mmap gommap.MMap

	// The size tells us the size of the index and
	// where to write the next entry appended to the index.
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	indx := &index{
		File: f,
	}

	f_info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	indx.size = uint64(f_info.Size())
	if err = os.Truncate(
		f.Name(),
		int64(c.Segment.MazIndexBytes),
	); err != nil {
		return nil, err
	}

	indx.mmap, err = gommap.Map(
		indx.File.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return indx, nil
}

/*
	{
		Close() makes sure the memory-mapped file has synced its data
		to the persisted file and that the persisted file has flushed
		its contents to stable storage. Then it truncates the persisted
		file to the amount of data that’s actually in it and closes the file.
	}
*/
func (indx *index) Close() error {
	if err := indx.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}

	if err := indx.File.Sync(); err != nil {
		return err
	}

	return indx.File.Close()
}

func (indx *index) Read(in int64) (out uint32, pos uint64, err error) {
	if indx.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((indx.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if indx.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(indx.mmap[pos : pos+offWidth])
	pos = enc.Uint64(indx.mmap[pos : pos+offWidth])

	return
}
