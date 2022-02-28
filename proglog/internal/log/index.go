package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// Get file stats and set size before expansion
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// Grow file to max index size before memory mapping
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// Memory map the index file
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// Error if file is empty
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// Convert in to index
	if in == -1 {
		out = uint32(i.size/entWidth - 1) // Get last
	} else {
		out = uint32(in) // Get in
	}
	pos = uint64(out) * entWidth

	// Error if position is out of bounds
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// Error if adding entry exceeds max bytes
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// Write encoded values to the memory map
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// Update the file size by the length of one entry
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Close() error {
	// Sync mem map changes to file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// Sync file changes to disk
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Shrink file back to size
	if err := os.Truncate(i.Name(), int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Last() (out uint32, pos uint64, err error) {
	return i.Read(-1)
}
