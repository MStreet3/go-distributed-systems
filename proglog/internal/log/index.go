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
	return nil, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	return 0, 0, io.EOF
}

func (i *index) Write(off uint32, pos uint64) error {
	return nil
}

func (i *index) Close() error {
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
