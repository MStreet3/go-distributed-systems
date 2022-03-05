package log

import (
	"io"
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/mstreet3/proglog/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// Boostraps a Log from the given directory
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// Collect the offset from the .store file names
	var baseOffsets []uint64
	for _, f := range files {
		ext := path.Ext(f.Name())
		if ext != ".index" {
			offStr := strings.TrimSuffix(f.Name(), ext)
			off, _ := strconv.ParseUint(offStr, 10, 0)
			baseOffsets = append(baseOffsets, off)
		}
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// Create a new segment for each offset
	for i, _ := range baseOffsets {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}

	// Create at least one new segment if the directory is empty
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	return 0, nil
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	return &api.Record{}, nil
}

func (l *Log) Close() error {
	return nil
}

func (l *Log) Remove() error {
	return nil
}

func (l *Log) Reset() error {
	return nil
}

func (l *Log) LowestOffset() (uint64, error) {
	return 0, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	return 0, nil
}

func (l *Log) Reader() io.Reader {
	return io.MultiReader()
}

func (l *Log) Truncate(lowest uint64) error {
	return nil
}

func (l *Log) newSegment(off uint64) error {
	return nil
}
