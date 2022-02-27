package log

import (
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
	return &Log{}, nil
}

func (l *Log) setup() error {
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

func (l *Log) Truncate(lowest uint64) error {
	return nil
}

func (l *Log) newSegment(off uint64) error {
	return nil
}
