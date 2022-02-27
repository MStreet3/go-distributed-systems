package log

import (
	api "github.com/mstreet3/proglog/api/v1"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	return &segment{}, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	return 0, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	return nil, nil
}

func (s *segment) Remove() error {
	return nil
}

func (s *segment) IsMaxed() bool {
	return false
}

func (s *segment) Close() error {
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
