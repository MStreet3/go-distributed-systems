package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/mstreet3/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment_test")
	defer os.RemoveAll(dir)

	want := &api.Record{
		Value: []byte("hello world"),
	}

	c := Config{}
	c.Segment.MaxIndexBytes = entWidth * 3
	c.Segment.MaxStoreBytes = 1024

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		// Read
		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// Should not be able to write because index is maxed
	_, err = s.Append(want)
	require.Error(t, io.EOF, err)
	require.True(t, s.IsMaxed())

	// Should be maxed if store is at max size
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	// Should remove segment
	err = s.Remove()
	require.NoError(t, err)

	// New segment should not be maxed
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
