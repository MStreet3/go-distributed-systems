package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TODO: split test into separate tests with a setup function
func TestIndex(t *testing.T) {
	// Get a temporary index file
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// Setup index instance
	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	// No entries in the index
	_, _, err = idx.Read(-1)
	require.Error(t, err)

	// Temp file is index file
	require.Equal(t, f.Name(), idx.Name())

	// Add entries
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		// Write error free
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		// Read position correctly
		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// Error when reading past number of entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// Should build index from existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entries[1].Off, off)
	require.Equal(t, entries[1].Pos, pos)
}
