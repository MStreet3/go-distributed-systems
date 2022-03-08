package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	api "github.com/mstreet3/proglog/api/v1"
)

type logTestFn func(*testing.T, *Log)

var _append = &api.Record{
	Value: []byte("hello world"),
}

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]logTestFn{
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"truncate":                          testTruncate,
		"reader":                            testReader,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	off, err := log.Append(_append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, _append.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr, ok := err.(api.ErrOffsetOutOfRange)
	require.True(t, ok)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	var (
		minOff = uint64(0)
		maxOff = uint64(2)
	)

	// Append to log and close it
	for i := 0; i < 3; i++ {
		_, err := log.Append(_append)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	// Validate lowest and highest offset
	validateOffsets(t, log, minOff, maxOff)

	// Create a new log and verify its lowest and highest offset
	n, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)
	validateOffsets(t, n, minOff, maxOff)

}

func validateOffsets(t *testing.T, log *Log, min, max uint64) {
	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, min, off)
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, max, off)
}

func testReader(t *testing.T, log *Log) {
	off, err := log.Append(_append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, _append.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	for i := 0; i < 3; i++ {
		_, err := log.Append(_append)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
