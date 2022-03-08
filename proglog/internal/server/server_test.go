package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	api "github.com/mstreet3/proglog/api/v1"
	"github.com/mstreet3/proglog/internal/log"
)

type grpcTestHelper func(
	t *testing.T,
	client api.LogClient,
	repo *LogRepository,
)

func TestServer(t *testing.T) {
	scenarios := map[string]grpcTestHelper{
		"produce/consume a message to/from the log succeeds": testProduceConsume,
	}
	for scenario, fn := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			client, repo, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, repo)
		})
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, repo *LogRepository) {
	ctx := context.Background()
	expected := api.Record{
		Value: []byte("hello world"),
	}
	// Write the expected value
	req := api.ProduceRequest{
		Record: &expected,
	}
	res, err := client.Produce(ctx, &req)
	require.NoError(t, err)
	require.Equal(t, res.Offset, uint64(0))

	// Read the expected value
	creq := api.ConsumeRequest{
		Offset: res.Offset,
	}
	cres, err := client.Consume(ctx, &creq)
	require.NoError(t, err)
	require.Equal(t, cres.Record.Value, expected.Value)
}

func setupTest(t *testing.T, fn func(*LogRepository)) (
	client api.LogClient,
	repo *LogRepository,
	teardown func(),
) {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	repo = &LogRepository{
		CommitLog: clog,
	}
	if fn != nil {
		fn(repo)
	}
	server, err := NewGRPCServer(repo)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)
	return client, repo, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}
