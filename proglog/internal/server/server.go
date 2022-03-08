package server

import (
	"context"

	api "github.com/mstreet3/proglog/api/v1"
	"google.golang.org/grpc"
)

var _ api.LogServer = (*grpcServer)(nil)

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(off uint64) (*api.Record, error)
}

type LogRepository struct {
	CommitLog CommitLog
}

type grpcServer struct {
	api.UnimplementedLogServer
	*LogRepository
}

func NewGRPCServer(c *LogRepository) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(c)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func newgrpcServer(c *LogRepository) (srv *grpcServer, err error) {
	srv = &grpcServer{
		LogRepository: c,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse,
	error,
) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse,
	error,
) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(
	req *api.ProduceRequest,
	stream api.Log_ProduceStreamServer,
) error {
	for {
		err := stream.RecvMsg(req)
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
