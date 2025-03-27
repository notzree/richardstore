package hdfs

import (
	"context"
	"fmt"

	"github.com/notzree/richardstore/proto"
	"google.golang.org/grpc"
)

type FilestreamReplicator struct {
	stream grpc.ClientStreamingClient[proto.FileStream, proto.WriteFileResponse]
	ctx    context.Context
	cancel context.CancelFunc
}

func NewReplicator(ctx context.Context, addr string, info *proto.StreamInfo) (*FilestreamReplicator, error) {
	ctx, cancel := context.WithCancel(ctx)

	client, err := NewDataNodeClient(addr)
	if err != nil {
		cancel()
		return nil, err
	}

	stream, err := client.client.WriteFile(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	// Send initial stream info
	err = stream.Send(&proto.FileStream{
		Type: &proto.FileStream_StreamInfo{
			StreamInfo: info,
		},
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("err sending info: %v", err)
	}

	return &FilestreamReplicator{
		stream: stream,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (r *FilestreamReplicator) SendChunk(chunk *proto.FileStream) error {
	return r.stream.Send(chunk)
}

func (r *FilestreamReplicator) CloseAndRecv() (*proto.WriteFileResponse, error) {
	defer r.cancel()
	return r.stream.CloseAndRecv()
}
