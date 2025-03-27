// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: proto/schema.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	DataNode_WriteFile_FullMethodName = "/raft.DataNode/WriteFile"
	DataNode_ReadFile_FullMethodName  = "/raft.DataNode/ReadFile"
)

// DataNodeClient is the client API for DataNode service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DataNodeClient interface {
	WriteFile(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[FileStream, WriteFileResponse], error)
	ReadFile(ctx context.Context, in *ReadFileRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[FileStream], error)
}

type dataNodeClient struct {
	cc grpc.ClientConnInterface
}

func NewDataNodeClient(cc grpc.ClientConnInterface) DataNodeClient {
	return &dataNodeClient{cc}
}

func (c *dataNodeClient) WriteFile(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[FileStream, WriteFileResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DataNode_ServiceDesc.Streams[0], DataNode_WriteFile_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[FileStream, WriteFileResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataNode_WriteFileClient = grpc.ClientStreamingClient[FileStream, WriteFileResponse]

func (c *dataNodeClient) ReadFile(ctx context.Context, in *ReadFileRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[FileStream], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &DataNode_ServiceDesc.Streams[1], DataNode_ReadFile_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[ReadFileRequest, FileStream]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataNode_ReadFileClient = grpc.ServerStreamingClient[FileStream]

// DataNodeServer is the server API for DataNode service.
// All implementations must embed UnimplementedDataNodeServer
// for forward compatibility.
type DataNodeServer interface {
	WriteFile(grpc.ClientStreamingServer[FileStream, WriteFileResponse]) error
	ReadFile(*ReadFileRequest, grpc.ServerStreamingServer[FileStream]) error
	mustEmbedUnimplementedDataNodeServer()
}

// UnimplementedDataNodeServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedDataNodeServer struct{}

func (UnimplementedDataNodeServer) WriteFile(grpc.ClientStreamingServer[FileStream, WriteFileResponse]) error {
	return status.Errorf(codes.Unimplemented, "method WriteFile not implemented")
}
func (UnimplementedDataNodeServer) ReadFile(*ReadFileRequest, grpc.ServerStreamingServer[FileStream]) error {
	return status.Errorf(codes.Unimplemented, "method ReadFile not implemented")
}
func (UnimplementedDataNodeServer) mustEmbedUnimplementedDataNodeServer() {}
func (UnimplementedDataNodeServer) testEmbeddedByValue()                  {}

// UnsafeDataNodeServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DataNodeServer will
// result in compilation errors.
type UnsafeDataNodeServer interface {
	mustEmbedUnimplementedDataNodeServer()
}

func RegisterDataNodeServer(s grpc.ServiceRegistrar, srv DataNodeServer) {
	// If the following call pancis, it indicates UnimplementedDataNodeServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&DataNode_ServiceDesc, srv)
}

func _DataNode_WriteFile_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DataNodeServer).WriteFile(&grpc.GenericServerStream[FileStream, WriteFileResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataNode_WriteFileServer = grpc.ClientStreamingServer[FileStream, WriteFileResponse]

func _DataNode_ReadFile_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReadFileRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DataNodeServer).ReadFile(m, &grpc.GenericServerStream[ReadFileRequest, FileStream]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type DataNode_ReadFileServer = grpc.ServerStreamingServer[FileStream]

// DataNode_ServiceDesc is the grpc.ServiceDesc for DataNode service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DataNode_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "raft.DataNode",
	HandlerType: (*DataNodeServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "WriteFile",
			Handler:       _DataNode_WriteFile_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "ReadFile",
			Handler:       _DataNode_ReadFile_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "proto/schema.proto",
}

const (
	NameNode_WriteFile_FullMethodName              = "/raft.NameNode/WriteFile"
	NameNode_ReadFile_FullMethodName               = "/raft.NameNode/ReadFile"
	NameNode_DeleteFile_FullMethodName             = "/raft.NameNode/DeleteFile"
	NameNode_Info_FullMethodName                   = "/raft.NameNode/Info"
	NameNode_Heartbeat_FullMethodName              = "/raft.NameNode/Heartbeat"
	NameNode_BlockReport_FullMethodName            = "/raft.NameNode/BlockReport"
	NameNode_IncrementalBlockReport_FullMethodName = "/raft.NameNode/IncrementalBlockReport"
)

// NameNodeClient is the client API for NameNode service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type NameNodeClient interface {
	WriteFile(ctx context.Context, in *WriteFileRequest, opts ...grpc.CallOption) (*CreateFileResponse, error)
	ReadFile(ctx context.Context, in *ReadFileRequest, opts ...grpc.CallOption) (*ReadFileResponse, error)
	DeleteFile(ctx context.Context, in *DeleteFileRequest, opts ...grpc.CallOption) (*DeleteFileResponse, error)
	Info(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*NameNodeInfo, error)
	// -- Consensus --
	// followers -> leader (lightweight)
	Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
	// followers -> leader (full inventory)
	BlockReport(ctx context.Context, in *BlockReportRequest, opts ...grpc.CallOption) (*BlockReportResponse, error)
	IncrementalBlockReport(ctx context.Context, in *IncrementalBlockReportRequest, opts ...grpc.CallOption) (*BlockReportResponse, error)
}

type nameNodeClient struct {
	cc grpc.ClientConnInterface
}

func NewNameNodeClient(cc grpc.ClientConnInterface) NameNodeClient {
	return &nameNodeClient{cc}
}

func (c *nameNodeClient) WriteFile(ctx context.Context, in *WriteFileRequest, opts ...grpc.CallOption) (*CreateFileResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreateFileResponse)
	err := c.cc.Invoke(ctx, NameNode_WriteFile_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nameNodeClient) ReadFile(ctx context.Context, in *ReadFileRequest, opts ...grpc.CallOption) (*ReadFileResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReadFileResponse)
	err := c.cc.Invoke(ctx, NameNode_ReadFile_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nameNodeClient) DeleteFile(ctx context.Context, in *DeleteFileRequest, opts ...grpc.CallOption) (*DeleteFileResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeleteFileResponse)
	err := c.cc.Invoke(ctx, NameNode_DeleteFile_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nameNodeClient) Info(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*NameNodeInfo, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(NameNodeInfo)
	err := c.cc.Invoke(ctx, NameNode_Info_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nameNodeClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HeartbeatResponse)
	err := c.cc.Invoke(ctx, NameNode_Heartbeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nameNodeClient) BlockReport(ctx context.Context, in *BlockReportRequest, opts ...grpc.CallOption) (*BlockReportResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(BlockReportResponse)
	err := c.cc.Invoke(ctx, NameNode_BlockReport_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nameNodeClient) IncrementalBlockReport(ctx context.Context, in *IncrementalBlockReportRequest, opts ...grpc.CallOption) (*BlockReportResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(BlockReportResponse)
	err := c.cc.Invoke(ctx, NameNode_IncrementalBlockReport_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// NameNodeServer is the server API for NameNode service.
// All implementations must embed UnimplementedNameNodeServer
// for forward compatibility.
type NameNodeServer interface {
	WriteFile(context.Context, *WriteFileRequest) (*CreateFileResponse, error)
	ReadFile(context.Context, *ReadFileRequest) (*ReadFileResponse, error)
	DeleteFile(context.Context, *DeleteFileRequest) (*DeleteFileResponse, error)
	Info(context.Context, *emptypb.Empty) (*NameNodeInfo, error)
	// -- Consensus --
	// followers -> leader (lightweight)
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	// followers -> leader (full inventory)
	BlockReport(context.Context, *BlockReportRequest) (*BlockReportResponse, error)
	IncrementalBlockReport(context.Context, *IncrementalBlockReportRequest) (*BlockReportResponse, error)
	mustEmbedUnimplementedNameNodeServer()
}

// UnimplementedNameNodeServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedNameNodeServer struct{}

func (UnimplementedNameNodeServer) WriteFile(context.Context, *WriteFileRequest) (*CreateFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteFile not implemented")
}
func (UnimplementedNameNodeServer) ReadFile(context.Context, *ReadFileRequest) (*ReadFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadFile not implemented")
}
func (UnimplementedNameNodeServer) DeleteFile(context.Context, *DeleteFileRequest) (*DeleteFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteFile not implemented")
}
func (UnimplementedNameNodeServer) Info(context.Context, *emptypb.Empty) (*NameNodeInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Info not implemented")
}
func (UnimplementedNameNodeServer) Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedNameNodeServer) BlockReport(context.Context, *BlockReportRequest) (*BlockReportResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BlockReport not implemented")
}
func (UnimplementedNameNodeServer) IncrementalBlockReport(context.Context, *IncrementalBlockReportRequest) (*BlockReportResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method IncrementalBlockReport not implemented")
}
func (UnimplementedNameNodeServer) mustEmbedUnimplementedNameNodeServer() {}
func (UnimplementedNameNodeServer) testEmbeddedByValue()                  {}

// UnsafeNameNodeServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to NameNodeServer will
// result in compilation errors.
type UnsafeNameNodeServer interface {
	mustEmbedUnimplementedNameNodeServer()
}

func RegisterNameNodeServer(s grpc.ServiceRegistrar, srv NameNodeServer) {
	// If the following call pancis, it indicates UnimplementedNameNodeServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&NameNode_ServiceDesc, srv)
}

func _NameNode_WriteFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NameNodeServer).WriteFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NameNode_WriteFile_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NameNodeServer).WriteFile(ctx, req.(*WriteFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NameNode_ReadFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NameNodeServer).ReadFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NameNode_ReadFile_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NameNodeServer).ReadFile(ctx, req.(*ReadFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NameNode_DeleteFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NameNodeServer).DeleteFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NameNode_DeleteFile_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NameNodeServer).DeleteFile(ctx, req.(*DeleteFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NameNode_Info_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NameNodeServer).Info(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NameNode_Info_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NameNodeServer).Info(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _NameNode_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NameNodeServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NameNode_Heartbeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NameNodeServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NameNode_BlockReport_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockReportRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NameNodeServer).BlockReport(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NameNode_BlockReport_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NameNodeServer).BlockReport(ctx, req.(*BlockReportRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NameNode_IncrementalBlockReport_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(IncrementalBlockReportRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NameNodeServer).IncrementalBlockReport(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: NameNode_IncrementalBlockReport_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NameNodeServer).IncrementalBlockReport(ctx, req.(*IncrementalBlockReportRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// NameNode_ServiceDesc is the grpc.ServiceDesc for NameNode service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var NameNode_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "raft.NameNode",
	HandlerType: (*NameNodeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "WriteFile",
			Handler:    _NameNode_WriteFile_Handler,
		},
		{
			MethodName: "ReadFile",
			Handler:    _NameNode_ReadFile_Handler,
		},
		{
			MethodName: "DeleteFile",
			Handler:    _NameNode_DeleteFile_Handler,
		},
		{
			MethodName: "Info",
			Handler:    _NameNode_Info_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _NameNode_Heartbeat_Handler,
		},
		{
			MethodName: "BlockReport",
			Handler:    _NameNode_BlockReport_Handler,
		},
		{
			MethodName: "IncrementalBlockReport",
			Handler:    _NameNode_IncrementalBlockReport_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/schema.proto",
}
