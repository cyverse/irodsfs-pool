// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package api

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ProxyAPIClient is the client API for ProxyAPI service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ProxyAPIClient interface {
	Login(ctx context.Context, in *LoginRequest, opts ...grpc.CallOption) (*LoginResponse, error)
	Logout(ctx context.Context, in *LogoutRequest, opts ...grpc.CallOption) (*Empty, error)
	List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (*ListResponse, error)
	Stat(ctx context.Context, in *StatRequest, opts ...grpc.CallOption) (*StatResponse, error)
	ExistsDir(ctx context.Context, in *ExistsDirRequest, opts ...grpc.CallOption) (*ExistsDirResponse, error)
	ExistsFile(ctx context.Context, in *ExistsFileRequest, opts ...grpc.CallOption) (*ExistsFileResponse, error)
	ListDirACLsWithGroupUsers(ctx context.Context, in *ListDirACLsWithGroupUsersRequest, opts ...grpc.CallOption) (*ListDirACLsWithGroupUsersResponse, error)
	ListFileACLsWithGroupUsers(ctx context.Context, in *ListFileACLsWithGroupUsersRequest, opts ...grpc.CallOption) (*ListFileACLsWithGroupUsersResponse, error)
	RemoveFile(ctx context.Context, in *RemoveFileRequest, opts ...grpc.CallOption) (*Empty, error)
	RemoveDir(ctx context.Context, in *RemoveDirRequest, opts ...grpc.CallOption) (*Empty, error)
	MakeDir(ctx context.Context, in *MakeDirRequest, opts ...grpc.CallOption) (*Empty, error)
	RenameDirToDir(ctx context.Context, in *RenameDirToDirRequest, opts ...grpc.CallOption) (*Empty, error)
	RenameFileToFile(ctx context.Context, in *RenameFileToFileRequest, opts ...grpc.CallOption) (*Empty, error)
	CreateFile(ctx context.Context, in *CreateFileRequest, opts ...grpc.CallOption) (*CreateFileResponse, error)
	OpenFile(ctx context.Context, in *OpenFileRequest, opts ...grpc.CallOption) (*OpenFileResponse, error)
	TruncateFile(ctx context.Context, in *TruncateFileRequest, opts ...grpc.CallOption) (*Empty, error)
	// file
	GetOffset(ctx context.Context, in *GetOffsetRequest, opts ...grpc.CallOption) (*GetOffsetResponse, error)
	ReadAt(ctx context.Context, in *ReadAtRequest, opts ...grpc.CallOption) (*ReadAtResponse, error)
	WriteAt(ctx context.Context, in *WriteAtRequest, opts ...grpc.CallOption) (*Empty, error)
	Close(ctx context.Context, in *CloseRequest, opts ...grpc.CallOption) (*Empty, error)
}

type proxyAPIClient struct {
	cc grpc.ClientConnInterface
}

func NewProxyAPIClient(cc grpc.ClientConnInterface) ProxyAPIClient {
	return &proxyAPIClient{cc}
}

func (c *proxyAPIClient) Login(ctx context.Context, in *LoginRequest, opts ...grpc.CallOption) (*LoginResponse, error) {
	out := new(LoginResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/Login", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) Logout(ctx context.Context, in *LogoutRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/Logout", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (*ListResponse, error) {
	out := new(ListResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/List", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) Stat(ctx context.Context, in *StatRequest, opts ...grpc.CallOption) (*StatResponse, error) {
	out := new(StatResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/Stat", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) ExistsDir(ctx context.Context, in *ExistsDirRequest, opts ...grpc.CallOption) (*ExistsDirResponse, error) {
	out := new(ExistsDirResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/ExistsDir", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) ExistsFile(ctx context.Context, in *ExistsFileRequest, opts ...grpc.CallOption) (*ExistsFileResponse, error) {
	out := new(ExistsFileResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/ExistsFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) ListDirACLsWithGroupUsers(ctx context.Context, in *ListDirACLsWithGroupUsersRequest, opts ...grpc.CallOption) (*ListDirACLsWithGroupUsersResponse, error) {
	out := new(ListDirACLsWithGroupUsersResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/ListDirACLsWithGroupUsers", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) ListFileACLsWithGroupUsers(ctx context.Context, in *ListFileACLsWithGroupUsersRequest, opts ...grpc.CallOption) (*ListFileACLsWithGroupUsersResponse, error) {
	out := new(ListFileACLsWithGroupUsersResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/ListFileACLsWithGroupUsers", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) RemoveFile(ctx context.Context, in *RemoveFileRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/RemoveFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) RemoveDir(ctx context.Context, in *RemoveDirRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/RemoveDir", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) MakeDir(ctx context.Context, in *MakeDirRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/MakeDir", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) RenameDirToDir(ctx context.Context, in *RenameDirToDirRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/RenameDirToDir", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) RenameFileToFile(ctx context.Context, in *RenameFileToFileRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/RenameFileToFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) CreateFile(ctx context.Context, in *CreateFileRequest, opts ...grpc.CallOption) (*CreateFileResponse, error) {
	out := new(CreateFileResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/CreateFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) OpenFile(ctx context.Context, in *OpenFileRequest, opts ...grpc.CallOption) (*OpenFileResponse, error) {
	out := new(OpenFileResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/OpenFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) TruncateFile(ctx context.Context, in *TruncateFileRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/TruncateFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) GetOffset(ctx context.Context, in *GetOffsetRequest, opts ...grpc.CallOption) (*GetOffsetResponse, error) {
	out := new(GetOffsetResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/GetOffset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) ReadAt(ctx context.Context, in *ReadAtRequest, opts ...grpc.CallOption) (*ReadAtResponse, error) {
	out := new(ReadAtResponse)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/ReadAt", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) WriteAt(ctx context.Context, in *WriteAtRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/WriteAt", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *proxyAPIClient) Close(ctx context.Context, in *CloseRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/api.ProxyAPI/Close", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ProxyAPIServer is the server API for ProxyAPI service.
// All implementations must embed UnimplementedProxyAPIServer
// for forward compatibility
type ProxyAPIServer interface {
	Login(context.Context, *LoginRequest) (*LoginResponse, error)
	Logout(context.Context, *LogoutRequest) (*Empty, error)
	List(context.Context, *ListRequest) (*ListResponse, error)
	Stat(context.Context, *StatRequest) (*StatResponse, error)
	ExistsDir(context.Context, *ExistsDirRequest) (*ExistsDirResponse, error)
	ExistsFile(context.Context, *ExistsFileRequest) (*ExistsFileResponse, error)
	ListDirACLsWithGroupUsers(context.Context, *ListDirACLsWithGroupUsersRequest) (*ListDirACLsWithGroupUsersResponse, error)
	ListFileACLsWithGroupUsers(context.Context, *ListFileACLsWithGroupUsersRequest) (*ListFileACLsWithGroupUsersResponse, error)
	RemoveFile(context.Context, *RemoveFileRequest) (*Empty, error)
	RemoveDir(context.Context, *RemoveDirRequest) (*Empty, error)
	MakeDir(context.Context, *MakeDirRequest) (*Empty, error)
	RenameDirToDir(context.Context, *RenameDirToDirRequest) (*Empty, error)
	RenameFileToFile(context.Context, *RenameFileToFileRequest) (*Empty, error)
	CreateFile(context.Context, *CreateFileRequest) (*CreateFileResponse, error)
	OpenFile(context.Context, *OpenFileRequest) (*OpenFileResponse, error)
	TruncateFile(context.Context, *TruncateFileRequest) (*Empty, error)
	// file
	GetOffset(context.Context, *GetOffsetRequest) (*GetOffsetResponse, error)
	ReadAt(context.Context, *ReadAtRequest) (*ReadAtResponse, error)
	WriteAt(context.Context, *WriteAtRequest) (*Empty, error)
	Close(context.Context, *CloseRequest) (*Empty, error)
	mustEmbedUnimplementedProxyAPIServer()
}

// UnimplementedProxyAPIServer must be embedded to have forward compatible implementations.
type UnimplementedProxyAPIServer struct {
}

func (UnimplementedProxyAPIServer) Login(context.Context, *LoginRequest) (*LoginResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Login not implemented")
}
func (UnimplementedProxyAPIServer) Logout(context.Context, *LogoutRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Logout not implemented")
}
func (UnimplementedProxyAPIServer) List(context.Context, *ListRequest) (*ListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method List not implemented")
}
func (UnimplementedProxyAPIServer) Stat(context.Context, *StatRequest) (*StatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Stat not implemented")
}
func (UnimplementedProxyAPIServer) ExistsDir(context.Context, *ExistsDirRequest) (*ExistsDirResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ExistsDir not implemented")
}
func (UnimplementedProxyAPIServer) ExistsFile(context.Context, *ExistsFileRequest) (*ExistsFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ExistsFile not implemented")
}
func (UnimplementedProxyAPIServer) ListDirACLsWithGroupUsers(context.Context, *ListDirACLsWithGroupUsersRequest) (*ListDirACLsWithGroupUsersResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListDirACLsWithGroupUsers not implemented")
}
func (UnimplementedProxyAPIServer) ListFileACLsWithGroupUsers(context.Context, *ListFileACLsWithGroupUsersRequest) (*ListFileACLsWithGroupUsersResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListFileACLsWithGroupUsers not implemented")
}
func (UnimplementedProxyAPIServer) RemoveFile(context.Context, *RemoveFileRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveFile not implemented")
}
func (UnimplementedProxyAPIServer) RemoveDir(context.Context, *RemoveDirRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveDir not implemented")
}
func (UnimplementedProxyAPIServer) MakeDir(context.Context, *MakeDirRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MakeDir not implemented")
}
func (UnimplementedProxyAPIServer) RenameDirToDir(context.Context, *RenameDirToDirRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RenameDirToDir not implemented")
}
func (UnimplementedProxyAPIServer) RenameFileToFile(context.Context, *RenameFileToFileRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RenameFileToFile not implemented")
}
func (UnimplementedProxyAPIServer) CreateFile(context.Context, *CreateFileRequest) (*CreateFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateFile not implemented")
}
func (UnimplementedProxyAPIServer) OpenFile(context.Context, *OpenFileRequest) (*OpenFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method OpenFile not implemented")
}
func (UnimplementedProxyAPIServer) TruncateFile(context.Context, *TruncateFileRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TruncateFile not implemented")
}
func (UnimplementedProxyAPIServer) GetOffset(context.Context, *GetOffsetRequest) (*GetOffsetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOffset not implemented")
}
func (UnimplementedProxyAPIServer) ReadAt(context.Context, *ReadAtRequest) (*ReadAtResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadAt not implemented")
}
func (UnimplementedProxyAPIServer) WriteAt(context.Context, *WriteAtRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteAt not implemented")
}
func (UnimplementedProxyAPIServer) Close(context.Context, *CloseRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Close not implemented")
}
func (UnimplementedProxyAPIServer) mustEmbedUnimplementedProxyAPIServer() {}

// UnsafeProxyAPIServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ProxyAPIServer will
// result in compilation errors.
type UnsafeProxyAPIServer interface {
	mustEmbedUnimplementedProxyAPIServer()
}

func RegisterProxyAPIServer(s grpc.ServiceRegistrar, srv ProxyAPIServer) {
	s.RegisterService(&ProxyAPI_ServiceDesc, srv)
}

func _ProxyAPI_Login_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LoginRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).Login(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/Login",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).Login(ctx, req.(*LoginRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_Logout_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LogoutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).Logout(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/Logout",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).Logout(ctx, req.(*LogoutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_List_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).List(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/List",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).List(ctx, req.(*ListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_Stat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).Stat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/Stat",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).Stat(ctx, req.(*StatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_ExistsDir_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExistsDirRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).ExistsDir(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/ExistsDir",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).ExistsDir(ctx, req.(*ExistsDirRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_ExistsFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExistsFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).ExistsFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/ExistsFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).ExistsFile(ctx, req.(*ExistsFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_ListDirACLsWithGroupUsers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListDirACLsWithGroupUsersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).ListDirACLsWithGroupUsers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/ListDirACLsWithGroupUsers",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).ListDirACLsWithGroupUsers(ctx, req.(*ListDirACLsWithGroupUsersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_ListFileACLsWithGroupUsers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListFileACLsWithGroupUsersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).ListFileACLsWithGroupUsers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/ListFileACLsWithGroupUsers",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).ListFileACLsWithGroupUsers(ctx, req.(*ListFileACLsWithGroupUsersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_RemoveFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).RemoveFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/RemoveFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).RemoveFile(ctx, req.(*RemoveFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_RemoveDir_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveDirRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).RemoveDir(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/RemoveDir",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).RemoveDir(ctx, req.(*RemoveDirRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_MakeDir_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MakeDirRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).MakeDir(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/MakeDir",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).MakeDir(ctx, req.(*MakeDirRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_RenameDirToDir_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RenameDirToDirRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).RenameDirToDir(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/RenameDirToDir",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).RenameDirToDir(ctx, req.(*RenameDirToDirRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_RenameFileToFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RenameFileToFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).RenameFileToFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/RenameFileToFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).RenameFileToFile(ctx, req.(*RenameFileToFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_CreateFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).CreateFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/CreateFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).CreateFile(ctx, req.(*CreateFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_OpenFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OpenFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).OpenFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/OpenFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).OpenFile(ctx, req.(*OpenFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_TruncateFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TruncateFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).TruncateFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/TruncateFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).TruncateFile(ctx, req.(*TruncateFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_GetOffset_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOffsetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).GetOffset(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/GetOffset",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).GetOffset(ctx, req.(*GetOffsetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_ReadAt_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadAtRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).ReadAt(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/ReadAt",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).ReadAt(ctx, req.(*ReadAtRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_WriteAt_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteAtRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).WriteAt(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/WriteAt",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).WriteAt(ctx, req.(*WriteAtRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ProxyAPI_Close_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CloseRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ProxyAPIServer).Close(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ProxyAPI/Close",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ProxyAPIServer).Close(ctx, req.(*CloseRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ProxyAPI_ServiceDesc is the grpc.ServiceDesc for ProxyAPI service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ProxyAPI_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.ProxyAPI",
	HandlerType: (*ProxyAPIServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Login",
			Handler:    _ProxyAPI_Login_Handler,
		},
		{
			MethodName: "Logout",
			Handler:    _ProxyAPI_Logout_Handler,
		},
		{
			MethodName: "List",
			Handler:    _ProxyAPI_List_Handler,
		},
		{
			MethodName: "Stat",
			Handler:    _ProxyAPI_Stat_Handler,
		},
		{
			MethodName: "ExistsDir",
			Handler:    _ProxyAPI_ExistsDir_Handler,
		},
		{
			MethodName: "ExistsFile",
			Handler:    _ProxyAPI_ExistsFile_Handler,
		},
		{
			MethodName: "ListDirACLsWithGroupUsers",
			Handler:    _ProxyAPI_ListDirACLsWithGroupUsers_Handler,
		},
		{
			MethodName: "ListFileACLsWithGroupUsers",
			Handler:    _ProxyAPI_ListFileACLsWithGroupUsers_Handler,
		},
		{
			MethodName: "RemoveFile",
			Handler:    _ProxyAPI_RemoveFile_Handler,
		},
		{
			MethodName: "RemoveDir",
			Handler:    _ProxyAPI_RemoveDir_Handler,
		},
		{
			MethodName: "MakeDir",
			Handler:    _ProxyAPI_MakeDir_Handler,
		},
		{
			MethodName: "RenameDirToDir",
			Handler:    _ProxyAPI_RenameDirToDir_Handler,
		},
		{
			MethodName: "RenameFileToFile",
			Handler:    _ProxyAPI_RenameFileToFile_Handler,
		},
		{
			MethodName: "CreateFile",
			Handler:    _ProxyAPI_CreateFile_Handler,
		},
		{
			MethodName: "OpenFile",
			Handler:    _ProxyAPI_OpenFile_Handler,
		},
		{
			MethodName: "TruncateFile",
			Handler:    _ProxyAPI_TruncateFile_Handler,
		},
		{
			MethodName: "GetOffset",
			Handler:    _ProxyAPI_GetOffset_Handler,
		},
		{
			MethodName: "ReadAt",
			Handler:    _ProxyAPI_ReadAt_Handler,
		},
		{
			MethodName: "WriteAt",
			Handler:    _ProxyAPI_WriteAt_Handler,
		},
		{
			MethodName: "Close",
			Handler:    _ProxyAPI_Close_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "service/api/api.proto",
}
