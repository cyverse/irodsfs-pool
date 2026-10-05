package client

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func newRoutingTestAccount(user string) *irodsclient_types.IRODSAccount {
	return &irodsclient_types.IRODSAccount{
		Host:       "data.example.org",
		Port:       1247,
		ClientUser: user,
		ClientZone: "zone",
		ProxyUser:  user,
		ProxyZone:  "zone",
	}
}

func TestMakeRoutingKeyNamesTheUser(t *testing.T) {
	alice := newRoutingTestAccount("alice")
	key := MakeRoutingKey(alice)
	if len(key) != 32 {
		t.Fatalf("routing key %q is %d characters long, want 32", key, len(key))
	}

	// the same user logging in with another resource or proxy user keeps the key
	other := newRoutingTestAccount("alice")
	other.DefaultResource = "otherResc"
	other.ProxyUser = "rods"
	if MakeRoutingKey(other) != key {
		t.Fatal("routing key changed with the default resource or proxy user")
	}

	if MakeRoutingKey(newRoutingTestAccount("bob")) == key {
		t.Fatal("two users share a routing key")
	}

	ticketA := newRoutingTestAccount("anonymous")
	ticketA.Ticket = "ticketA"
	ticketB := newRoutingTestAccount("anonymous")
	ticketB.Ticket = "ticketB"
	if MakeRoutingKey(ticketA) == MakeRoutingKey(ticketB) {
		t.Fatal("ticket logins with different tickets share a routing key")
	}

	if MakeRoutingKey(nil) != "" {
		t.Fatal("nil account made a routing key")
	}
}

// metadataRecordingServer records the metadata of every call it serves
type metadataRecordingServer struct {
	api.UnimplementedPoolAPIServer

	mutex   sync.Mutex
	calls   []string
	headers map[string][]string // method -> routing keys
	logins  int
	logouts int
}

func (server *metadataRecordingServer) record(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)

	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.calls = append(server.calls, method)
	server.headers[method] = append(server.headers[method], md.Get(commons.RoutingKeyMetadataKey)...)
}

func (server *metadataRecordingServer) unaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	server.record(ctx, info.FullMethod)
	return handler(ctx, req)
}

func (server *metadataRecordingServer) Login(_ context.Context, _ *api.LoginRequest) (*api.LoginResponse, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.logins++
	return &api.LoginResponse{SessionId: "session"}, nil
}

func (server *metadataRecordingServer) Logout(_ context.Context, _ *api.LogoutRequest) (*api.Empty, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.logouts++
	return &api.Empty{}, nil
}

func (server *metadataRecordingServer) List(_ context.Context, _ *api.ListRequest) (*api.ListResponse, error) {
	return &api.ListResponse{}, nil
}

func startMetadataRecordingServer(t *testing.T) (*metadataRecordingServer, string) {
	t.Helper()

	socketPath := filepath.Join(t.TempDir(), "pool.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to listen on %q: %v", socketPath, err)
	}

	recorder := &metadataRecordingServer{headers: map[string][]string{}}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(recorder.unaryInterceptor))
	api.RegisterPoolAPIServer(grpcServer, recorder)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)

	return recorder, "unix://" + socketPath
}

func TestPoolServiceClientSendsRoutingKeyWithEveryCall(t *testing.T) {
	recorder, endpoint := startMetadataRecordingServer(t)

	account := newRoutingTestAccount("alice")
	poolClient := NewPoolServiceClient(endpoint, 5*time.Second, false, "", account, "test", "", nil)
	session, err := poolClient.Connect()
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if session == nil {
		t.Fatal("Connect did not log in")
	}
	if _, err := session.List("/zone/home/alice"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if err := poolClient.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()

	if len(recorder.calls) != 3 {
		t.Fatalf("server saw calls %v, want Login, List, and Logout", recorder.calls)
	}
	want := MakeRoutingKey(account)
	for _, method := range recorder.calls {
		keys := recorder.headers[method]
		if len(keys) != 1 || keys[0] != want {
			t.Errorf("%s carried routing keys %v, want [%s]", method, keys, want)
		}
	}
}

func TestPoolServiceClientHoldsOneSession(t *testing.T) {
	recorder, endpoint := startMetadataRecordingServer(t)

	poolClient := NewPoolServiceClient(endpoint, 5*time.Second, false, "", newRoutingTestAccount("alice"), "test", "", nil)
	session, err := poolClient.Connect()
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	again, err := poolClient.Connect()
	if err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	if again != session {
		t.Fatal("second Connect returned another session")
	}

	if err := session.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if poolClient.GetSession() != nil {
		t.Fatal("Release left the session in place")
	}
	// releasing or disconnecting again does nothing
	if err := poolClient.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
	if err := poolClient.Disconnect(); err != nil {
		t.Fatalf("Disconnect after Release: %v", err)
	}

	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.logins != 1 || recorder.logouts != 1 {
		t.Fatalf("server saw %d logins and %d logouts, want 1 each", recorder.logins, recorder.logouts)
	}
}

func TestPoolServiceClientConnectRequiresAccount(t *testing.T) {
	poolClient := NewPoolServiceClient("tcp://test.invalid:12020", time.Second, false, "", nil, "", "", nil)
	if _, err := poolClient.Connect(); err == nil {
		t.Fatal("Connect without an account did not fail")
	}
}

func TestPoolServiceClientReleaseDisconnectsOnlyWhenConnected(t *testing.T) {
	recorder, endpoint := startMetadataRecordingServer(t)

	// Release on a connected client disconnects it
	connected := NewPoolServiceClient(endpoint, 5*time.Second, false, "", newRoutingTestAccount("alice"), "test", "", nil)
	if _, err := connected.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := connected.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if connected.isConnected() || connected.GetSession() != nil {
		t.Fatal("Release left the client connected")
	}

	// Release after Disconnect does not disconnect again
	disconnected := NewPoolServiceClient(endpoint, 5*time.Second, false, "", newRoutingTestAccount("bob"), "test", "", nil)
	if _, err := disconnected.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := disconnected.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := disconnected.Release(); err != nil {
		t.Fatalf("Release after Disconnect: %v", err)
	}

	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.logins != 2 || recorder.logouts != 2 {
		t.Fatalf("server saw %d logins and %d logouts, want 2 each", recorder.logins, recorder.logouts)
	}
}

func TestPoolServiceClientConnectAndDisconnectRepeatedly(t *testing.T) {
	recorder, endpoint := startMetadataRecordingServer(t)

	poolClient := NewPoolServiceClient(endpoint, 5*time.Second, false, "", newRoutingTestAccount("alice"), "test", "", nil)

	// concurrent Connects log in once and share the session
	sessions := make(chan irodsfs_common_irods.IRODSFSClient, 8)
	var wg sync.WaitGroup
	for i := 0; i < cap(sessions); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, err := poolClient.Connect()
			if err != nil {
				t.Errorf("Connect: %v", err)
			}
			sessions <- session
		}()
	}
	wg.Wait()
	close(sessions)

	first := poolClient.GetSession()
	for session := range sessions {
		if session != first {
			t.Fatal("concurrent Connects returned different sessions")
		}
	}

	// concurrent Disconnects log out once
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := poolClient.Disconnect(); err != nil {
				t.Errorf("Disconnect: %v", err)
			}
		}()
	}
	wg.Wait()

	// the client can connect again after a disconnect
	session, err := poolClient.Connect()
	if err != nil {
		t.Fatalf("Connect after Disconnect: %v", err)
	}
	if session == nil || session == first {
		t.Fatal("Connect after Disconnect did not log in again")
	}
	if err := poolClient.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := poolClient.Disconnect(); err != nil {
		t.Fatalf("second Disconnect: %v", err)
	}

	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.logins != 2 || recorder.logouts != 2 {
		t.Fatalf("server saw %d logins and %d logouts, want 2 each", recorder.logins, recorder.logouts)
	}
}
