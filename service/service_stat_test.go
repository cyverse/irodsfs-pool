package service

import (
	"context"
	"testing"

	"github.com/cyverse/irodsfs-pool/commons"
	"google.golang.org/grpc/metadata"
)

func TestClientIDComesFromTheRequestMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(commons.ClientIDMetadataKey, "client-a"))

	ctx = withClientIDFromMetadata(ctx)

	if clientID := ClientIDFromContext(ctx); clientID != "client-a" {
		t.Fatalf("client id = %q, want %q", clientID, "client-a")
	}
}

// The caller id has to outlive a reconnect, so the client's own id wins over
// the connection id, which is made anew for every connection.
func TestCallerIDPrefersTheClientID(t *testing.T) {
	ctx := context.WithValue(context.Background(), connIDKey, "conn-1")
	ctx = context.WithValue(ctx, clientIDKey, "client-a")

	if callerID := CallerIDFromContext(ctx); callerID != "client-a" {
		t.Fatalf("caller id = %q, want the client id", callerID)
	}
}

// An older client sends no id, and then the connection is the only thing that
// tells it apart from the others sharing its session
func TestCallerIDFallsBackToTheConnectionID(t *testing.T) {
	ctx := context.WithValue(context.Background(), connIDKey, "conn-1")

	if callerID := CallerIDFromContext(ctx); callerID != "conn-1" {
		t.Fatalf("caller id = %q, want the connection id", callerID)
	}

	if callerID := CallerIDFromContext(context.Background()); callerID != "" {
		t.Fatalf("caller id = %q, want an empty id", callerID)
	}
}

func TestClientIDMetadataIsIgnoredWhenAbsentOrEmpty(t *testing.T) {
	if clientID := ClientIDFromContext(withClientIDFromMetadata(context.Background())); clientID != "" {
		t.Fatalf("client id = %q, want an empty id", clientID)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(commons.ClientIDMetadataKey, ""))
	if clientID := ClientIDFromContext(withClientIDFromMetadata(ctx)); clientID != "" {
		t.Fatalf("client id = %q, want an empty id", clientID)
	}
}
