package testingutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSTestHelper provides a real NATS connection for integration tests.
// It requires a running NATS server (either via testcontainers or external).
type NATSTestHelper struct {
	URL        string
	NC         *nats.Conn
	JS         jetstream.JetStream
	BucketName string
	tb         testing.TB
}

// NATSTestHelperConfig contains configuration for the NATS test helper.
type NATSTestHelperConfig struct {
	// URL is the NATS server URL. If empty, defaults to LITESTREAM_NATS_URL env var or nats://localhost:4222.
	URL string
	// BucketName is the Object Store bucket name. If empty, a unique name is generated.
	BucketName string
	// BucketReplicas is the number of replicas for the bucket (default: 1).
	BucketReplicas int
}

// NewNATSTestHelper creates a new NATS test helper with a unique bucket.
// It connects to the NATS server and creates an Object Store bucket for testing.
// The bucket is automatically cleaned up when the test completes.
func NewNATSTestHelper(tb testing.TB, cfg NATSTestHelperConfig) *NATSTestHelper {
	tb.Helper()

	url := cfg.URL
	if url == "" {
		url = *natsURL
		if url == "" {
			url = nats.DefaultURL
		}
	}

	bucketName := cfg.BucketName
	if bucketName == "" {
		bucketName = fmt.Sprintf("test-%d", time.Now().UnixNano())
	}

	replicas := cfg.BucketReplicas
	if replicas <= 0 {
		replicas = 1
	}

	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(100*time.Millisecond),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		tb.Skipf("NATS connection failed (is NATS running?): %v", err)
		return nil
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		tb.Fatalf("Failed to create JetStream context: %v", err)
	}

	ctx := context.Background()

	// Create Object Store bucket
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:   bucketName,
		Replicas: replicas,
	})
	if err != nil {
		nc.Close()
		tb.Fatalf("Failed to create Object Store bucket: %v", err)
	}

	helper := &NATSTestHelper{
		URL:        url,
		NC:         nc,
		JS:         js,
		BucketName: bucketName,
		tb:         tb,
	}

	// Register cleanup
	tb.Cleanup(func() {
		helper.Cleanup()
	})

	return helper
}

// Cleanup deletes the test bucket and closes the connection.
func (h *NATSTestHelper) Cleanup() {
	if h.JS != nil && h.BucketName != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.JS.DeleteObjectStore(ctx, h.BucketName)
	}
	if h.NC != nil {
		h.NC.Close()
	}
}

// ObjectStore returns the JetStream ObjectStore for the test bucket.
func (h *NATSTestHelper) ObjectStore(ctx context.Context) (jetstream.ObjectStore, error) {
	return h.JS.ObjectStore(ctx, h.BucketName)
}

// CreateKVBucket creates a KV bucket for testing leader election.
func (h *NATSTestHelper) CreateKVBucket(ctx context.Context, name string, ttl time.Duration) (jetstream.KeyValue, error) {
	kv, err := h.JS.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: name,
		TTL:    ttl,
	})
	if err != nil {
		return nil, err
	}
	// Register cleanup
	h.tb.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.JS.DeleteKeyValue(cleanupCtx, name)
	})
	return kv, nil
}
