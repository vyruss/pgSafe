package cloudtest

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

// GCSEndpoint carries the connection details for the spun-up
// fake-gcs-server container. The production GCS driver uses
// cloud.google.com/go/storage; pointing it at this URL via
// option.WithEndpoint + option.WithoutAuthentication makes it talk to
// the emulator transparently.
type GCSEndpoint struct {
	URL    string // http://host:port  (root; the SDK adds /storage/v1/...)
	Bucket string // pre-created
}

// StartGCS launches a fake-gcs-server container in HTTP mode and pre-creates
// a bucket. The Go GCS SDK works against fake-gcs-server when given:
//   - STORAGE_EMULATOR_HOST=host:port (set via t.Setenv per test), and
//   - the matching -public-host flag on the server so its returned URLs
//     point back at the host-side mapped port.
//
// We can't know the mapped port until after start, so we start the server
// with a placeholder, then update the public-host via fake-gcs-server's
// /_internal/config endpoint. The test sets STORAGE_EMULATOR_HOST.
func StartGCS(t *testing.T) GCSEndpoint {
	t.Helper()
	ctx := context.Background()

	const containerPort = "4443/tcp"
	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server:latest",
		ExposedPorts: []string{containerPort},
		Cmd:          []string{"-scheme", "http", "-port", "4443", "-backend", "memory"},
		WaitingFor: wait.ForListeningPort(containerPort).
			WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("fake-gcs-server start: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("Host: %v", err)
	}
	port, err := c.MappedPort(ctx, containerPort)
	if err != nil {
		t.Fatalf("MappedPort: %v", err)
	}
	hostPort := fmt.Sprintf("%s:%s", host, port.Port())
	endpointURL := "http://" + hostPort

	// Tell fake-gcs-server its publicly-visible host so resumable upload URLs
	// it returns are reachable from the host side. Without this the Writer's
	// resumable-upload follow-up requests target localhost:4443 inside the
	// container and time out.
	updatePublicHost(t, endpointURL, hostPort)

	ep := GCSEndpoint{
		URL:    endpointURL,
		Bucket: "pgsafe-test",
	}

	// Pre-create the bucket via direct HTTP (the JSON API). We avoid the
	// Go cloud.google.com/go/storage SDK here because its
	// STORAGE_EMULATOR_HOST/option.WithEndpoint behavior is brittle when
	// set up before the per-test fake-gcs-server is reachable; bucket-
	// creation via curl-equivalent is reliable.
	// itself uses the SDK and works around the SDK's quirks once.
	createBucketHTTP(t, endpointURL, ep.Bucket)

	return ep
}

func createBucketHTTP(t *testing.T, endpointURL, bucket string) {
	t.Helper()
	body := strings.NewReader(fmt.Sprintf(`{"name":%q}`, bucket))
	req, err := http.NewRequest(http.MethodPost,
		endpointURL+"/storage/v1/b?project=test-project", body)
	if err != nil {
		t.Fatalf("build bucket-create req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("create bucket: HTTP %d", resp.StatusCode)
	}
}

// updatePublicHost POSTs to fake-gcs-server's /_internal/config endpoint to
// reset the externalUrl that resumable-upload responses include. Without
// this the SDK's Writer.Close hits a redirect URL that points inside the
// container.
func updatePublicHost(t *testing.T, endpointURL, hostPort string) {
	t.Helper()
	body := strings.NewReader(fmt.Sprintf(`{"externalUrl":"http://%s"}`, hostPort))
	req, err := http.NewRequest(http.MethodPut, endpointURL+"/_internal/config", body)
	if err != nil {
		t.Fatalf("build config req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("set externalUrl: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("set externalUrl: HTTP %d", resp.StatusCode)
	}
}

// NewGCSClient returns a GCS client wired to the emulator. The caller is
// expected to have set STORAGE_EMULATOR_HOST already (the SDK reads it on
// construction and routes both data and upload paths to the emulator,
// using HTTP and skipping auth). Passing option.WithEndpoint here would
// override the env var with a path that confuses the SDK's upload URL
// builder.
func NewGCSClient(t *testing.T, ep GCSEndpoint) *storage.Client {
	t.Helper()
	_ = ep // intentional: env var is the load-bearing config
	client, err := storage.NewClient(context.Background(),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
