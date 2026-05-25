package cloudtest

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/azurite"
)

// AzureEndpoint carries the connection details for the spun-up Azurite
// container. Azurite emulates Azure Blob Storage and accepts the well-known
// "devstoreaccount1" credentials used by the Azure SDKs in test mode.
type AzureEndpoint struct {
	BlobURL     string // http://host:port/devstoreaccount1
	AccountName string
	AccountKey  string
	Container   string // pre-created
}

// Azurite uses fixed test credentials documented at
// https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key
const (
	azuriteAccountName = "devstoreaccount1"
	azuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// StartAzurite launches an Azurite container with Blob service enabled,
// creates a fresh container ("pgsafe-test"), and returns the endpoint.
func StartAzurite(t *testing.T) AzureEndpoint {
	t.Helper()
	ctx := context.Background()

	// --skipApiVersionCheck: the azure-sdk-for-go's azblob library sends
	// API-version headers that Azurite hasn't shipped support for yet
	// (Azurite trails the SDK by 6-12 months). Skipping the check is the
	// official Azurite recommendation for this exact mismatch.
	c, err := azurite.Run(ctx, "mcr.microsoft.com/azure-storage/azurite:latest",
		testcontainers.WithCmd(
			"azurite",
			"-l", "/data",
			"--blobHost", "0.0.0.0",
			"--queueHost", "0.0.0.0",
			"--tableHost", "0.0.0.0",
			"--skipApiVersionCheck",
		),
	)
	if err != nil {
		t.Fatalf("azurite.Run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	blobURL, err := c.BlobServiceURL(ctx)
	if err != nil {
		t.Fatalf("BlobServiceURL: %v", err)
	}

	ep := AzureEndpoint{
		BlobURL:     blobURL + "/" + azuriteAccountName,
		AccountName: azuriteAccountName,
		AccountKey:  azuriteAccountKey,
		Container:   "pgsafe-test",
	}

	// Pre-create the container.
	client := NewAzureServiceClient(t, ep)
	if _, err := client.CreateContainer(ctx, ep.Container, &service.CreateContainerOptions{}); err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	return ep
}

// NewAzureServiceClient builds an azblob service client wired to the emulator.
func NewAzureServiceClient(t *testing.T, ep AzureEndpoint) *service.Client {
	t.Helper()
	cred, err := azblob.NewSharedKeyCredential(ep.AccountName, ep.AccountKey)
	if err != nil {
		t.Fatalf("NewSharedKeyCredential: %v", err)
	}
	client, err := service.NewClientWithSharedKeyCredential(ep.BlobURL, cred, nil)
	if err != nil {
		t.Fatalf("service.NewClientWithSharedKeyCredential: %v", err)
	}
	return client
}

// NewAzureContainerClient returns a client scoped to the pre-created container
// for the given endpoint. Convenience wrapper for tests that only need to
// touch the test container.
func NewAzureContainerClient(t *testing.T, ep AzureEndpoint) *container.Client {
	t.Helper()
	return NewAzureServiceClient(t, ep).NewContainerClient(ep.Container)
}
