package main

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/vyruss/pgsafe/internal/config"
)

// newAzureContainerClient builds the azblob container-scoped client
// pgsafe writes to. Extracted from openAzure so its compat-critical
// option choices can be unit-tested independently of credential
// loading and backend wiring.
//
// Compat-critical defaults:
//
//   - cloud=AzurePublic. Government / China clouds use different
//     URL templates and SDK feature surfaces; pgsafe pins to public
//     Azure. Operators on sovereign clouds need an explicit knob
//     (not yet exposed) — failing now is better than silently writing
//     to the wrong cloud.
//   - No `azcore.ClientOptions.Telemetry`, `Retry`, `Transport`
//     overrides. The SDK defaults are deliberate; pgsafe doesn't
//     opt into per-chunk CRC64 validation, customer-provided
//     encryption keys, or any access-tier shenanigans here. Those
//     are per-operation choices in internal/storage/azure that
//     also stay unset.
func newAzureContainerClient(c *config.AzureConfig) (*container.Client, error) {
	if c.ConnectionString != "" {
		svc, err := azblob.NewClientFromConnectionString(c.ConnectionString, nil)
		if err != nil {
			return nil, err
		}
		return svc.ServiceClient().NewContainerClient(c.Container), nil
	}
	clientOpts := &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Cloud: cloud.AzurePublic}}
	if c.AccountKey != "" {
		cred, err := azblob.NewSharedKeyCredential(c.AccountName, c.AccountKey)
		if err != nil {
			return nil, err
		}
		serviceURL := c.BlobEndpoint
		if serviceURL == "" {
			serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", c.AccountName)
		}
		svc, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, clientOpts)
		if err != nil {
			return nil, err
		}
		return svc.ServiceClient().NewContainerClient(c.Container), nil
	}
	if c.SASToken != "" {
		serviceURL := c.BlobEndpoint
		if serviceURL == "" {
			serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", c.AccountName)
		}
		full := serviceURL + "?" + c.SASToken
		svc, err := azblob.NewClientWithNoCredential(full, clientOpts)
		if err != nil {
			return nil, err
		}
		return svc.ServiceClient().NewContainerClient(c.Container), nil
	}
	return nil, errors.New("azure: no credentials supplied")
}
