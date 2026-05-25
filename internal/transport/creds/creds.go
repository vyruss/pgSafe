// Package creds is Tenet-3 scoped-credential layer. The
// caller (running on the backup host, where long-lived storage
// credentials are allowed) mints a short-lived, prefix-scoped, write-only
// credential and delivers it to the PG-host worker over the JSON-RPC
// channel. The worker uses the credential ONLY for the duration of the
// backup; it never persists to disk on the PG host.
//
// Per Tenet 3 (no storage credentials on disk on the PG host):
//
//	Backend       Mechanism                                 Lifetime
//	-----------   ---------------------------------------   ----------
//	S3            sts:AssumeRole + inline session policy    1–2 hours
//	Azure Blob    User Delegation SAS                       1–2 hours
//	GCS           Service-account impersonation token       ~1 hour
//	SFTP          PEM key bytes (in-memory)                 process lifetime
//	POSIX         (no credentials; mount perms govern)      n/a
//
// All Credential payloads are JSON-marshalable; the worker receives them
// inside a ConfigureRequest's `Credentials` field and never touches the
// filesystem. The lifetime is enforced server-side (the cloud rejects
// expired tokens; SFTP key bytes go away when the worker process exits).
package creds

import (
	"encoding/json"
	"fmt"
	"time"
)

// Type discriminates the Credential payload.
type Type string

// Credential payload type discriminators.
const (
	// TypeNone is POSIX — no credential needed.
	TypeNone Type = ""
	// TypeS3STS is AWS S3 (or S3-compatible) scoped via STS AssumeRole.
	TypeS3STS Type = "s3sts"
	// TypeAzureSAS is Azure Blob User Delegation SAS.
	TypeAzureSAS Type = "azure_sas"
	// TypeGCSToken is GCS service-account impersonation.
	TypeGCSToken Type = "gcs_token"
	// TypeSFTPKey is SFTP PEM private-key bytes.
	TypeSFTPKey Type = "sftp_key"
)

// Credential is the discriminated union the caller ships to the
// worker. Exactly one variant is non-nil for any non-None type.
type Credential struct {
	Type Type `json:"type"`

	S3STS    *S3STSCredential    `json:"s3sts,omitempty"`
	AzureSAS *AzureSASCredential `json:"azure_sas,omitempty"`
	GCSToken *GCSTokenCredential `json:"gcs_token,omitempty"`
	SFTPKey  *SFTPKeyCredential  `json:"sftp_key,omitempty"`
}

// S3STSCredential is the output of MintS3STS. Region/Bucket/Prefix carry
// over from the caller's StorageConfig so the worker doesn't have to
// re-read the YAML.
type S3STSCredential struct {
	AccessKeyID     string    `json:"access_key_id"`
	SecretAccessKey string    `json:"secret_access_key"`
	SessionToken    string    `json:"session_token"`
	Expiration      time.Time `json:"expiration"`
	Region          string    `json:"region"`
	Bucket          string    `json:"bucket"`
	Prefix          string    `json:"prefix"`
	Endpoint        string    `json:"endpoint,omitempty"`
	UsePathStyle    bool      `json:"use_path_style,omitempty"`
}

// AzureSASCredential is the output of MintAzureSAS. SASToken is the query
// string portion (no leading "?"); ServiceURL is the full
// https://<account>.blob.core.windows.net/ form.
type AzureSASCredential struct {
	AccountName string    `json:"account_name"`
	Container   string    `json:"container"`
	SASToken    string    `json:"sas_token"`
	ServiceURL  string    `json:"service_url"`
	Expiration  time.Time `json:"expiration"`
	Prefix      string    `json:"prefix,omitempty"`
}

// GCSTokenCredential is the output of MintGCSToken. AccessToken is a
// short-lived OAuth2 token from iamcredentials.GenerateAccessToken.
type GCSTokenCredential struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"` // typically "Bearer"
	Expiration  time.Time `json:"expiration"`
	Bucket      string    `json:"bucket"`
	Prefix      string    `json:"prefix,omitempty"`
	Endpoint    string    `json:"endpoint,omitempty"` // for emulators
}

// SFTPKeyCredential carries the PEM-encoded private key bytes plus the
// connection metadata. Unlike the cloud variants this credential is not
// time-bounded server-side; we rely on the SFTP server's own access list
// to constrain what the worker can do, and on the worker process exit to
// clear the bytes from memory.
type SFTPKeyCredential struct {
	Host                  string `json:"host"`
	Port                  int    `json:"port"`
	Username              string `json:"username"`
	PrivateKeyPEM         []byte `json:"private_key_pem"`
	BasePath              string `json:"base_path"`
	HostKey               string `json:"host_key,omitempty"`
	InsecureIgnoreHostKey bool   `json:"insecure_ignore_host_key,omitempty"`
}

// Marshal returns a Configure-ready JSON byte slice. The worker calls
// Unmarshal on the same bytes.
func (c Credential) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Unmarshal parses a JSON byte slice into a Credential and asserts the
// variant matches the type tag.
func Unmarshal(b []byte) (Credential, error) {
	var c Credential
	if err := json.Unmarshal(b, &c); err != nil {
		return Credential{}, fmt.Errorf("creds: unmarshal: %w", err)
	}
	if err := c.Validate(); err != nil {
		return Credential{}, err
	}
	return c, nil
}

// Validate asserts the right variant is populated for the declared Type.
func (c Credential) Validate() error {
	switch c.Type {
	case TypeNone:
		return nil
	case TypeS3STS:
		if c.S3STS == nil {
			return fmt.Errorf("creds: type=s3sts but s3sts payload missing")
		}
	case TypeAzureSAS:
		if c.AzureSAS == nil {
			return fmt.Errorf("creds: type=azure_sas but azure_sas payload missing")
		}
	case TypeGCSToken:
		if c.GCSToken == nil {
			return fmt.Errorf("creds: type=gcs_token but gcs_token payload missing")
		}
	case TypeSFTPKey:
		if c.SFTPKey == nil {
			return fmt.Errorf("creds: type=sftp_key but sftp_key payload missing")
		}
	default:
		return fmt.Errorf("creds: unknown type %q", c.Type)
	}
	return nil
}
