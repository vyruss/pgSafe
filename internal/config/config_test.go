// Package config is the YAML configuration parser for pgSafe.
//
// Test-first: this file declares the expected public surface (Config, Load,
// Validate) and exercises every locked behavior of the implementation.
package config_test

import (
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/config"
)

// validYAML is a known-good configuration. All other test cases mutate
// a substring of this fixture so the diff to a "good" config is one field.
const validYAML = `
server: demo
pg:
  conn_string: "host=localhost port=5432 user=pgsafe dbname=postgres sslmode=prefer"
  version: 18
storages:
  - type: posix
    path: /var/lib/pgsafe/storage
compression:
  codec: zstd
  level: 3
encryption:
  recipients:
    - "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
log:
  format: json
  level: info
`

func mustLoad(t *testing.T, yaml string) *config.Config {
	t.Helper()
	c, err := config.Load(strings.NewReader(yaml))
	if err != nil {
		t.Fatalf("Load: unexpected error: %v", err)
	}
	return c
}

func TestLoadGoodConfigRoundTrips(t *testing.T) {
	t.Parallel()

	c := mustLoad(t, validYAML)

	if c.Server != "demo" {
		t.Errorf("Server = %q, want %q", c.Server, "demo")
	}
	if c.PG.Version != 18 {
		t.Errorf("PG.Version = %d, want 18", c.PG.Version)
	}
	if c.Storages[0].Type != "posix" {
		t.Errorf("Storages[0].Type = %q, want %q", c.Storages[0].Type, "posix")
	}
	if c.Compression.Codec != "zstd" {
		t.Errorf("Compression.Codec = %q, want %q", c.Compression.Codec, "zstd")
	}
	if c.Log.Format != "json" {
		t.Errorf("Log.Format = %q, want %q", c.Log.Format, "json")
	}
	if got := len(c.Encryption.Recipients); got != 1 {
		t.Errorf("len(Encryption.Recipients) = %d, want 1", got)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate on good config: unexpected error: %v", err)
	}
}

func TestLoadRejectsUnknownYAMLKey(t *testing.T) {
	t.Parallel()

	yaml := validYAML + "\nbogus_field: 42\n"
	_, err := config.Load(strings.NewReader(yaml))
	if err == nil {
		t.Fatal("Load with unknown key: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bogus_field") {
		t.Errorf("error %q does not name the offending field", err)
	}
}

func TestValidateRejectsMissingServer(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML, "server: demo", "server: \"\"", 1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with empty server: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "server") {
		t.Errorf("error %q does not name the offending field", err)
	}
}

func TestValidateRejectsUnknownStorageType(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML, "type: posix", "type: floppy", 1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with unknown storage type: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "floppy") {
		t.Errorf("error %q does not name the offending value", err)
	}
}

func TestValidateAcceptsAllBackends(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"s3": `
server: demo
pg: { conn_string: "host=localhost", version: 18 }
storages:
  - type: s3
    s3:
      bucket: backups
      region: us-east-1
compression: { codec: zstd, level: 3 }
encryption: { recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"] }
`,
		"azure": `
server: demo
pg: { conn_string: "host=localhost", version: 18 }
storages:
  - type: azure
    azure:
      account_name: mystorage
      container: backups
      account_key: c29tZWtleQ==
compression: { codec: zstd, level: 3 }
encryption: { recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"] }
`,
		"gcs": `
server: demo
pg: { conn_string: "host=localhost", version: 18 }
storages:
  - type: gcs
    gcs:
      bucket: backups
compression: { codec: zstd, level: 3 }
encryption: { recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"] }
`,
		"sftp": `
server: demo
pg: { conn_string: "host=localhost", version: 18 }
storages:
  - type: sftp
    sftp:
      host: backup.example.com
      username: pgsafe
      password: hunter2
      base_path: /backups
      insecure_ignore_host_key: true
compression: { codec: zstd, level: 3 }
encryption: { recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"] }
`,
	}
	for name, yaml := range cases {
		yaml := yaml
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := mustLoad(t, yaml)
			if err := c.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

func TestValidateAcceptsPGVersions13Through18(t *testing.T) {
	t.Parallel()
	for _, v := range []int{13, 14, 15, 16, 17, 18} {
		v := v
		t.Run(strings.ReplaceAll(validYAML, "version: 18", "version:")+"_"+itoa(v), func(t *testing.T) {
			t.Parallel()
			yaml := strings.Replace(validYAML, "version: 18", "version: "+itoa(v), 1)
			c := mustLoad(t, yaml)
			if err := c.Validate(); err != nil {
				t.Errorf("PG %d: %v", v, err)
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [4]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestValidateRejectsMismatchedStorageSections(t *testing.T) {
	t.Parallel()

	// type=s3 with a leftover path: from a copy-pasted posix config is a
	// classic operator footgun — Validate must catch it.
	yaml := `
server: demo
pg: { conn_string: "host=localhost", version: 18 }
storages:
  - type: s3
    path: /var/lib/pgsafe/storage
    s3:
      bucket: backups
      region: us-east-1
compression: { codec: zstd, level: 3 }
encryption: { recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"] }
`
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with stale path under type=s3: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "path") {
		t.Errorf("error %q should call out the leftover path field", err)
	}
}

func TestValidateRejectsRelativeStoragePath(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML, "/var/lib/pgsafe/storage", "relative/path", 1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with relative storage path: expected error, got nil")
	}
}

func TestValidateRejectsUnknownCodec(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML, "codec: zstd", "codec: brotli", 1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with unknown codec: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "brotli") {
		t.Errorf("error %q does not name the bad codec", err)
	}
}

func TestValidateAllowsKnownCodecs(t *testing.T) {
	t.Parallel()

	for _, codec := range []string{"gzip", "lz4", "zstd", "bzip2"} {
		t.Run(codec, func(t *testing.T) {
			yaml := strings.Replace(validYAML, "codec: zstd", "codec: "+codec, 1)
			c := mustLoad(t, yaml)
			if err := c.Validate(); err != nil {
				t.Errorf("Validate with codec=%q: unexpected error: %v", codec, err)
			}
		})
	}
}

func TestValidateRejectsBadAgeRecipient(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML,
		`"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"`,
		`"not-a-valid-age-recipient"`, 1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with bad age recipient: expected error, got nil")
	}
}

func TestValidateRejectsBadLogFormat(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML, "format: json", "format: xml", 1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with bad log format: expected error, got nil")
	}
}

func TestValidateRejectsBadLogLevel(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML, "level: info", "level: trace", 1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with bad log level: expected error, got nil")
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	t.Parallel()

	// Same config but log section omitted entirely; defaults must fill in.
	yaml := `
server: demo
pg:
  conn_string: "host=localhost port=5432 user=pgsafe dbname=postgres sslmode=prefer"
  version: 18
storages:
  - type: posix
    path: /var/lib/pgsafe/storage
compression:
  codec: zstd
  level: 3
encryption:
  recipients:
    - "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
`
	c := mustLoad(t, yaml)
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: unexpected error: %v", err)
	}
	if c.Log.Format != "json" {
		t.Errorf("Log.Format default = %q, want %q", c.Log.Format, "json")
	}
	if c.Log.Level != "info" {
		t.Errorf("Log.Level default = %q, want %q", c.Log.Level, "info")
	}
}

func TestLoadRejectsMalformedYAML(t *testing.T) {
	t.Parallel()

	_, err := config.Load(strings.NewReader("server: [unclosed"))
	if err == nil {
		t.Fatal("Load on malformed YAML: expected error, got nil")
	}
}

// TestLoadStoragesSingle verifies that a single-element storages: list parses
// correctly and PrimaryStorage() returns the first entry.
func TestLoadStoragesSingle(t *testing.T) {
	t.Parallel()

	c := mustLoad(t, validYAML)
	if len(c.Storages) != 1 {
		t.Fatalf("Storages len = %d, want 1", len(c.Storages))
	}
	if c.Storages[0].Type != "posix" {
		t.Errorf("Storages[0].Type = %q, want posix", c.Storages[0].Type)
	}
	if c.PrimaryStorage().Type != "posix" {
		t.Errorf("PrimaryStorage().Type = %q, want posix", c.PrimaryStorage().Type)
	}
}

// TestLoadStoragesList exercises the v3 `storages:` list form with two backends.
func TestLoadStoragesList(t *testing.T) {
	t.Parallel()

	yaml := `
server: demo
pg:
  conn_string: "host=localhost"
  version: 18
storages:
  - type: posix
    path: /var/lib/pgsafe/primary
  - type: s3
    s3:
      bucket: pgsafe-offsite
      region: us-east-1
compression: { codec: zstd, level: 3 }
encryption:
  recipients:
    - "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
log: { format: json, level: info }
`
	c := mustLoad(t, yaml)
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate multi-storage config: %v", err)
	}
	if len(c.Storages) != 2 {
		t.Fatalf("Storages len = %d, want 2", len(c.Storages))
	}
	if c.Storages[0].Type != "posix" {
		t.Errorf("Storages[0].Type = %q, want posix", c.Storages[0].Type)
	}
	if c.Storages[1].Type != "s3" {
		t.Errorf("Storages[1].Type = %q, want s3", c.Storages[1].Type)
	}
	if c.PrimaryStorage().Type != "posix" {
		t.Errorf("PrimaryStorage().Type = %q, want posix", c.PrimaryStorage().Type)
	}
}

// TestValidateStoragesEmpty confirms Validate rejects a config with no storage
// section at all.
func TestValidateStoragesEmpty(t *testing.T) {
	t.Parallel()

	yaml := `
server: demo
pg: { conn_string: "host=localhost", version: 18 }
compression: { codec: zstd, level: 3 }
encryption:
  recipients:
    - "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
`
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with no storage config: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "storage") {
		t.Errorf("error %q does not mention storage", err)
	}
}

// TestValidateStoragesListRejectsInvalidEntry checks per-element validation
// TestPGHostAndSSHExtraArgsRoundTrip asserts the new caller-relative
// SSH-reach fields parse, round-trip into the Config, and remain
// optional (their absence is not an error).
func TestPGHostAndSSHExtraArgsRoundTrip(t *testing.T) {
	t.Parallel()

	yaml := strings.Replace(validYAML,
		"  version: 18",
		"  version: 18\n  host: \"pgsafe@pg.example.com\"\n  ssh_extra_args: \"-p 2222 -i ~/.ssh/id_ed25519\"",
		1)
	c := mustLoad(t, yaml)
	if c.PG.Host != "pgsafe@pg.example.com" {
		t.Errorf("PG.Host = %q, want %q", c.PG.Host, "pgsafe@pg.example.com")
	}
	if c.PG.SSHExtraArgs != "-p 2222 -i ~/.ssh/id_ed25519" {
		t.Errorf("PG.SSHExtraArgs = %q, want %q", c.PG.SSHExtraArgs, "-p 2222 -i ~/.ssh/id_ed25519")
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate with PG.Host + PG.SSHExtraArgs: %v", err)
	}
}

// TestPGStorageReachAccepted exercises the storage_reach knob's
// three permitted values. Default (empty / "auto") must parse + pass
// validate; "native_only" and "via_caller" likewise. "auto" with the
// literal string must round-trip identically to empty (treated as
// the default).
func TestPGStorageReachAccepted(t *testing.T) {
	t.Parallel()
	for _, val := range []string{"", "auto", "native_only", "via_caller"} {
		t.Run("storage_reach="+val, func(t *testing.T) {
			t.Parallel()
			yaml := strings.Replace(validYAML,
				"  version: 18",
				"  version: 18\n  storage_reach: \""+val+"\"",
				1)
			c := mustLoad(t, yaml)
			if c.PG.StorageReach != val {
				t.Errorf("PG.StorageReach = %q, want %q", c.PG.StorageReach, val)
			}
			if err := c.Validate(); err != nil {
				t.Errorf("Validate with storage_reach=%q: %v", val, err)
			}
		})
	}
}

// TestPGStorageReachRejected catches typos at parse + validate time
// rather than waiting until the inference helper sees them.
func TestPGStorageReachRejected(t *testing.T) {
	t.Parallel()
	yaml := strings.Replace(validYAML,
		"  version: 18",
		"  version: 18\n  storage_reach: \"sometimes\"",
		1)
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with storage_reach=sometimes: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "storage_reach") {
		t.Errorf("error %q does not name the offending field", err)
	}
}

// TestPGHostAndSSHExtraArgsOptional confirms that the validYAML (which
// sets neither field) still parses cleanly and yields zero-valued
// fields. Backwards-compatible — operators without remote-PG topologies
// don't have to add anything.
func TestPGHostAndSSHExtraArgsOptional(t *testing.T) {
	t.Parallel()

	c := mustLoad(t, validYAML)
	if c.PG.Host != "" {
		t.Errorf("PG.Host = %q on validYAML, want zero value", c.PG.Host)
	}
	if c.PG.SSHExtraArgs != "" {
		t.Errorf("PG.SSHExtraArgs = %q on validYAML, want zero value", c.PG.SSHExtraArgs)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate without PG.Host/SSHExtraArgs: %v", err)
	}
}

// when using the storages: list form.
func TestValidateStoragesListRejectsInvalidEntry(t *testing.T) {
	t.Parallel()

	yaml := `
server: demo
pg: { conn_string: "host=localhost", version: 18 }
storages:
  - type: posix
    path: /var/lib/pgsafe/primary
  - type: floppy
compression: { codec: zstd, level: 3 }
encryption:
  recipients:
    - "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
`
	c := mustLoad(t, yaml)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate with invalid storages[1]: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "floppy") {
		t.Errorf("error %q does not name the bad type", err)
	}
}
