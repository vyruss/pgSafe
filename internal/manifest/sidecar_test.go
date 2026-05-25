package manifest_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/manifest"
)

func TestSidecarRoundTrip(t *testing.T) {
	t.Parallel()
	s := manifest.Sidecar{
		Version:              1,
		Server:               "demo",
		EncryptionRecipients: []string{"age1abc"},
		Compression:          "zstd:3",
		StorageLayoutVersion: 1,
		WALSegments: []manifest.WALSegmentRecord{
			{
				Name:   "000000010000000000000003",
				Size:   16777216,
				SHA256: [32]byte{0x01, 0x02, 0x03},
			},
		},
	}
	data, err := manifest.MarshalSidecar(s)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := manifest.UnmarshalSidecar(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Errorf("round-trip mismatch:\n got %#v\nwant %#v", got, s)
	}
}

func TestSidecarMarshalIsIndentedJSON(t *testing.T) {
	t.Parallel()
	s := manifest.Sidecar{Version: 1, Server: "demo", StorageLayoutVersion: 1}
	data, err := manifest.MarshalSidecar(s)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), "\n") {
		t.Errorf("expected indented JSON; got %q", data)
	}
}

func TestSidecarUnmarshalRejectsUnknownField(t *testing.T) {
	t.Parallel()
	bad := []byte(`{"version": 1, "server": "demo", "rogue_field": true}`)
	_, err := manifest.UnmarshalSidecar(bad)
	if err == nil {
		t.Fatal("Unmarshal with unknown field: want error")
	}
	if !strings.Contains(err.Error(), "rogue_field") {
		t.Errorf("error %q should name the offender", err)
	}
}

func TestSidecarUnmarshalRejectsMalformed(t *testing.T) {
	t.Parallel()
	_, err := manifest.UnmarshalSidecar([]byte("not json"))
	if err == nil {
		t.Fatal("Unmarshal malformed JSON: want error")
	}
}
