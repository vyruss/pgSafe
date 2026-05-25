package manifest_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
)

func sampleStart() manifest.BackupStartInfo {
	return manifest.BackupStartInfo{
		SystemIdentifier: 7633557436145790995,
		Timeline:         1,
		StartLSN:         manifest.LSN(0x0_2000028),
		StartTime:        time.Date(2026, 4, 27, 12, 0, 0, 0, time.UTC),
	}
}

func sampleStop() manifest.BackupStopInfo {
	return manifest.BackupStopInfo{
		StopLSN:  manifest.LSN(0x0_2000120),
		StopTime: time.Date(2026, 4, 27, 12, 1, 0, 0, time.UTC),
	}
}

func TestBuilderProducesParseableJSONStructure(t *testing.T) {
	t.Parallel()
	b := manifest.NewBuilder(sampleStart())
	b.AddFile("PG_VERSION", 3, [32]byte{0x01}, time.Date(2026, 4, 27, 11, 59, 0, 0, time.UTC))
	out, err := b.Finalize(sampleStop())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	s := string(out)

	for _, want := range []string{
		`"PostgreSQL-Backup-Manifest-Version": 2`,
		`"System-Identifier": 7633557436145790995`,
		`"Files":`,
		`"Path": "PG_VERSION"`,
		`"Size": 3`,
		`"Checksum-Algorithm": "SHA256"`,
		`"Checksum": "0100000000000000000000000000000000000000000000000000000000000000"`,
		`"WAL-Ranges":`,
		`"Timeline": 1`,
		`"Start-LSN": "0/2000028"`,
		`"End-LSN": "0/2000120"`,
		`"Manifest-Checksum":`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("manifest missing %q\n--- output ---\n%s", want, s)
		}
	}
}

// TestManifestChecksumMatchesBody asserts that the trailing Manifest-Checksum
// equals SHA-256 of the manifest body up to (but not including) the
// `"Manifest-Checksum"` literal. This is the PG-defined invariant; if we ever
// drift from it, pg_verifybackup will reject our output.
func TestManifestChecksumMatchesBody(t *testing.T) {
	t.Parallel()
	b := manifest.NewBuilder(sampleStart())
	b.AddFile("a.txt", 5, [32]byte{0x02, 0x03}, time.Date(2026, 4, 27, 11, 59, 0, 0, time.UTC))
	b.AddFile("b.txt", 8, [32]byte{0x04, 0x05}, time.Date(2026, 4, 27, 11, 59, 0, 0, time.UTC))
	out, err := b.Finalize(sampleStop())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	mark := `"Manifest-Checksum"`
	idx := strings.Index(string(out), mark)
	if idx < 0 {
		t.Fatalf("manifest missing Manifest-Checksum")
	}
	body := out[:idx]
	want := sha256.Sum256(body)
	wantHex := hex.EncodeToString(want[:])

	prefix := `"Manifest-Checksum": "`
	hexStart := strings.Index(string(out), prefix) + len(prefix)
	hexEnd := strings.Index(string(out[hexStart:]), `"`)
	if hexEnd < 0 {
		t.Fatalf("malformed checksum tail: %q", out[idx:])
	}
	gotHex := string(out[hexStart : hexStart+hexEnd])

	if gotHex != wantHex {
		t.Errorf("Manifest-Checksum mismatch\n got %s\nwant %s", gotHex, wantHex)
	}
}

func TestLSNStringHexUppercase(t *testing.T) {
	t.Parallel()
	cases := map[manifest.LSN]string{
		0:                                  "0/0",
		manifest.LSN(0x12345678):           "0/12345678",
		manifest.LSN(uint64(1)<<32 | 0x42): "1/42",
		manifest.LSN(uint64(0xABCDEF12)<<32 | 0x34567890): "ABCDEF12/34567890",
	}
	for lsn, want := range cases {
		got := lsn.String()
		if got != want {
			t.Errorf("LSN(%#x).String() = %q, want %q", uint64(lsn), got, want)
		}
	}
}

func TestParseLSN(t *testing.T) {
	t.Parallel()
	good := map[string]manifest.LSN{
		"0/0":        0,
		"0/12345678": manifest.LSN(0x12345678),
		"1/42":       manifest.LSN(uint64(1)<<32 | 0x42),
	}
	for s, want := range good {
		got, err := manifest.ParseLSN(s)
		if err != nil {
			t.Errorf("ParseLSN(%q): %v", s, err)
			continue
		}
		if got != want {
			t.Errorf("ParseLSN(%q) = %#x, want %#x", s, uint64(got), uint64(want))
		}
	}
	for _, bad := range []string{"", "garbage", "0/0/0", "/0", "0/", "xyz/000", "1/zzz"} {
		if _, err := manifest.ParseLSN(bad); err == nil {
			t.Errorf("ParseLSN(%q): want error", bad)
		}
	}
}

func TestEmptyFilesBuilds(t *testing.T) {
	t.Parallel()
	b := manifest.NewBuilder(sampleStart())
	out, err := b.Finalize(sampleStop())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if !strings.Contains(string(out), `"Files": []`) && !strings.Contains(string(out), `"Files":[]`) {
		// PG produces "Files": [\n...\n] always; for empty we expect an empty array.
		// Either form (with or without space) is acceptable for parsers.
		t.Errorf("empty manifest should still serialize; got:\n%s", out)
	}
}
