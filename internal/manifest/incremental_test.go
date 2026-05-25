package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

func TestEncodeDecodeIncrementalRoundTrip(t *testing.T) {
	t.Parallel()

	blocks := []uint32{0, 4, 17, 9999}
	data := make([]byte, len(blocks)*PGPageSize)
	// Fill each page with a pattern keyed off the block number.
	for i, b := range blocks {
		for j := 0; j < PGPageSize; j++ {
			data[i*PGPageSize+j] = byte((int(b) + j) & 0xff)
		}
	}

	var buf bytes.Buffer
	if err := EncodeIncremental(&buf, 12345, blocks, data); err != nil {
		t.Fatalf("EncodeIncremental: %v", err)
	}

	hdr, err := DecodeIncrementalHeader(&buf)
	if err != nil {
		t.Fatalf("DecodeIncrementalHeader: %v", err)
	}
	if hdr.NumBlocks != uint32(len(blocks)) {
		t.Errorf("NumBlocks = %d, want %d", hdr.NumBlocks, len(blocks))
	}
	if hdr.TruncationBlockLength != 12345 {
		t.Errorf("TruncationBlockLength = %d, want 12345", hdr.TruncationBlockLength)
	}

	gotBlocks, err := DecodeIncrementalBlockNumbers(&buf, hdr.NumBlocks)
	if err != nil {
		t.Fatalf("DecodeIncrementalBlockNumbers: %v", err)
	}
	for i, b := range gotBlocks {
		if b != blocks[i] {
			t.Errorf("block[%d] = %d, want %d", i, b, blocks[i])
		}
	}

	// Remaining buf is exactly the block-data section.
	remaining, err := io.ReadAll(&buf)
	if err != nil {
		t.Fatalf("read remaining: %v", err)
	}
	if !bytes.Equal(remaining, data) {
		t.Errorf("block data did not round-trip")
	}
}

func TestEncodeIncrementalRejectsDataLengthMismatch(t *testing.T) {
	t.Parallel()
	blocks := []uint32{0, 1}
	wrong := make([]byte, PGPageSize) // half the expected length
	err := EncodeIncremental(io.Discard, 0, blocks, wrong)
	if err == nil {
		t.Fatal("EncodeIncremental: want error for short blockData, got nil")
	}
}

func TestDecodeIncrementalHeaderRejectsBadMagic(t *testing.T) {
	t.Parallel()
	bad := make([]byte, 12)
	binary.LittleEndian.PutUint32(bad[0:4], 0xDEADBEEF)
	_, err := DecodeIncrementalHeader(bytes.NewReader(bad))
	if err == nil {
		t.Fatal("DecodeIncrementalHeader: want error for bad magic, got nil")
	}
}

func TestDecodeIncrementalHeaderTruncated(t *testing.T) {
	t.Parallel()
	short := []byte{0x01, 0x02}
	_, err := DecodeIncrementalHeader(bytes.NewReader(short))
	if err == nil {
		t.Fatal("DecodeIncrementalHeader: want error for short header, got nil")
	}
}

func TestEmptyIncrementalIsValid(t *testing.T) {
	t.Parallel()
	// Zero blocks is legal — represents a relation that didn't change.
	var buf bytes.Buffer
	if err := EncodeIncremental(&buf, 100, nil, nil); err != nil {
		t.Fatalf("EncodeIncremental(0 blocks): %v", err)
	}
	if buf.Len() != 12 {
		t.Errorf("0-block incremental len = %d, want 12 (header only)", buf.Len())
	}
	hdr, err := DecodeIncrementalHeader(&buf)
	if err != nil {
		t.Fatalf("DecodeIncrementalHeader: %v", err)
	}
	if hdr.NumBlocks != 0 {
		t.Errorf("NumBlocks = %d, want 0", hdr.NumBlocks)
	}
	if hdr.TruncationBlockLength != 100 {
		t.Errorf("TruncationBlockLength = %d, want 100", hdr.TruncationBlockLength)
	}
	blocks, err := DecodeIncrementalBlockNumbers(&buf, 0)
	if err != nil {
		t.Fatalf("DecodeIncrementalBlockNumbers(0): %v", err)
	}
	if len(blocks) != 0 {
		t.Errorf("blocks = %v, want empty", blocks)
	}
}

func TestBuilderEmitsIncrementalFlag(t *testing.T) {
	t.Parallel()
	b := NewBuilder(BackupStartInfo{
		SystemIdentifier: 7,
		Timeline:         1,
		StartLSN:         LSN(0x1000),
		StartTime:        time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC),
	})
	b.AddFile("PG_VERSION", 3, [32]byte{}, time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC))
	b.AddIncrementalFile("base/16384/16385", 8204, [32]byte{}, []uint32{0, 1}, time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC))
	out, err := b.Finalize(BackupStopInfo{StopLSN: LSN(0x2000), StopTime: time.Date(2026, 4, 28, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	s := string(out)
	if !bytes.Contains(out, []byte(`"Path": "base/16384/16385"`)) || !bytes.Contains(out, []byte(`"Incremental": true`)) {
		t.Errorf("incremental flag missing in manifest:\n%s", s)
	}
	// Count occurrences of "Incremental" — must be exactly one, on the
	// incremental file's entry.
	if got := bytes.Count(out, []byte(`"Incremental"`)); got != 1 {
		t.Errorf(`expected exactly one "Incremental" field; got %d:\n%s`, got, s)
	}
}

func TestErrTruncatedIncrementalIsExported(t *testing.T) {
	t.Parallel()
	if ErrTruncatedIncremental == nil {
		t.Fatal("ErrTruncatedIncremental is nil")
	}
	// Sentinel: callers can errors.Is against it.
	if !errors.Is(ErrTruncatedIncremental, ErrTruncatedIncremental) {
		t.Fatal("errors.Is sanity check failed")
	}
}
