package pagechecksum

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// makePage builds a fixture 8 KiB page with the given block number and a
// pre-computed checksum stored at the conventional offset. The page body is
// otherwise deterministic so re-running the test produces the same bytes.
func makePage(t *testing.T, blkNo uint32) []byte {
	t.Helper()
	page := make([]byte, PageSize)
	// Fill with a recognisable pattern.
	for i := range page {
		page[i] = byte(i & 0xff)
	}
	// Zero the checksum field, then compute and store.
	page[pdChecksumOffset] = 0
	page[pdChecksumOffset+1] = 0
	sum := computePageChecksum(page, blkNo)
	binary.LittleEndian.PutUint16(page[pdChecksumOffset:], sum)
	return page
}

func TestValidatorOffPassesThrough(t *testing.T) {
	t.Parallel()
	src := bytes.Repeat([]byte{0xff}, PageSize)
	v := New(bytes.NewReader(src), ModeOff, 0, "noop.bin")
	got, err := io.ReadAll(v)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Errorf("ModeOff should be a passthrough")
	}
}

func TestValidatorAcceptsZeroChecksumPage(t *testing.T) {
	t.Parallel()
	// A page with checksum=0 is "checksums disabled" per PG semantics; the
	// validator MUST accept it even in strict mode.
	page := bytes.Repeat([]byte{0xab}, PageSize)
	page[pdChecksumOffset] = 0
	page[pdChecksumOffset+1] = 0

	v := New(bytes.NewReader(page), ModeStrict, 0, "zero.bin")
	if _, err := io.ReadAll(v); err != nil {
		t.Errorf("zero-checksum page should be accepted; got %v", err)
	}
}

func TestValidatorAcceptsSelfConsistentChecksum(t *testing.T) {
	t.Parallel()
	// Build a page whose checksum was computed by our own algorithm. Whatever
	// computePageChecksum's bit-exact correctness against PG, it must be
	// internally self-consistent: validate(makePage(blkNo)) passes.
	page := makePage(t, 0)
	v := New(bytes.NewReader(page), ModeStrict, 0, "self.bin")
	if _, err := io.ReadAll(v); err != nil {
		t.Errorf("self-consistent page should validate; got %v", err)
	}
}

func TestValidatorRejectsCorruptedPage(t *testing.T) {
	t.Parallel()
	page := makePage(t, 0)
	// Flip one byte in the data area (avoid the checksum field itself).
	page[100] ^= 0xff

	v := New(bytes.NewReader(page), ModeStrict, 0, "corrupt.bin")
	_, err := io.ReadAll(v)
	if err == nil {
		t.Fatal("corrupted page should fail validation")
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("error should wrap ErrChecksumMismatch; got %v", err)
	}
}

func TestValidatorMultiPageStream(t *testing.T) {
	t.Parallel()
	// Three pages with different block numbers — exercise the
	// page-counter-incrementing path.
	var buf bytes.Buffer
	for blk := uint32(0); blk < 3; blk++ {
		buf.Write(makePage(t, blk))
	}

	v := New(bytes.NewReader(buf.Bytes()), ModeStrict, 0, "multi.bin")
	got, err := io.ReadAll(v)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 3*PageSize {
		t.Errorf("expected %d bytes, got %d", 3*PageSize, len(got))
	}
}

func TestValidatorPartialFinalPagePasses(t *testing.T) {
	t.Parallel()
	// PG occasionally writes a partial trailing page during recovery; the
	// validator must accept these (they don't have a full checksum block).
	page := makePage(t, 0)
	src := make([]byte, 0, len(page)+32)
	src = append(src, page...)
	src = append(src, []byte("partial trailing data")...)

	v := New(bytes.NewReader(src), ModeStrict, 0, "partial.bin")
	got, err := io.ReadAll(v)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(src) {
		t.Errorf("expected %d bytes, got %d", len(src), len(got))
	}
}
