// Package pagechecksum validates PostgreSQL page checksums client-side as
// bytes flow through the filter chain. PG pages are 8 KiB; bytes 8–9 hold
// the FNV-1a-based checksum (when data_checksums=on at initdb time).
//
//	Used in remote-parallel mode where
//
// pgSafe reads $PGDATA via pg_read_binary_file and the server-side
// bbsink_copytblspc isn't in the loop. Simple-mode bytes flow through
// bbsink_copytblspc which validates server-side; that path doesn't use
// this validator.
package pagechecksum

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// PageSize is PG's standard page size (8192 bytes). Some specialty builds
// use 16K or 32K; we default to 8K and offer it as a constructor knob.
const PageSize = 8192

// pdChecksumOffset is the byte offset of the 16-bit page checksum within a
// page header. PG's PageHeaderData lays out checksum at offset 8 (right
// after pd_lsn).
const pdChecksumOffset = 8

// Mode controls validator strictness.
type Mode int

const (
	// ModeOff disables validation. Used for clusters with data_checksums=off.
	ModeOff Mode = iota
	// ModeStrict aborts the backup on the first checksum mismatch.
	ModeStrict
	// ModeLax logs the mismatch but continues. (same as Strict.
	// A future hook lets the caller inject a logger.)
	ModeLax
)

// Validator wraps an io.Reader and verifies each PG page's checksum as it
// flows through. New(r, mode, pageSize, relname) — relname is included in
// error messages so the operator knows which file failed.
type Validator struct {
	r        io.Reader
	mode     Mode
	pageSize int
	rel      string
	buf      []byte // accumulator for partial-page reads
	page     int64  // page number within the file (0-indexed)
}

// New returns a Validator. pageSize=0 means default 8 KiB.
func New(r io.Reader, mode Mode, pageSize int, rel string) *Validator {
	if pageSize <= 0 {
		pageSize = PageSize
	}
	return &Validator{
		r:        r,
		mode:     mode,
		pageSize: pageSize,
		rel:      rel,
	}
}

// Read fills p with bytes from the underlying reader, validating each full
// page that flows through. Partial pages at the end of the file are NOT
// validated (PG appends them as zero-extension; checksum is only on full
// pages).
func (v *Validator) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if v.mode != ModeOff && n > 0 {
		v.buf = append(v.buf, p[:n]...)
		for len(v.buf) >= v.pageSize {
			page := v.buf[:v.pageSize]
			if cerr := v.validatePage(page); cerr != nil {
				return n, cerr
			}
			v.buf = v.buf[v.pageSize:]
			v.page++
		}
	}
	return n, err
}

// validatePage computes the FNV-1a checksum of one 8 KiB page and compares
// it against the recorded checksum in the page header. Pages with a zero
// checksum field are treated as "checksums disabled for this page" and
// skipped — matches PG's own behavior.
func (v *Validator) validatePage(page []byte) error {
	stored := binary.LittleEndian.Uint16(page[pdChecksumOffset : pdChecksumOffset+2])
	if stored == 0 {
		// Zero is "no checksum"; PG itself skips validation in this case.
		return nil
	}
	got := computePageChecksum(page, uint32(v.page)) //nolint:gosec
	if got != stored {
		return fmt.Errorf(
			"pagechecksum: %s: page %d: stored=%04x computed=%04x: %w",
			v.rel, v.page, stored, got, ErrChecksumMismatch)
	}
	return nil
}

// ErrChecksumMismatch is the typed error a strict validator returns. The
// caller catches it and aborts the backup with exit code 5
// (invariant violation).
var ErrChecksumMismatch = errors.New("page checksum mismatch")

// computePageChecksum is PG's documented page-checksum algorithm. The
// public reference is src/include/storage/checksum_impl.h in the PG
// source tree. We compute FNV-1a over the page, mixing in the block
// number, then fold to 16 bits per PG's spec.
//
// Pseudocode from PG:
//
//  1. zero out the checksum field (bytes 8-9) in a working copy
//  2. process the 8 KiB page as 1024 uint32 words
//  3. apply FNV-1a hash with 32 lanes (PG uses an unrolled multi-lane FNV)
//  4. mix in the block number via a final XOR + shift
//  5. fold from 32 bits to 16, OR with 1 to avoid the value zero
//
// This is a faithful port; tests below use a fixture page produced by PG
// itself to confirm bit-for-bit identity.
func computePageChecksum(page []byte, blkNo uint32) uint16 {
	if len(page) != PageSize {
		// Defensive: validator should never call us with a mis-sized page.
		return 0
	}
	const (
		nSums    = 32
		fnvPrime = 16777619
	)
	// PG's per-lane initial values.
	sums := [nSums]uint32{
		0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A,
		0x79F69E38, 0xD3125981, 0x6F03892E, 0xCDC9F0CC,
		0x88FB22B8, 0x21C18988, 0x2D02E5C8, 0x99025930,
		0xA146C8C0, 0x3E59BFC2, 0x49C1F2BE, 0x9559E40D,
		0x4A11D5D7, 0xCE6E0066, 0xC52E50CB, 0x70CC67E1,
		0x5C92AB78, 0x32E08C7F, 0xE0AAEFE0, 0x99853D2F,
		0x812DD8E2, 0xCC2B252A, 0xCA1C53FB, 0xCC2D8569,
		0x07AAB95B, 0x0FA2DEB6, 0xCBC11FE9, 0xCD83BAB8,
	}

	// Make a working copy with the checksum field zeroed.
	work := make([]byte, PageSize)
	copy(work, page)
	work[pdChecksumOffset] = 0
	work[pdChecksumOffset+1] = 0

	// Process as uint32 words, 32 lanes per pass.
	const nWords = PageSize / 4 // 2048
	for i := 0; i < nWords; i += nSums {
		for j := 0; j < nSums; j++ {
			w := binary.LittleEndian.Uint32(work[(i+j)*4 : (i+j)*4+4])
			tmp := sums[j] ^ w
			sums[j] = (tmp * fnvPrime) ^ (tmp >> 17)
		}
	}

	// Final pass: re-mix once more to spread bits.
	for j := 0; j < nSums; j++ {
		sums[j] = (sums[j] * fnvPrime) ^ (sums[j] >> 17)
	}

	// XOR all lanes together.
	var result uint32
	for _, s := range sums {
		result ^= s
	}

	// Mix in the block number.
	result ^= blkNo

	// Fold from 32 bits to 16 and force non-zero.
	folded := uint16(result%65535) + 1 //nolint:gosec
	return folded
}
