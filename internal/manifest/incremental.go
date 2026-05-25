package manifest

// PG 17+ incremental file format. Used by pg_combinebackup to merge a full
// backup with one or more incrementals into a usable cluster.
//
// On-disk layout (everything in host byte order — pg_combinebackup runs on
// the same architecture as the cluster, so we use little-endian which matches
// every platform pgSafe supports today):
//
//	offset 0   : uint32  magic               = INCREMENTAL_MAGIC ("INCR")
//	offset 4   : uint32  num_blocks
//	offset 8   : uint32  truncation_block_length
//	offset 12  : uint32 × num_blocks         block_numbers
//	offset 12+4n: byte[8192] × num_blocks    block_data
//
//

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// IncrementalMagic is the four-byte sentinel PG writes at offset 0 of every
// INCREMENTAL.* file. Stored little-endian on disk on every supported
// platform; the on-disk byte sequence is "RCNI" but the in-memory uint32
// reads as the ASCII letters "INCR" reversed (PG's source spells it out in
// hex: 0x494e4352).
const IncrementalMagic uint32 = 0x494E4352

// PGPageSize is PostgreSQL's compiled-in BLCKSZ. pgSafe assumes the default
// 8 KiB; non-default builds need a recompile of pgSafe and PG together.
const PGPageSize = 8192

// EncodeIncremental writes an INCREMENTAL.* file body into w from blocks +
// data. blockNumbers and blockData must satisfy len(blockData) ==
// len(blockNumbers) * PGPageSize. truncationBlockLength is the relation's
// length in blocks at backup time (so pg_combinebackup can detect truncated
// relations).
func EncodeIncremental(w io.Writer, truncationBlockLength uint32, blockNumbers []uint32, blockData []byte) error {
	if len(blockNumbers) > int(^uint32(0)) {
		return fmt.Errorf("manifest: too many incremental blocks: %d", len(blockNumbers))
	}
	if len(blockData) != len(blockNumbers)*PGPageSize {
		return fmt.Errorf("manifest: blockData length %d != %d * %d (PG page size)",
			len(blockData), len(blockNumbers), PGPageSize)
	}

	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header[0:4], IncrementalMagic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(blockNumbers))) //nolint:gosec // bounds-checked
	binary.LittleEndian.PutUint32(header[8:12], truncationBlockLength)
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("manifest: write incremental header: %w", err)
	}

	blockNumBytes := make([]byte, 4*len(blockNumbers))
	for i, bn := range blockNumbers {
		binary.LittleEndian.PutUint32(blockNumBytes[4*i:4*(i+1)], bn)
	}
	if _, err := w.Write(blockNumBytes); err != nil {
		return fmt.Errorf("manifest: write incremental block numbers: %w", err)
	}

	if _, err := w.Write(blockData); err != nil {
		return fmt.Errorf("manifest: write incremental block data: %w", err)
	}
	return nil
}

// IncrementalHeader is the parsed-out top of an INCREMENTAL.* file.
type IncrementalHeader struct {
	NumBlocks             uint32
	TruncationBlockLength uint32
}

// DecodeIncrementalHeader reads + validates the 12-byte header. The reader is
// left positioned at the block-numbers section.
func DecodeIncrementalHeader(r io.Reader) (IncrementalHeader, error) {
	var hdr [12]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return IncrementalHeader{}, fmt.Errorf("manifest: read incremental header: %w", err)
	}
	magic := binary.LittleEndian.Uint32(hdr[0:4])
	if magic != IncrementalMagic {
		return IncrementalHeader{}, fmt.Errorf("manifest: bad incremental magic 0x%08x (want 0x%08x)",
			magic, IncrementalMagic)
	}
	return IncrementalHeader{
		NumBlocks:             binary.LittleEndian.Uint32(hdr[4:8]),
		TruncationBlockLength: binary.LittleEndian.Uint32(hdr[8:12]),
	}, nil
}

// DecodeIncrementalBlockNumbers reads num × uint32 from r.
func DecodeIncrementalBlockNumbers(r io.Reader, num uint32) ([]uint32, error) {
	if num == 0 {
		return nil, nil
	}
	raw := make([]byte, 4*int64(num))
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, fmt.Errorf("manifest: read block numbers: %w", err)
	}
	out := make([]uint32, num)
	for i := uint32(0); i < num; i++ {
		out[i] = binary.LittleEndian.Uint32(raw[4*i : 4*(i+1)])
	}
	return out, nil
}

// AddIncrementalFile records a per-file entry that pgSafe materializes as an
// INCREMENTAL.* on disk. The PG manifest's Files entries support an
// "Incremental": true flag per the PG 18 spec; we set it via b.AddIncremental.
//
// blocks are the block numbers present in the incremental file; size is the
// total on-disk size of the INCREMENTAL.* (i.e. 12 + 4*len(blocks) +
// PGPageSize*len(blocks)).
func (b *Builder) AddIncrementalFile(path string, size int64, sha256Sum [32]byte, blocks []uint32, modTime time.Time) {
	b.files = append(b.files, fileRec{
		path:        path,
		size:        size,
		sha256:      sha256Sum,
		modTime:     modTime.UTC(),
		incremental: true,
		blockCount:  uint32(len(blocks)), //nolint:gosec // bounds: caller is the caller
	})
}

// ErrTruncatedIncremental is returned when DecodeIncrementalHeader sees a
// header with NumBlocks > 0 but the file is too short to hold all blocks +
// data. Used by integrity checks during restore.
var ErrTruncatedIncremental = errors.New("manifest: incremental file truncated")
