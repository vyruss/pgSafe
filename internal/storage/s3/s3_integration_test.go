//go:build integration_cloud

package s3_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/cloudtest"
	pgsafes3 "github.com/vyruss/pgsafe/internal/storage/s3"
)

func newStorage(t *testing.T) *pgsafes3.Backend {
	t.Helper()
	ep := cloudtest.StartS3(t)
	r, err := pgsafes3.New(pgsafes3.Options{
		Client: cloudtest.NewS3Client(ep),
		Bucket: ep.Bucket,
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	if err := r.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return r
}

func putBytes(t *testing.T, r *pgsafes3.Backend, rel string, body []byte) {
	t.Helper()
	wc, err := r.Put(context.Background(), rel)
	if err != nil {
		t.Fatalf("Put(%s): %v", rel, err)
	}
	if _, err := wc.Write(body); err != nil {
		t.Fatalf("Write(%s): %v", rel, err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close(%s): %v", rel, err)
	}
}

func TestS3PutRoundTrip(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	body := []byte("the quick brown fox\n")

	putBytes(t, r, "subdir/file.bin", body)

	rc, err := r.Get(context.Background(), "subdir/file.bin")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("round-trip mismatch: got %q, want %q", got, body)
	}
}

func TestS3StatReportsSize(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	body := []byte("twelve bytes")

	putBytes(t, r, "x.bin", body)

	fi, err := r.Stat(context.Background(), "x.bin")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size != int64(len(body)) {
		t.Errorf("Size = %d, want %d", fi.Size, len(body))
	}
}

func TestS3StatNotFound(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	_, err := r.Stat(context.Background(), "nope")
	if err == nil {
		t.Fatal("Stat on missing object: want error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error should wrap os.ErrNotExist; got %v", err)
	}
}

func TestS3GetNotFound(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	_, err := r.Get(context.Background(), "nope")
	if err == nil {
		t.Fatal("Get on missing object: want error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error should wrap os.ErrNotExist; got %v", err)
	}
}

func TestS3List(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	for _, rel := range []string{"a", "sub/b", "sub/c", "deep/sub/d"} {
		putBytes(t, r, rel, []byte(rel))
	}

	all, err := r.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	got := make([]string, len(all))
	for i, fi := range all {
		got[i] = fi.Path
	}
	sort.Strings(got)
	want := []string{"a", "deep/sub/d", "sub/b", "sub/c"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("List(\"\") = %v, want %v", got, want)
	}

	subOnly, err := r.List(context.Background(), "sub")
	if err != nil {
		t.Fatalf("List(sub): %v", err)
	}
	if len(subOnly) != 2 {
		t.Errorf("List(sub) returned %d entries, want 2", len(subOnly))
	}
}

func TestS3CommitAtomicRename(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	body := []byte("manifest content")

	putBytes(t, r, "manifest.tmp", body)

	if err := r.Commit(context.Background(), "manifest.tmp", "manifest"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// final present, content matches
	rc, err := r.Get(context.Background(), "manifest")
	if err != nil {
		t.Fatalf("Get(manifest): %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, body) {
		t.Errorf("manifest content mismatch")
	}

	// tmp gone
	if _, err := r.Stat(context.Background(), "manifest.tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("manifest.tmp still present after Commit; expected NotFound, got %v", err)
	}
}

func TestS3CommitRefusesOverwrite(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	putBytes(t, r, "manifest.tmp", []byte("new"))
	putBytes(t, r, "manifest", []byte("existing"))

	err := r.Commit(context.Background(), "manifest.tmp", "manifest")
	if err == nil {
		t.Fatal("Commit overwriting existing final: want error")
	}
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("error should wrap os.ErrExist; got %v", err)
	}
}

// TestS3CommitConcurrentRace fires N concurrent Commits to the same final
// key. The driver does HEAD + CopyObject(IfNoneMatch=*); on AWS S3 the
// conditional write closes the race and exactly one goroutine wins. On
// MinIO (which may not honor IfNoneMatch on CopyObject in older versions),
// the HEAD pre-check still catches sequenced commits but a tight race
// window can let multiple Copys land. Either way:
//
//   - at least one commit succeeds, and
//   - the final manifest object exists with content from one of the
//     racers (no garbage state, no half-final).
//
// retention work introduces per-server lockfile + Invariant #4
// to make concurrent committers strictly sequential; the test is the
// pre-sanity check.
func TestS3CommitConcurrentRace(t *testing.T) {
	t.Parallel()
	r := newStorage(t)

	const N = 5
	for i := 0; i < N; i++ {
		putBytes(t, r, "tmp"+string(rune('a'+i)), []byte{byte('A' + i)})
	}

	var wg sync.WaitGroup
	results := make([]error, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i] = r.Commit(context.Background(),
				"tmp"+string(rune('a'+i)), "manifest")
		}()
	}
	wg.Wait()

	wins := 0
	for _, err := range results {
		if err == nil {
			wins++
		}
	}
	if wins < 1 {
		t.Errorf("concurrent race: zero successful commits (results=%v)", results)
	}

	// Whatever the resolution, final must hold one of the racers' contents.
	rc, _ := r.Get(context.Background(), "manifest")
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if len(got) != 1 || got[0] < 'A' || got[0] > byte('A'+N) {
		t.Errorf("final manifest content unexpected: %q", got)
	}
}

func TestS3PutLargeStreamMultipart(t *testing.T) {
	t.Parallel()
	r := newStorage(t)

	// 8 MiB — above the 5 MiB multipart threshold the SDK uses by default.
	body := bytes.Repeat([]byte("0123456789ABCDEF"), 512*1024)

	putBytes(t, r, "large.bin", body)

	rc, err := r.Get(context.Background(), "large.bin")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, body) {
		t.Errorf("large round-trip mismatch (got %d bytes, want %d)", len(got), len(body))
	}
}
