//go:build integration_cloud

package gcs_test

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/cloudtest"
	"github.com/vyruss/pgsafe/internal/storage/gcs"
)

// GCS tests deliberately don't use t.Parallel: the SDK respects
// STORAGE_EMULATOR_HOST process-wide, and per-test fake-gcs-server endpoints
// would race. Sequential execution is fine — the per-test container start
// is the slow part (~0.5s) and we only have a few cases.

func newStorage(t *testing.T) *gcs.Backend {
	t.Helper()
	ep := cloudtest.StartGCS(t)
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(ep.URL, "http://"))
	r, err := gcs.New(gcs.Options{
		Client: cloudtest.NewGCSClient(t, ep),
		Bucket: ep.Bucket,
	})
	if err != nil {
		t.Fatalf("gcs.New: %v", err)
	}
	if err := r.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return r
}

func putBytes(t *testing.T, r *gcs.Backend, rel string, body []byte) {
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

// TestGCSPutThenStat — fake-gcs-server has a known bug where the SDK's
// NewReader can't find an object the SDK's Writer just created (the
// SDK-returned mediaLink and the emulator's stored generation disagree).
// Driver Get against real GCS works; the round-trip is exercised in the
// extended-cloud manual job. Here we verify Put landed an object of the
// right size via Stat — sufficient to confirm Put writes work.
func TestGCSPutThenStat(t *testing.T) {
	r := newStorage(t)
	body := []byte("the quick brown fox\n")

	putBytes(t, r, "subdir/file.bin", body)

	fi, err := r.Stat(context.Background(), "subdir/file.bin")
	if err != nil {
		t.Fatalf("Stat after Put: %v", err)
	}
	if fi.Size != int64(len(body)) {
		t.Errorf("Stat size = %d, want %d", fi.Size, len(body))
	}
}

func TestGCSStat(t *testing.T) {
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

	if _, err := r.Stat(context.Background(), "nope"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Stat missing should wrap os.ErrNotExist; got %v", err)
	}
}

func TestGCSGetMissing(t *testing.T) {
	r := newStorage(t)
	_, err := r.Get(context.Background(), "nope")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Get missing should wrap os.ErrNotExist; got %v", err)
	}
}

func TestGCSList(t *testing.T) {
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
}

func TestGCSCommitAtomicRename(t *testing.T) {
	r := newStorage(t)
	body := []byte("manifest content")
	putBytes(t, r, "manifest.tmp", body)

	if err := r.Commit(context.Background(), "manifest.tmp", "manifest"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Use Stat (HEAD-equivalent) instead of Get to confirm the rename;
	// the SDK's NewReader has a known bug against fake-gcs-server. Stat
	// hits the JSON-API metadata endpoint which works reliably.
	fi, err := r.Stat(context.Background(), "manifest")
	if err != nil {
		t.Fatalf("Stat(manifest) after Commit: %v", err)
	}
	if fi.Size != int64(len(body)) {
		t.Errorf("manifest size = %d, want %d", fi.Size, len(body))
	}

	if _, err := r.Stat(context.Background(), "manifest.tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("manifest.tmp still present after Commit; got %v", err)
	}
}

func TestGCSCommitRefusesOverwrite(t *testing.T) {
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
