//go:build integration_cloud

package sftp_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/cloudtest"
	"github.com/vyruss/pgsafe/internal/storage/sftp"
)

func newStorage(t *testing.T) *sftp.Backend {
	t.Helper()
	ep := cloudtest.StartSFTP(t)
	r, err := sftp.New(sftp.Options{
		Client:   cloudtest.NewSFTPClient(t, ep),
		BasePath: ep.BasePath,
	})
	if err != nil {
		t.Fatalf("sftp.New: %v", err)
	}
	if err := r.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return r
}

func putBytes(t *testing.T, r *sftp.Backend, rel string, body []byte) {
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

func TestSFTPPutRoundTrip(t *testing.T) {
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

func TestSFTPStat(t *testing.T) {
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

	if _, err := r.Stat(context.Background(), "nope"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Stat missing should wrap os.ErrNotExist; got %v", err)
	}
}

func TestSFTPGetMissing(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	_, err := r.Get(context.Background(), "nope")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Get missing should wrap os.ErrNotExist; got %v", err)
	}
}

func TestSFTPList(t *testing.T) {
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
}

func TestSFTPCommitAtomicRename(t *testing.T) {
	t.Parallel()
	r := newStorage(t)
	body := []byte("manifest content")
	putBytes(t, r, "manifest.tmp", body)

	if err := r.Commit(context.Background(), "manifest.tmp", "manifest"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rc, err := r.Get(context.Background(), "manifest")
	if err != nil {
		t.Fatalf("Get(manifest): %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, body) {
		t.Errorf("manifest content mismatch")
	}

	if _, err := r.Stat(context.Background(), "manifest.tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("manifest.tmp still present after Commit; got %v", err)
	}
}

func TestSFTPCommitRefusesOverwrite(t *testing.T) {
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
