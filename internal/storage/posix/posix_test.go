package posix_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

func newStorage(t *testing.T) (*posix.Backend, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "storage")
	r, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := r.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return r, root
}

func writeFile(t *testing.T, r storage.Backend, rel string, content []byte) {
	t.Helper()
	wc, err := r.Put(context.Background(), rel)
	if err != nil {
		t.Fatalf("Put(%s): %v", rel, err)
	}
	if _, err := wc.Write(content); err != nil {
		t.Fatalf("Write(%s): %v", rel, err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close(%s): %v", rel, err)
	}
}

func readFile(t *testing.T, r storage.Backend, rel string) []byte {
	t.Helper()
	rc, err := r.Get(context.Background(), rel)
	if err != nil {
		t.Fatalf("Get(%s): %v", rel, err)
	}
	defer func() { _ = rc.Close() }()
	out, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%s): %v", rel, err)
	}
	return out
}

func TestOpenCreatesRootIfMissing(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "deep", "nested", "storage")
	r, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := r.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("root not created: %v", err)
	}
	// And the wal/ sibling.
	if _, err := os.Stat(filepath.Join(root, "wal")); err != nil {
		t.Fatalf("wal/ subdir not created: %v", err)
	}
}

func TestOpenIdempotent(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	if err := r.Open(context.Background()); err != nil {
		t.Errorf("re-Open: %v", err)
	}
}

func TestPutRoundTrip(t *testing.T) {
	t.Parallel()
	r, root := newStorage(t)
	content := []byte("the quick brown fox\n")

	writeFile(t, r, "subdir/file.bin", content)
	got := readFile(t, r, "subdir/file.bin")
	if string(got) != string(content) {
		t.Errorf("roundtrip mismatch: got %q, want %q", got, content)
	}

	// On-disk path matches the relPath.
	abs := filepath.Join(root, "subdir/file.bin")
	if _, err := os.Stat(abs); err != nil {
		t.Errorf("file not visible at %s: %v", abs, err)
	}
}

func TestPutLeavesNoTempBehind(t *testing.T) {
	t.Parallel()
	r, root := newStorage(t)
	writeFile(t, r, "f.bin", []byte("x"))

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".pgsafe-tmp") {
			t.Errorf("tmp leaked: %s", e.Name())
		}
	}
}

func TestStatReportsSize(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	content := []byte("twelve bytes")
	writeFile(t, r, "x", content)

	fi, err := r.Stat(context.Background(), "x")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size != int64(len(content)) {
		t.Errorf("Size = %d, want %d", fi.Size, len(content))
	}
	if fi.Path != "x" {
		t.Errorf("Path = %q, want %q", fi.Path, "x")
	}
}

func TestStatMissingFile(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	_, err := r.Stat(context.Background(), "nope")
	if err == nil {
		t.Fatal("Stat on missing file: want error")
	}
}

func TestList(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	for _, rel := range []string{"a", "sub/b", "sub/c", "deep/sub/d"} {
		writeFile(t, r, rel, []byte(rel))
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
	if !equalStrings(got, want) {
		t.Errorf("List(\"\") = %v, want %v", got, want)
	}

	subOnly, _ := r.List(context.Background(), "sub")
	if len(subOnly) != 2 {
		t.Errorf("List(\"sub\") returned %d entries, want 2", len(subOnly))
	}
}

func TestCommitAtomicRename(t *testing.T) {
	t.Parallel()
	r, root := newStorage(t)
	writeFile(t, r, "manifest.tmp", []byte("manifest content"))

	if err := r.Commit(context.Background(), "manifest.tmp", "manifest"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// .tmp gone, final present.
	if _, err := os.Stat(filepath.Join(root, "manifest.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("manifest.tmp still exists after Commit")
	}
	if _, err := os.Stat(filepath.Join(root, "manifest")); err != nil {
		t.Errorf("manifest missing after Commit: %v", err)
	}
}

func TestCommitRefusesOverwrite(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	writeFile(t, r, "manifest.tmp", []byte("new"))
	writeFile(t, r, "manifest", []byte("existing"))

	err := r.Commit(context.Background(), "manifest.tmp", "manifest")
	if err == nil {
		t.Fatal("Commit overwriting existing final: want error")
	}
}

func TestPutRejectsAbsolutePath(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	_, err := r.Put(context.Background(), "/etc/passwd")
	if err == nil {
		t.Fatal("Put with absolute path: want error")
	}
}

func TestPutRejectsTraversal(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	_, err := r.Put(context.Background(), "../escape")
	if err == nil {
		t.Fatal("Put with traversal: want error")
	}
}

func TestDeleteRoundTrip(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	writeFile(t, r, "foo/bar", []byte("hello"))

	if err := r.Delete(context.Background(), "foo/bar"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Stat(context.Background(), "foo/bar"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Stat after Delete: want os.ErrNotExist, got %v", err)
	}
}

func TestDeleteMissingWrapsErrNotExist(t *testing.T) {
	t.Parallel()
	r, _ := newStorage(t)
	err := r.Delete(context.Background(), "never-existed")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Delete missing: want os.ErrNotExist, got %v", err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
