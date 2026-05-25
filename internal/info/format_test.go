package info_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

func TestFormatTableEmpty(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := info.FormatTable(&buf, nil, nil); err != nil {
		t.Fatalf("FormatTable: %v", err)
	}
	if !strings.Contains(buf.String(), "no backups") {
		t.Errorf("empty FormatTable output should mention 'no backups'; got %q", buf.String())
	}
}

func TestFormatTableHeaderAndRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	bb := backuptest.New(ctx, b, "demo")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	full1 := bb.AddFull(t0, "")
	bb.AddIncremental(full1, t0.Add(2*time.Hour), "RC1")

	records, warnings, err := info.List(ctx, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var buf bytes.Buffer
	if err := info.FormatTable(&buf, records, warnings); err != nil {
		t.Fatalf("FormatTable: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "ID") || !strings.Contains(out, "TYPE") || !strings.Contains(out, "ANNOTATION") {
		t.Errorf("table missing header columns; got %q", out)
	}
	if !strings.Contains(out, "demo") {
		t.Errorf("table missing server name 'demo'; got %q", out)
	}
	if !strings.Contains(out, "RC1") {
		t.Errorf("table missing annotation 'RC1'; got %q", out)
	}
	if !strings.Contains(out, "incremental") {
		t.Errorf("table missing 'incremental' row; got %q", out)
	}
}

func TestFormatJSONStrictDecode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	bb := backuptest.New(ctx, b, "demo")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")

	records, warnings, err := info.List(ctx, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var buf bytes.Buffer
	if err := info.FormatJSON(&buf, records, warnings); err != nil {
		t.Fatalf("FormatJSON: %v", err)
	}

	dec := json.NewDecoder(&buf)
	dec.DisallowUnknownFields()
	var got struct {
		Backups  []info.BackupRecord `json:"backups"`
		Warnings []struct {
			BackupID string `json:"backup_id"`
			Error    string `json:"error"`
		} `json:"warnings"`
	}
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("strict json.Decode: %v", err)
	}
	if len(got.Backups) != 1 {
		t.Errorf("backups count = %d, want 1", len(got.Backups))
	}
	if got.Backups[0].Server != "demo" {
		t.Errorf("backups[0].Server = %q, want %q", got.Backups[0].Server, "demo")
	}
}

func TestFormatJSONEmptyStorageIsValidJSON(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := info.FormatJSON(&buf, nil, nil); err != nil {
		t.Fatalf("FormatJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("json.Unmarshal: %v\noutput: %s", err, buf.String())
	}
	// `backups` must be present as an empty array, not null.
	bs, ok := got["backups"].([]any)
	if !ok {
		t.Errorf("backups missing or wrong type: %T", got["backups"])
	}
	if len(bs) != 0 {
		t.Errorf("backups should be empty for empty storage; got %v", bs)
	}
}
