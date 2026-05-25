package log_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	pglog "github.com/vyruss/pgsafe/internal/log"
)

func TestNewJSONEmitsValidJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := pglog.New("json", slog.LevelInfo, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("hello", "key", "value")

	out := buf.String()
	if !strings.Contains(out, `"msg":"hello"`) {
		t.Errorf("JSON output missing msg=hello; got %q", out)
	}
	if !strings.Contains(out, `"key":"value"`) {
		t.Errorf("JSON output missing key=value; got %q", out)
	}
}

func TestNewTextEmitsKeyEqualsValue(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := pglog.New("text", slog.LevelInfo, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("hello", "key", "value")

	out := buf.String()
	if !strings.Contains(out, "msg=hello") {
		t.Errorf("text output missing msg=hello; got %q", out)
	}
	if !strings.Contains(out, "key=value") {
		t.Errorf("text output missing key=value; got %q", out)
	}
}

func TestNewRejectsUnknownFormat(t *testing.T) {
	t.Parallel()

	_, err := pglog.New("xml", slog.LevelInfo, &bytes.Buffer{})
	if err == nil {
		t.Fatal("New(xml): expected error, got nil")
	}
}

func TestNewRespectsLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := pglog.New("text", slog.LevelWarn, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("filtered")
	logger.Warn("emitted")

	out := buf.String()
	if strings.Contains(out, "filtered") {
		t.Errorf("info-level message leaked at warn level: %q", out)
	}
	if !strings.Contains(out, "emitted") {
		t.Errorf("warn-level message missing: %q", out)
	}
}

func TestParseLevelKnownValues(t *testing.T) {
	t.Parallel()

	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for s, want := range cases {
		got, err := pglog.ParseLevel(s)
		if err != nil {
			t.Errorf("ParseLevel(%q): unexpected error: %v", s, err)
			continue
		}
		if got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestParseLevelRejectsUnknown(t *testing.T) {
	t.Parallel()

	_, err := pglog.ParseLevel("trace")
	if err == nil {
		t.Fatal("ParseLevel(trace): expected error, got nil")
	}
}
