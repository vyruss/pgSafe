// Package log is a thin wrapper around log/slog that locks pgSafe to a known
// format set (json|text) and provides string→slog.Level parsing for the
// config layer.
package log

import (
	"fmt"
	"io"
	"log/slog"
)

// New constructs a *slog.Logger for the given format and minimum level,
// writing to w. format must be "json" or "text".
func New(format string, level slog.Level, w io.Writer) (*slog.Logger, error) {
	opts := &slog.HandlerOptions{Level: level}
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("log: unknown format %q (want json|text)", format)
	}
}

// ParseLevel converts the YAML/CLI level strings to slog.Level. The input set
// is identical to config's validLogLevels and locked.
func ParseLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("log: unknown level %q (want debug|info|warn|error)", s)
	}
}
