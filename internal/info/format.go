package info

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

// FormatTable writes a fixed-width table of records to w. Used by
// `pgsafe info` (default output) and reusable by any other command
// that wants the same human-readable layout.
//
// Columns: ID, TYPE, PARENT, SERVER, SIZE, FILES, AGE, ANNOTATION.
// Empty record list emits a single "(no backups)" line so operators
// don't have to second-guess whether the command ran.
func FormatTable(w io.Writer, records []BackupRecord, warnings []Warning) error {
	if len(records) == 0 && len(warnings) == 0 {
		_, err := fmt.Fprintln(w, "(no backups)")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tTYPE\tPARENT\tSERVER\tSIZE\tFILES\tAGE\tANNOTATION"); err != nil {
		return err
	}
	for _, r := range records {
		parent := r.ParentBackupID
		if parent == "" {
			parent = "-"
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			r.BackupID, r.Type, parent, r.Server,
			humanBytes(r.Bytes), r.Files, humanAge(r.Age()), r.Annotation,
		); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, wn := range warnings {
		if _, err := fmt.Fprintln(w, wn.String()); err != nil {
			return err
		}
	}
	return nil
}

// FormatJSON writes records + warnings as one JSON object with two
// top-level keys: `backups` and `warnings`. Stable schema, monitoring-
// friendly.
func FormatJSON(w io.Writer, records []BackupRecord, warnings []Warning) error {
	type jsonWarning struct {
		BackupID string `json:"backup_id"`
		Error    string `json:"error"`
	}
	jws := make([]jsonWarning, 0, len(warnings))
	for _, wn := range warnings {
		jws = append(jws, jsonWarning{BackupID: wn.BackupID, Error: wn.Err.Error()})
	}
	if records == nil {
		records = []BackupRecord{}
	}
	out := struct {
		Backups  []BackupRecord `json:"backups"`
		Warnings []jsonWarning  `json:"warnings"`
	}{Backups: records, Warnings: jws}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// humanBytes renders a byte count as the largest unit ≥ 1 (KiB / MiB /
// GiB / TiB / PiB), with one decimal of precision past the unit
// boundary. Zero or negative inputs return "-".
func humanBytes(n int64) string {
	if n <= 0 {
		return "-"
	}
	const k = 1024.0
	v := float64(n)
	switch {
	case v < k:
		return fmt.Sprintf("%d B", n)
	case v < k*k:
		return fmt.Sprintf("%.1f KiB", v/k)
	case v < k*k*k:
		return fmt.Sprintf("%.1f MiB", v/(k*k))
	case v < k*k*k*k:
		return fmt.Sprintf("%.1f GiB", v/(k*k*k))
	case v < k*k*k*k*k:
		return fmt.Sprintf("%.1f TiB", v/(k*k*k*k))
	default:
		return fmt.Sprintf("%.1f PiB", v/(k*k*k*k*k))
	}
}

// humanAge renders a duration since-completion in operator-friendly
// units. 0 → "-".
func humanAge(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d < 365*24*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	default:
		return fmt.Sprintf("%dy", int(d.Hours()/(24*365)))
	}
}
