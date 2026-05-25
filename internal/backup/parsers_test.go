package backup

import (
	"strings"
	"testing"
)

func TestParseBackupLabel(t *testing.T) {
	t.Parallel()
	in := `START WAL LOCATION: 0/3000028 (file 000000010000000000000003)
CHECKPOINT LOCATION: 0/3000060
BACKUP METHOD: streamed
BACKUP FROM: primary
START TIME: 2026-04-28 12:00:00 GMT
LABEL: pgsafe
START TIMELINE: 1
`
	lsn, tli, err := parseBackupLabel(in)
	if err != nil {
		t.Fatalf("parseBackupLabel: %v", err)
	}
	if got := lsn.String(); got != "0/3000028" {
		t.Errorf("LSN = %s, want 0/3000028", got)
	}
	if tli != 1 {
		t.Errorf("Timeline = %d, want 1", tli)
	}
}

func TestParseBackupLabelMissingFields(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		"BACKUP METHOD: streamed\n",
		"START WAL LOCATION: 0/3000028 (file ...)\n",   // missing timeline
		"START TIMELINE: 1\nBACKUP METHOD: streamed\n", // missing LSN
	}
	for _, in := range cases {
		t.Run(strings.SplitN(in, "\n", 2)[0], func(t *testing.T) {
			if _, _, err := parseBackupLabel(in); err == nil {
				t.Errorf("parseBackupLabel(%q): want error", in)
			}
		})
	}
}

func TestParseBackupLabelMultipleTimelines(t *testing.T) {
	t.Parallel()
	in := `START WAL LOCATION: ABCDEF12/34567890 (file ...)
START TIMELINE: 42
`
	lsn, tli, err := parseBackupLabel(in)
	if err != nil {
		t.Fatalf("parseBackupLabel: %v", err)
	}
	if tli != 42 {
		t.Errorf("Timeline = %d, want 42", tli)
	}
	if got := lsn.String(); got != "ABCDEF12/34567890" {
		t.Errorf("LSN = %s, want ABCDEF12/34567890", got)
	}
}
