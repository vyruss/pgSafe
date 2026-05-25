package backup

import (
	"bytes"
	"strings"
	"testing"
)

// TestWantsSFTPProxy covers the dispatch helper that gates the
// SFTP-via-caller branch in runWorkerBackup. The matrix is small but
// the same-host short-circuit is the load-bearing part: same-host
// transport has no ssh session for `-R`, so via_caller must collapse to
// a no-op there rather than blowing up at dial time. wantsCloudProxy
// shares the same shape (different dispatch site, same gating).
func TestWantsSFTPProxy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		reach     string
		sshTarget string
		want      bool
	}{
		{"empty reach + ssh target", "", "user@pg", false},
		{"auto + ssh target", "auto", "user@pg", false},
		{"native_only + ssh target", "native_only", "user@pg", false},
		{"via_caller + ssh target", "via_caller", "user@pg", true},
		{"via_caller + same-host", "via_caller", "", false},
		{"empty reach + same-host", "", "", false},
		{"auto + same-host", "auto", "", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotSFTP := wantsSFTPProxy(tc.reach, tc.sshTarget)
			if gotSFTP != tc.want {
				t.Errorf("wantsSFTPProxy(%q, %q) = %v, want %v",
					tc.reach, tc.sshTarget, gotSFTP, tc.want)
			}
			gotCloud := wantsCloudProxy(tc.reach, tc.sshTarget)
			if gotCloud != tc.want {
				t.Errorf("wantsCloudProxy(%q, %q) = %v, want %v",
					tc.reach, tc.sshTarget, gotCloud, tc.want)
			}
		})
	}
}

// TestConfirmProxyFallback exercises the --confirm-proxy prompt: any
// "y"-prefixed answer means "yes, fall back to caller-proxy"; anything
// else (including EOF, which is what cron / systemd see) means "no,
// abort". This is the gate that prevents a non-interactive backup from
// silently changing its byte path when the operator opted in to
// confirmation.
func TestConfirmProxyFallback(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"lowercase y", "y\n", true},
		{"lowercase yes", "yes\n", true},
		{"uppercase Y", "Y\n", true},
		{"lowercase n", "n\n", false},
		{"empty newline", "\n", false},
		{"explicit no", "no\n", false},
		{"eof immediately", "", false},
		{"random char", "x\n", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var prompt bytes.Buffer
			got, err := confirmProxyFallback(strings.NewReader(tc.in), &prompt)
			if err != nil {
				t.Fatalf("confirmProxyFallback: %v", err)
			}
			if got != tc.want {
				t.Errorf("confirmProxyFallback(%q) = %v, want %v", tc.in, got, tc.want)
			}
			if !strings.Contains(prompt.String(), "fall back to caller-proxy") {
				t.Errorf("prompt missing expected text; got %q", prompt.String())
			}
		})
	}
}

// TestIsCloudStorageType pins the membership of the "cloud" set to
// exactly the three SDK-driven backends. POSIX and SFTP have other
// proxy paths (mount perms / ssh -R port forward respectively); the
// HTTPS_PROXY shape only applies to the SDK-mediated cloud APIs.
func TestIsCloudStorageType(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"s3":      true,
		"azure":   true,
		"gcs":     true,
		"sftp":    false,
		"posix":   false,
		"":        false,
		"unknown": false,
	}
	for in, want := range cases {
		if got := isCloudStorageType(in); got != want {
			t.Errorf("isCloudStorageType(%q) = %v, want %v", in, got, want)
		}
	}
}
