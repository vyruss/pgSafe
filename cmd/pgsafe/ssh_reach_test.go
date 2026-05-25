package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
)

func newCmdWithSSHFlags() *cobra.Command {
	c := &cobra.Command{Use: "test"}
	c.Flags().String("ssh-target", "", "")
	c.Flags().String("ssh-extra-args", "", "")
	return c
}

func TestResolveSSHReachYAMLOnly(t *testing.T) {
	t.Parallel()
	cmd := newCmdWithSSHFlags()
	cfg := &config.Config{PG: config.PGConfig{
		Host:         "pgsafe@pg.example.com",
		SSHExtraArgs: "-p 2222",
	}}

	target, extra := resolveSSHReach(cmd, cfg)
	if target != "pgsafe@pg.example.com" {
		t.Errorf("target = %q, want YAML value", target)
	}
	if extra != "-p 2222" {
		t.Errorf("extra = %q, want YAML value", extra)
	}
}

func TestResolveSSHReachCLIOverridesYAML(t *testing.T) {
	t.Parallel()
	cmd := newCmdWithSSHFlags()
	if err := cmd.Flags().Set("ssh-target", "pgsafe@cli.example.com"); err != nil {
		t.Fatalf("set ssh-target: %v", err)
	}
	if err := cmd.Flags().Set("ssh-extra-args", "-p 9999"); err != nil {
		t.Fatalf("set ssh-extra-args: %v", err)
	}
	cfg := &config.Config{PG: config.PGConfig{
		Host:         "pgsafe@yaml.example.com",
		SSHExtraArgs: "-p 1111",
	}}

	target, extra := resolveSSHReach(cmd, cfg)
	if target != "pgsafe@cli.example.com" {
		t.Errorf("target = %q, want CLI value", target)
	}
	if extra != "-p 9999" {
		t.Errorf("extra = %q, want CLI value", extra)
	}
}

// CLI explicit empty (--ssh-target= without value) still wins over the
// YAML default — operators sometimes need to force "no SSH target" on
// a host whose YAML declares one.
func TestResolveSSHReachCLIExplicitEmptyWinsOverYAML(t *testing.T) {
	t.Parallel()
	cmd := newCmdWithSSHFlags()
	if err := cmd.Flags().Set("ssh-target", ""); err != nil {
		t.Fatalf("set ssh-target='': %v", err)
	}
	cfg := &config.Config{PG: config.PGConfig{
		Host: "pgsafe@yaml.example.com",
	}}

	target, _ := resolveSSHReach(cmd, cfg)
	if target != "" {
		t.Errorf("target = %q, want CLI explicit empty", target)
	}
}

func TestResolveSSHReachBothUnset(t *testing.T) {
	t.Parallel()
	cmd := newCmdWithSSHFlags()
	cfg := &config.Config{}

	target, extra := resolveSSHReach(cmd, cfg)
	if target != "" || extra != "" {
		t.Errorf("got (%q, %q), want zero values", target, extra)
	}
}
