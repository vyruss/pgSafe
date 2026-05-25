package main

import (
	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
)

// resolveSSHReach picks the effective SSH target + raw extra-args string,
// applying CLI > YAML > unset precedence. The CLI flag wins whenever it
// was set on the command line (even to the empty string); otherwise the
// caller-relative YAML defaults apply.
func resolveSSHReach(cmd *cobra.Command, cfg *config.Config) (target, extraArgsRaw string) {
	target, _ = cmd.Flags().GetString("ssh-target")
	if !cmd.Flags().Changed("ssh-target") {
		target = cfg.PG.Host
	}
	extraArgsRaw, _ = cmd.Flags().GetString("ssh-extra-args")
	if !cmd.Flags().Changed("ssh-extra-args") {
		extraArgsRaw = cfg.PG.SSHExtraArgs
	}
	return target, extraArgsRaw
}
