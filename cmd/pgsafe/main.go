// Command pgsafe is the pgSafe CLI entry point. The root and subcommand wiring
// lives in root.go so tests can build a fresh command tree without touching
// process state.
package main

import (
	"os"
)

func main() {
	root := newRootCmd()
	err := root.Execute()
	os.Exit(errExit(err))
}
