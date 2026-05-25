package main

// `pgsafe worker --stdio` is the PG-host side of hybrid-parallel mode. It is
// NEVER invoked by operators directly; the caller's SSH session
// spawns it as the remote command and speaks JSON-RPC over the pair's
// stdio. Running it interactively does nothing useful — it sits waiting
// for the caller's RPC frames on stdin and exits when stdin closes.
//
// Hidden from `--help` output to discourage accidental use.

import (
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
	"github.com/vyruss/pgsafe/internal/worker"
)

func newWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "worker",
		Short:  "Internal: PG-host JSON-RPC worker (invoked by hybrid-parallel caller)",
		Hidden: true,
	}
	cmd.AddCommand(newWorkerStdioCmd())
	return cmd
}

func newWorkerStdioCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stdio",
		Short: "Internal: serve a JSON-RPC worker over stdin/stdout",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pgdata := os.Getenv("PGDATA")
			impl := worker.New(version, pgdata)
			conn := stdioConn{in: cmd.InOrStdin(), out: cmd.OutOrStdout()}
			return rpc.Serve(conn, impl)
		},
	}
}

// stdioConn pairs stdin and stdout into one io.ReadWriteCloser for the
// JSON-RPC server codec. Close() is a no-op because stdin/stdout are owned
// by the process, not by us; the caller closes the SSH session to
// signal EOF, which makes Read return io.EOF and the serve loop exits.
type stdioConn struct {
	in  io.Reader
	out io.Writer
}

func (s stdioConn) Read(p []byte) (int, error)  { return s.in.Read(p) }
func (s stdioConn) Write(p []byte) (int, error) { return s.out.Write(p) }
func (stdioConn) Close() error                  { return nil }
