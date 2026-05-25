package main

// Wires the restore package's Run into Worker.Restore. Lives in
// cmd/pgsafe to avoid the import cycle that would result from the
// worker package depending on restore directly. init() runs before
// any worker session accepts an RPC, so the function pointer is
// populated by the time the caller dials.

import (
	"context"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/restore"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
	"github.com/vyruss/pgsafe/internal/worker"
)

func init() {
	worker.SetRunRestore(runRestoreOnWorker)
}

func runRestoreOnWorker(
	ctx context.Context,
	backend storage.Backend,
	req *rpc.RestoreRequest,
	identities []age.Identity,
) (worker.RestoreResult, error) {
	res, err := restore.Run(ctx, restore.Options{
		Backend:        backend,
		Target:         req.TargetPath,
		Identities:     identities,
		BackupID:       req.BackupID,
		Workers:        req.Workers,
		StandbyMode:    req.StandbyMode,
		RestoreCommand: req.RestoreCommand,
	})
	if err != nil {
		return worker.RestoreResult{}, err
	}
	return worker.RestoreResult{
		BackupID: res.BackupID,
		Files:    res.Files,
		WAL:      res.WAL,
		Bytes:    res.Bytes,
	}, nil
}
