// Package cloudtest spins up cloud-storage emulators (MinIO for S3, Azurite
// for Azure Blob, fake-gcs-server for GCS, OpenSSH for SFTP) under
// testcontainers-go for use in pgSafe integration tests.
//
// Each StartXxx(t) helper:
//   - launches the container,
//   - registers cleanup via t.Cleanup,
//   - returns the connection details an SDK needs to reach the emulator
//     (URL, credentials, default bucket / container / path).
//
// IMPORTANT: tests using these helpers should carry the `integration_cloud`
// build tag so PR-time CI (which runs with cloud emulators disabled) doesn't
// pull every image on every push. See run-ci-local.sh step 7.5.
package cloudtest

// Endpoint is what every StartXxx returns: enough information to instantiate
// the SDK client. Each cloud has its own concrete struct embedding common
// fields where it makes sense (URL, default container/bucket).
//
// Defined here only as a documentation anchor; per-cloud helpers return their
// own struct types because the credential shape differs.
