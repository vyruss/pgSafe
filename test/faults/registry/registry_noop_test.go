//go:build !faults

package registry_test

import (
	"errors"
	"testing"

	"github.com/vyruss/pgsafe/test/faults/registry"
)

// TestNoopBuildAlwaysReturnsNil pins the contract that production builds
// (without -tags=faults) ignore Arm and let every Trigger pass through.
// Without this guard a stray test-only Arm import in a production code
// path would silently turn into a panic risk under load.
func TestNoopBuildAlwaysReturnsNil(t *testing.T) {
	registry.Arm(registry.HookPreFsyncManifest, errors.New("would fire under -tags=faults"))
	if got := registry.Trigger(registry.HookPreFsyncManifest); got != nil {
		t.Fatalf("Trigger in no-op build = %v, want nil", got)
	}
	registry.Disarm(registry.HookPreFsyncManifest)
}
