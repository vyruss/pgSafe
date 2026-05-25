//go:build faults

package registry_test

import (
	"errors"
	"testing"

	"github.com/vyruss/pgsafe/test/faults/registry"
)

// TestArmFiresOnceThenClears asserts the documented one-shot semantics:
// Arm → Trigger returns the error → Trigger returns nil thereafter.
func TestArmFiresOnceThenClears(t *testing.T) {
	want := errors.New("injected")
	registry.Arm(registry.HookPreFsyncManifest, want)
	if got := registry.Trigger(registry.HookPreFsyncManifest); !errors.Is(got, want) {
		t.Fatalf("Trigger after Arm = %v, want %v", got, want)
	}
	if got := registry.Trigger(registry.HookPreFsyncManifest); got != nil {
		t.Errorf("Trigger second time = %v, want nil (one-shot)", got)
	}
}

func TestDisarmClearsArmedFault(t *testing.T) {
	registry.Arm(registry.HookMidBaseBackupTar, errors.New("never fires"))
	registry.Disarm(registry.HookMidBaseBackupTar)
	if got := registry.Trigger(registry.HookMidBaseBackupTar); got != nil {
		t.Errorf("Trigger after Disarm = %v, want nil", got)
	}
}

func TestTriggerUnarmedHookIsNil(t *testing.T) {
	if got := registry.Trigger(registry.HookSingleStorageOutage); got != nil {
		t.Errorf("Trigger unarmed = %v, want nil", got)
	}
}

func TestAllHooksUnique(t *testing.T) {
	seen := map[registry.Hook]struct{}{}
	for _, h := range registry.AllHooks() {
		if _, dup := seen[h]; dup {
			t.Errorf("duplicate hook in AllHooks: %q", h)
		}
		seen[h] = struct{}{}
	}
}
