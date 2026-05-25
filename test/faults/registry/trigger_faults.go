//go:build faults

package registry

import "sync"

// armedFaults records the error each Hook is currently armed to return.
// A test calls Arm(hook, err); the next Trigger(hook) call returns err
// (and the entry is cleared). Concurrent calls are safe.
var (
	armedMu sync.Mutex
	armed   = map[Hook]error{}
)

// Trigger checks whether the named hook is currently armed. If so, the
// armed error is returned and cleared (a hook fires once per Arm). If
// not, returns nil and lets the production code path proceed.
//
// Production callers under -tags=faults pattern:
//
//	if err := registry.Trigger(registry.HookPreFsyncManifest); err != nil {
//	    return err
//	}
func Trigger(h Hook) error {
	armedMu.Lock()
	defer armedMu.Unlock()
	err, ok := armed[h]
	if !ok {
		return nil
	}
	delete(armed, h)
	return err
}

// Arm makes the next Trigger(h) call return err. Re-arming overwrites
// any previous arm for the same hook.
func Arm(h Hook, err error) {
	armedMu.Lock()
	defer armedMu.Unlock()
	armed[h] = err
}

// Disarm clears any armed fault for h, restoring the default no-op.
func Disarm(h Hook) {
	armedMu.Lock()
	defer armedMu.Unlock()
	delete(armed, h)
}
