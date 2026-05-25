//go:build !faults

package registry

// Trigger is the no-op production-build version. Code paths that consult
// the fault registry compile without any test machinery in normal builds
// — the call site goes away at link time when the inlined empty body is
// dead-code-eliminated.
func Trigger(_ Hook) error { return nil }

// Arm is a no-op outside the faults build tag. The faults-tagged variant
// in trigger_faults.go records the error so the next Trigger(hook) call
// returns it.
func Arm(_ Hook, _ error) {}

// Disarm clears any armed fault for the hook. No-op outside the faults
// build tag.
func Disarm(_ Hook) {}
