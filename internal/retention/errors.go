package retention

import "errors"

// ErrEmptyPolicy is returned by Evaluate when the policy has no
// non-zero fields. A zero policy would mark every backup expirable,
// which is almost certainly an operator error rather than intent;
// callers refuse to act on it and surface a clear "specify at least
// one keep_*" message.
var ErrEmptyPolicy = errors.New("retention: empty policy (specify at least one keep_* rule)")
