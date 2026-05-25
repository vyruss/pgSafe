// Package retention is the pure-logic policy evaluator for `pgsafe
// prune`. Given a list of BackupRecord (from `internal/info`) and a
// Policy, Evaluate returns the set of expirable backup IDs and the
// oldest WAL LSN any surviving backup still needs.
//
// The evaluator is deliberately I/O-free: it reads records, returns a
// Plan, and that's it. The CLI does all storage-backend work after.
//
// Key invariant: incremental chains are atomic. A full F survives until
// every incremental that descends from it (transitively) is itself
// expirable. The reverse: deleting F also requires deleting every
// descendant. The policy checks below operate at the per-chain level —
// a chain is "kept" if any of its backups satisfies a keep rule.
package retention

import (
	"sort"
	"time"

	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/manifest"
)

// Policy is the YAML-configured retention rules. Zero values mean
// "rule is disabled" — KeepFulls=0 does NOT mean "expire everything";
// it means the rule isn't engaged. At least one rule must produce
// kept chains, otherwise Evaluate refuses to proceed (returns
// ErrEmptyPolicy) — see PolicyValidate.
type Policy struct {
	// KeepFulls keeps the N most recent chains (by their full's
	// StopTime). 0 disables.
	KeepFulls int

	// KeepFullAge keeps chains whose full is younger than this
	// duration. 0 disables.
	KeepFullAge time.Duration

	// KeepDaily / KeepWeekly / KeepMonthly: keep one chain per
	// calendar day / ISO week / month for the past N units. 0 disables.
	KeepDaily   int
	KeepWeekly  int
	KeepMonthly int

	// Now is the reference "now" for age-based rules. Zero defaults
	// to time.Now() at Evaluate time. Tests inject a fixed value so
	// keep_daily and keep_full_age are deterministic.
	Now time.Time
}

// Plan is what Evaluate produces.
type Plan struct {
	// ExpirableBackupIDs is the set of backup IDs the operator may
	// delete. Sorted for determinism.
	ExpirableBackupIDs []string

	// OldestNeededLSN is the minimum start-LSN across surviving
	// backups. WAL segments ending before this LSN are unreachable
	// from any kept restore-point and may be pruned. Zero if no
	// backups survive (operator should not blindly prune WAL in
	// that case — Evaluate's zero-survivor case is itself a red
	// flag covered by ErrAllExpirable).
	OldestNeededLSN manifest.LSN

	// KeptBackupIDs is the complement of ExpirableBackupIDs (sorted).
	// Returned for operator-side display and for the WAL-pruning
	// pass.
	KeptBackupIDs []string
}

// Validate checks the policy is non-trivial. A zeroed policy would
// expire every backup which is almost certainly an operator error;
// callers should refuse to act on a zero policy.
func (p Policy) Validate() error {
	if p.KeepFulls == 0 && p.KeepFullAge == 0 && p.KeepDaily == 0 && p.KeepWeekly == 0 && p.KeepMonthly == 0 {
		return ErrEmptyPolicy
	}
	return nil
}

// Evaluate runs the policy against the records. Records may arrive in
// any order; Evaluate sorts internally. The returned Plan is
// deterministic for the same (records, policy, Now) triple.
func Evaluate(records []info.BackupRecord, policy Policy) (Plan, error) {
	if err := policy.Validate(); err != nil {
		return Plan{}, err
	}
	now := policy.Now
	if now.IsZero() {
		now = time.Now()
	}

	chains := groupChains(records)
	keep := map[string]bool{} // chain key (full's BackupID) -> kept

	// keep_fulls: most-recent N chains by their full's StopTime.
	if policy.KeepFulls > 0 {
		ordered := orderedChainKeys(chains, byNewestFullStopTime(chains))
		for i := 0; i < len(ordered) && i < policy.KeepFulls; i++ {
			keep[ordered[i]] = true
		}
	}
	// keep_full_age: chains whose full is younger than the cutoff.
	if policy.KeepFullAge > 0 {
		cutoff := now.Add(-policy.KeepFullAge)
		for k, ch := range chains {
			if !ch.full.StopTime.IsZero() && ch.full.StopTime.After(cutoff) {
				keep[k] = true
			}
		}
	}
	// keep_daily/weekly/monthly: one slot per bucket for the last N
	// buckets, picking the most recent chain per bucket.
	if policy.KeepDaily > 0 {
		applyBucketRule(chains, keep, policy.KeepDaily, dayBucket(now))
	}
	if policy.KeepWeekly > 0 {
		applyBucketRule(chains, keep, policy.KeepWeekly, weekBucket(now))
	}
	if policy.KeepMonthly > 0 {
		applyBucketRule(chains, keep, policy.KeepMonthly, monthBucket(now))
	}

	var (
		expirable []string
		kept      []string
		minLSN    manifest.LSN
		anyKept   bool
	)
	for k, ch := range chains {
		if keep[k] {
			for _, r := range ch.all() {
				kept = append(kept, r.BackupID)
			}
			lsn, _ := manifest.ParseLSN(ch.full.StartLSN)
			if !anyKept || lsn < minLSN {
				minLSN = lsn
				anyKept = true
			}
		} else {
			for _, r := range ch.all() {
				expirable = append(expirable, r.BackupID)
			}
		}
	}
	sort.Strings(expirable)
	sort.Strings(kept)
	return Plan{
		ExpirableBackupIDs: expirable,
		KeptBackupIDs:      kept,
		OldestNeededLSN:    minLSN,
	}, nil
}

// chain groups one full and its descendant incrementals.
type chain struct {
	full         info.BackupRecord
	incrementals []info.BackupRecord
}

func (c chain) all() []info.BackupRecord {
	out := make([]info.BackupRecord, 0, 1+len(c.incrementals))
	out = append(out, c.full)
	out = append(out, c.incrementals...)
	return out
}

// groupChains buckets records into chains keyed by the full's
// BackupID. Orphaned incrementals (parent full is missing) are
// silently dropped from the planning view — `pgsafe info` would have
// reported them as warnings, and `prune` refuses to delete what it
// can't decode.
func groupChains(records []info.BackupRecord) map[string]chain {
	fulls := map[string]info.BackupRecord{}
	for _, r := range records {
		if r.Type == manifest.BackupTypeFull {
			fulls[r.BackupID] = r
		}
	}
	chains := map[string]chain{}
	for id, f := range fulls {
		chains[id] = chain{full: f}
	}
	for _, r := range records {
		if r.Type != manifest.BackupTypeIncremental {
			continue
		}
		root := rootOf(r, records)
		if root == "" {
			continue
		}
		c := chains[root]
		c.incrementals = append(c.incrementals, r)
		chains[root] = c
	}
	return chains
}

// rootOf walks parent pointers up the chain until it hits a full.
// Empty result means the chain is orphaned (parent isn't in records).
func rootOf(r info.BackupRecord, all []info.BackupRecord) string {
	byID := map[string]info.BackupRecord{}
	for _, rr := range all {
		byID[rr.BackupID] = rr
	}
	cur := r
	for i := 0; i < len(all)+1; i++ { // bounded iter to avoid loops
		if cur.Type == manifest.BackupTypeFull {
			return cur.BackupID
		}
		parent, ok := byID[cur.ParentBackupID]
		if !ok {
			return ""
		}
		cur = parent
	}
	return ""
}

// byNewestFullStopTime returns a less-fn that orders chain keys by
// the chain's full StopTime, newest first. Stable; ties broken by
// BackupID lex order.
func byNewestFullStopTime(chains map[string]chain) func(a, b string) bool {
	return func(a, b string) bool {
		ta, tb := chains[a].full.StopTime, chains[b].full.StopTime
		if !ta.Equal(tb) {
			return ta.After(tb)
		}
		return chains[a].full.BackupID < chains[b].full.BackupID
	}
}

func orderedChainKeys(chains map[string]chain, less func(a, b string) bool) []string {
	keys := make([]string, 0, len(chains))
	for k := range chains {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return less(keys[i], keys[j]) })
	return keys
}

// applyBucketRule: for each bucket boundary back N steps from now,
// keep the most-recent chain that falls into that bucket.
func applyBucketRule(chains map[string]chain, keep map[string]bool, n int, bucket bucketFunc) {
	if n <= 0 {
		return
	}
	type chainEntry struct {
		key string
		t   time.Time
	}
	byBucket := map[int][]chainEntry{}
	for k, ch := range chains {
		t := ch.full.StopTime
		if t.IsZero() {
			continue
		}
		idx := bucket(t)
		if idx < 0 || idx >= n {
			continue
		}
		byBucket[idx] = append(byBucket[idx], chainEntry{k, t})
	}
	for _, entries := range byBucket {
		// pick newest in the bucket
		best := entries[0]
		for _, e := range entries[1:] {
			if e.t.After(best.t) {
				best = e
			}
		}
		keep[best.key] = true
	}
}

// bucketFunc maps a backup time to a bucket index in [0, N) where 0
// is "this bucket" and N-1 is "N-1 buckets ago." Returns negative
// for backups outside the window.
type bucketFunc func(time.Time) int

func dayBucket(now time.Time) bucketFunc {
	today := truncateToDay(now)
	return func(t time.Time) int {
		days := int(today.Sub(truncateToDay(t)).Hours() / 24)
		return days
	}
}

func weekBucket(now time.Time) bucketFunc {
	thisWeek := truncateToWeek(now)
	return func(t time.Time) int {
		weeks := int(thisWeek.Sub(truncateToWeek(t)).Hours() / (24 * 7))
		return weeks
	}
}

func monthBucket(now time.Time) bucketFunc {
	return func(t time.Time) int {
		return monthsBetween(t, now)
	}
}

func truncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func truncateToWeek(t time.Time) time.Time {
	d := truncateToDay(t)
	wd := int(d.Weekday()) // Sunday=0
	return d.AddDate(0, 0, -wd)
}

func monthsBetween(t, now time.Time) int {
	return (now.Year()-t.Year())*12 + int(now.Month()-t.Month())
}
