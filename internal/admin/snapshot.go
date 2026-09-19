// Package admin is the operator surface: the Snapshot counters that feed both
// the OTel meter and the hand-written Prometheus text exposer (so the two
// cannot disagree), plus /metrics, /admin/stats, /healthz and /readyz.
//
// Owner: workstream C (service edge). Snapshot's method set is called by
// workstream B's stores and must stay nil-receiver safe.
package admin

import (
	"sync/atomic"
	"time"
)

// Snapshot holds the process-lifetime counters and gauges. Every method is a
// no-op on a nil receiver so tests can pass nil. Labelled series
// (tenant, policy, allowed, backend) are bounded by the policy file; the
// subject is never a label (docs/PLAN.md, "Label cardinality").
//
// STUB: the atomics below are the unlabelled totals; the labelled maps,
// the latency histogram and the fallback timer land in step 9.
type Snapshot struct {
	allowed       atomic.Uint64
	denied        atomic.Uint64
	backendErrors atomic.Uint64
	scriptLoads   atomic.Uint64
	reloadErrors  atomic.Uint64
	localEvicted  atomic.Uint64
	localKeys     atomic.Int64
	clockSkewUS   atomic.Int64
	fallback      atomic.Bool
}

// NewSnapshot returns an empty Snapshot.
func NewSnapshot() *Snapshot { return &Snapshot{} }

// RecordDecision bumps rl_decisions_total{tenant,policy,allowed,backend} and
// observes latency into rl_decision_latency_seconds.
func (s *Snapshot) RecordDecision(_, _ string, allowed bool, _ string, _ time.Duration) {
	if s == nil {
		return
	}
	if allowed {
		s.allowed.Add(1)
	} else {
		s.denied.Add(1)
	}
}

// RecordBackendError bumps rl_backend_errors_total{backend,kind}.
func (s *Snapshot) RecordBackendError(_, _ string) {
	if s == nil {
		return
	}
	s.backendErrors.Add(1)
}

// SetFallbackActive sets rl_fallback_active{backend} and accumulates
// rl_fallback_seconds_total while active.
func (s *Snapshot) SetFallbackActive(_ string, active bool) {
	if s == nil {
		return
	}
	s.fallback.Store(active)
}

// AddScriptLoad bumps rl_redis_script_loads_total.
func (s *Snapshot) AddScriptLoad() {
	if s == nil {
		return
	}
	s.scriptLoads.Add(1)
}

// SetPolicyVersion sets rl_policy_version_info{version}.
func (s *Snapshot) SetPolicyVersion(_ string) {}

// AddPolicyReloadError bumps rl_policy_reload_errors_total.
func (s *Snapshot) AddPolicyReloadError() {
	if s == nil {
		return
	}
	s.reloadErrors.Add(1)
}

// SetClockSkew sets rl_clock_skew_seconds.
func (s *Snapshot) SetClockSkew(d time.Duration) {
	if s == nil {
		return
	}
	s.clockSkewUS.Store(d.Microseconds())
}

// SetLocalKeys sets rl_local_keys.
func (s *Snapshot) SetLocalKeys(n int64) {
	if s == nil {
		return
	}
	s.localKeys.Store(n)
}

// AddLocalEvicted bumps rl_local_evicted_total by n.
func (s *Snapshot) AddLocalEvicted(n int64) {
	if s == nil || n <= 0 {
		return
	}
	s.localEvicted.Add(uint64(n))
}

// Counters is the unlabelled view used by /admin/stats and AdminService.Stats.
type Counters struct {
	Allowed        uint64
	Denied         uint64
	BackendErrors  uint64
	ScriptLoads    uint64
	ReloadErrors   uint64
	LocalKeys      int64
	LocalEvicted   uint64
	ClockSkew      time.Duration
	FallbackActive bool
	FallbackTotal  time.Duration
}

// Counters reads every total at once.
func (s *Snapshot) Counters() Counters {
	if s == nil {
		return Counters{}
	}
	return Counters{
		Allowed:        s.allowed.Load(),
		Denied:         s.denied.Load(),
		BackendErrors:  s.backendErrors.Load(),
		ScriptLoads:    s.scriptLoads.Load(),
		ReloadErrors:   s.reloadErrors.Load(),
		LocalKeys:      s.localKeys.Load(),
		LocalEvicted:   s.localEvicted.Load(),
		ClockSkew:      time.Duration(s.clockSkewUS.Load()) * time.Microsecond,
		FallbackActive: s.fallback.Load(),
	}
}
