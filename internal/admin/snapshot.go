// Package admin is the operator surface: the Snapshot counters that feed both
// the OTel meter and the hand-written Prometheus text exposer (so the two
// cannot disagree), plus /metrics, /admin/stats, /healthz and /readyz.
//
// Owner: workstream C (service edge). Snapshot's method set is called by
// workstream B's stores and must stay nil-receiver safe.
package admin

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LatencyBucketsUS are the explicit upper bounds of rl_decision_latency_seconds
// in integer microseconds (50us .. 1s); +Inf is implicit. The exposer renders
// them as seconds.
var LatencyBucketsUS = latencyBucketsUS[:]

const numLatencyBuckets = 14

var latencyBucketsUS = [numLatencyBuckets]int64{
	50, 100, 250, 500,
	1_000, 2_500, 5_000, 10_000, 25_000, 50_000,
	100_000, 250_000, 500_000, 1_000_000,
}

// MaxDecisionSeries bounds the labelled rl_decisions_total map. tenant and
// policy are validated names from the policy file, so the bound is never hit
// in practice; past it new label sets collapse into {tenant="other",
// policy="other"} instead of growing without limit.
const MaxDecisionSeries = 4096

// DecisionKey labels one rl_decisions_total series. The subject is never a
// label (docs/PLAN.md, "Label cardinality").
type DecisionKey struct {
	Tenant  string
	Policy  string
	Allowed bool
	Backend string
}

// DecisionSeries is a read-only view of one labelled counter.
type DecisionSeries struct {
	Key DecisionKey
	N   uint64
}

// LatencySeries is a read-only view of one rl_decision_latency_seconds series
// (per backend) with cumulative bucket counts in LatencyBucketsUS order.
type LatencySeries struct {
	Backend    string
	Cumulative []uint64 // len(LatencyBucketsUS); the +Inf bucket equals Count
	SumUS      int64    // integer microseconds
	Count      uint64
}

// BackendErrorKey labels one rl_backend_errors_total series.
type BackendErrorKey struct {
	Backend string
	Kind    string
}

// BackendErrorSeries is a read-only view of one labelled error counter.
type BackendErrorSeries struct {
	Key BackendErrorKey
	N   uint64
}

// FallbackSeries is a read-only view of rl_fallback_active{backend} and its
// accumulated rl_fallback_seconds_total.
type FallbackSeries struct {
	Backend string
	Active  bool
	Total   time.Duration // includes the open interval when Active
}

// Snapshot holds the process-lifetime counters and gauges. Every method is a
// no-op on a nil receiver so tests can pass nil. Labelled series
// (tenant, policy, allowed, backend) are bounded by the policy file; the
// subject is never a label (docs/PLAN.md, "Label cardinality").
//
// The hot path (RecordDecision) takes a read lock to find its series and then
// bumps atomics; the write lock is taken only the first time a label set is
// seen, so 50k decisions/s never convoy on the map.
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
	policyVersion atomic.Pointer[string]

	now func() time.Time // wall clock for the fallback timer; tests override

	mu        sync.RWMutex
	decisions map[DecisionKey]*atomic.Uint64
	latency   map[string]*latencySeries // by backend
	errors    map[BackendErrorKey]*atomic.Uint64
	fallbacks map[string]*fallbackState // by backend
}

type latencySeries struct {
	buckets [numLatencyBuckets]atomic.Uint64 // per-bucket (not cumulative)
	inf     atomic.Uint64
	sumUS   atomic.Int64
	count   atomic.Uint64
}

type fallbackState struct {
	active bool
	since  time.Time
	total  time.Duration
}

// NewSnapshot returns an empty Snapshot.
func NewSnapshot() *Snapshot {
	return &Snapshot{
		now:       time.Now,
		decisions: make(map[DecisionKey]*atomic.Uint64),
		latency:   make(map[string]*latencySeries),
		errors:    make(map[BackendErrorKey]*atomic.Uint64),
		fallbacks: make(map[string]*fallbackState),
	}
}

// RecordDecision bumps rl_decisions_total{tenant,policy,allowed,backend} and
// observes latency into rl_decision_latency_seconds{backend}. An empty
// backend (allow/deny list short-circuits) is labelled "none".
func (s *Snapshot) RecordDecision(tenant, policy string, allowed bool, backend string, latency time.Duration) {
	if s == nil {
		return
	}
	if allowed {
		s.allowed.Add(1)
	} else {
		s.denied.Add(1)
	}
	if backend == "" {
		backend = "none"
	}
	s.decisionSeries(DecisionKey{Tenant: tenant, Policy: policy, Allowed: allowed, Backend: backend}).Add(1)
	s.latencySeries(backend).observe(latency.Microseconds())
}

func (s *Snapshot) decisionSeries(k DecisionKey) *atomic.Uint64 {
	s.mu.RLock()
	c := s.decisions[k]
	s.mu.RUnlock()
	if c != nil {
		return c
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.decisions[k]; existing != nil {
		return existing
	}
	if len(s.decisions) >= MaxDecisionSeries {
		k.Tenant, k.Policy = "other", "other"
		if existing := s.decisions[k]; existing != nil {
			return existing
		}
	}
	fresh := new(atomic.Uint64)
	s.decisions[k] = fresh
	return fresh
}

func (s *Snapshot) latencySeries(backend string) *latencySeries {
	s.mu.RLock()
	l := s.latency[backend]
	s.mu.RUnlock()
	if l != nil {
		return l
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.latency[backend]; existing != nil {
		return existing
	}
	fresh := &latencySeries{}
	s.latency[backend] = fresh
	return fresh
}

func (l *latencySeries) observe(us int64) {
	if us < 0 {
		us = 0
	}
	i := sort.Search(numLatencyBuckets, func(i int) bool { return latencyBucketsUS[i] >= us })
	if i < numLatencyBuckets {
		l.buckets[i].Add(1)
	} else {
		l.inf.Add(1)
	}
	l.sumUS.Add(us)
	l.count.Add(1)
}

// RecordBackendError bumps rl_backend_errors_total{backend,kind}.
func (s *Snapshot) RecordBackendError(backend, kind string) {
	if s == nil {
		return
	}
	s.backendErrors.Add(1)
	k := BackendErrorKey{Backend: backend, Kind: kind}
	s.mu.RLock()
	c := s.errors[k]
	s.mu.RUnlock()
	if c == nil {
		s.mu.Lock()
		if c = s.errors[k]; c == nil {
			c = new(atomic.Uint64)
			s.errors[k] = c
		}
		s.mu.Unlock()
	}
	c.Add(1)
}

// SetFallbackActive sets rl_fallback_active{backend} and accumulates
// rl_fallback_seconds_total while active. Repeated calls with the same value
// are idempotent.
func (s *Snapshot) SetFallbackActive(backend string, active bool) {
	if s == nil {
		return
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.fallbacks[backend]
	if f == nil {
		f = &fallbackState{}
		s.fallbacks[backend] = f
	}
	if f.active == active {
		return
	}
	if active {
		f.since = now
	} else {
		f.total += now.Sub(f.since)
	}
	f.active = active
	anyActive := false
	for _, x := range s.fallbacks {
		anyActive = anyActive || x.active
	}
	s.fallback.Store(anyActive)
}

// AddScriptLoad bumps rl_redis_script_loads_total.
func (s *Snapshot) AddScriptLoad() {
	if s == nil {
		return
	}
	s.scriptLoads.Add(1)
}

// SetPolicyVersion sets rl_policy_version_info{version}.
func (s *Snapshot) SetPolicyVersion(v string) {
	if s == nil {
		return
	}
	s.policyVersion.Store(&v)
}

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
	PolicyVersion  string
}

// Counters reads every total at once.
func (s *Snapshot) Counters() Counters {
	if s == nil {
		return Counters{}
	}
	var total time.Duration
	for _, f := range s.Fallbacks() {
		total += f.Total
	}
	var version string
	if v := s.policyVersion.Load(); v != nil {
		version = *v
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
		FallbackTotal:  total,
		PolicyVersion:  version,
	}
}

// Decisions returns every rl_decisions_total series sorted by tenant, policy,
// backend, allowed.
func (s *Snapshot) Decisions() []DecisionSeries {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]DecisionSeries, 0, len(s.decisions))
	for k, c := range s.decisions {
		out = append(out, DecisionSeries{Key: k, N: c.Load()})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Key, out[j].Key
		if a.Tenant != b.Tenant {
			return a.Tenant < b.Tenant
		}
		if a.Policy != b.Policy {
			return a.Policy < b.Policy
		}
		if a.Backend != b.Backend {
			return a.Backend < b.Backend
		}
		return !a.Allowed && b.Allowed
	})
	return out
}

// Latencies returns every rl_decision_latency_seconds series sorted by backend.
func (s *Snapshot) Latencies() []LatencySeries {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]LatencySeries, 0, len(s.latency))
	for backend, l := range s.latency {
		cum := make([]uint64, numLatencyBuckets)
		var acc uint64
		for i := range l.buckets {
			acc += l.buckets[i].Load()
			cum[i] = acc
		}
		out = append(out, LatencySeries{Backend: backend, Cumulative: cum, SumUS: l.sumUS.Load(), Count: l.count.Load()})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Backend < out[j].Backend })
	return out
}

// BackendErrors returns every rl_backend_errors_total series sorted by
// backend then kind.
func (s *Snapshot) BackendErrors() []BackendErrorSeries {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]BackendErrorSeries, 0, len(s.errors))
	for k, c := range s.errors {
		out = append(out, BackendErrorSeries{Key: k, N: c.Load()})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.Backend != out[j].Key.Backend {
			return out[i].Key.Backend < out[j].Key.Backend
		}
		return out[i].Key.Kind < out[j].Key.Kind
	})
	return out
}

// Fallbacks returns rl_fallback_active / rl_fallback_seconds_total per
// backend, sorted by backend. Total includes the still-open interval.
func (s *Snapshot) Fallbacks() []FallbackSeries {
	if s == nil {
		return nil
	}
	now := s.now()
	s.mu.RLock()
	out := make([]FallbackSeries, 0, len(s.fallbacks))
	for backend, f := range s.fallbacks {
		total := f.total
		if f.active {
			total += now.Sub(f.since)
		}
		out = append(out, FallbackSeries{Backend: backend, Active: f.active, Total: total})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Backend < out[j].Backend })
	return out
}
