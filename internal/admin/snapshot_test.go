package admin

import (
	"sync"
	"testing"
	"time"
)

func TestNilSnapshotIsSafe(t *testing.T) {
	t.Parallel()
	var s *Snapshot
	s.RecordDecision("t", "p", true, "redis", time.Millisecond)
	s.RecordBackendError("redis", "timeout")
	s.SetFallbackActive("redis", true)
	s.AddScriptLoad()
	s.SetPolicyVersion("v")
	s.AddPolicyReloadError()
	s.SetClockSkew(time.Second)
	s.SetLocalKeys(3)
	s.AddLocalEvicted(2)
	if c := s.Counters(); c != (Counters{}) {
		t.Errorf("nil Counters = %+v, want zero", c)
	}
	if s.Decisions() != nil || s.Latencies() != nil || s.BackendErrors() != nil || s.Fallbacks() != nil {
		t.Error("nil views must be nil")
	}
}

func TestRecordDecisionSeriesAndHistogram(t *testing.T) {
	t.Parallel()
	s := NewSnapshot()
	s.RecordDecision("acme", "api", true, "redis", 40*time.Microsecond)
	s.RecordDecision("acme", "api", true, "redis", 60*time.Microsecond)
	s.RecordDecision("acme", "api", false, "redis", 2*time.Second)
	s.RecordDecision("acme", "deny", false, "", -time.Millisecond) // short-circuit: backend "none", negative clamps
	s.RecordDecision("beta", "api", true, "degraded", 3*time.Millisecond)

	c := s.Counters()
	if c.Allowed != 3 || c.Denied != 2 {
		t.Fatalf("Counters allowed/denied = %d/%d, want 3/2", c.Allowed, c.Denied)
	}

	want := []DecisionSeries{
		{DecisionKey{"acme", "api", false, "redis"}, 1},
		{DecisionKey{"acme", "api", true, "redis"}, 2},
		{DecisionKey{"acme", "deny", false, "none"}, 1},
		{DecisionKey{"beta", "api", true, "degraded"}, 1},
	}
	got := s.Decisions()
	if len(got) != len(want) {
		t.Fatalf("Decisions = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Decisions[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	lat := s.Latencies()
	if len(lat) != 3 || lat[0].Backend != "degraded" || lat[1].Backend != "none" || lat[2].Backend != "redis" {
		t.Fatalf("Latencies backends = %+v", lat)
	}
	r := lat[2]
	if r.Count != 3 || r.SumUS != 40+60+2_000_000 {
		t.Errorf("redis count/sum = %d/%d", r.Count, r.SumUS)
	}
	// 40us -> bucket 50us; 60us -> bucket 100us; 2s -> +Inf.
	if r.Cumulative[0] != 1 || r.Cumulative[1] != 2 || r.Cumulative[numLatencyBuckets-1] != 2 {
		t.Errorf("redis cumulative = %v", r.Cumulative)
	}
	n := lat[1]
	if n.SumUS != 0 || n.Cumulative[0] != 1 {
		t.Errorf("negative latency must clamp to 0: %+v", n)
	}
}

func TestDecisionSeriesBound(t *testing.T) {
	t.Parallel()
	s := NewSnapshot()
	for i := range MaxDecisionSeries + 10 {
		s.RecordDecision("t", string(rune('a'+i%26))+itoa(i), true, "local", 0)
	}
	got := s.Decisions()
	if len(got) != MaxDecisionSeries+1 {
		t.Fatalf("series = %d, want %d (+1 collapsed)", len(got), MaxDecisionSeries+1)
	}
	var other uint64
	for _, d := range got {
		if d.Key.Tenant == "other" && d.Key.Policy == "other" {
			other = d.N
		}
	}
	if other != 10 {
		t.Errorf("collapsed series = %d, want 10", other)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

func TestFallbackTimer(t *testing.T) {
	t.Parallel()
	s := NewSnapshot()
	base := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	now := base
	s.now = func() time.Time { return now }

	s.SetFallbackActive("redis", false) // idempotent: never active, no interval
	s.SetFallbackActive("redis", true)
	s.SetFallbackActive("redis", true) // idempotent: does not reset since
	now = base.Add(3 * time.Second)
	if c := s.Counters(); !c.FallbackActive || c.FallbackTotal != 3*time.Second {
		t.Fatalf("while open: %+v", c)
	}
	s.SetFallbackActive("redis", false)
	now = base.Add(10 * time.Second)
	if c := s.Counters(); c.FallbackActive || c.FallbackTotal != 3*time.Second {
		t.Fatalf("after close: %+v", c)
	}
	s.SetFallbackActive("redis", true)
	now = base.Add(12 * time.Second)
	fbs := s.Fallbacks()
	if len(fbs) != 1 || !fbs[0].Active || fbs[0].Total != 5*time.Second {
		t.Fatalf("Fallbacks = %+v", fbs)
	}
}

func TestCountersAndErrors(t *testing.T) {
	t.Parallel()
	s := NewSnapshot()
	s.RecordBackendError("redis", "timeout")
	s.RecordBackendError("redis", "timeout")
	s.RecordBackendError("redis", "noscript")
	s.AddScriptLoad()
	s.SetPolicyVersion("abc123")
	s.AddPolicyReloadError()
	s.SetClockSkew(-250 * time.Millisecond)
	s.SetLocalKeys(42)
	s.AddLocalEvicted(0) // ignored
	s.AddLocalEvicted(-1)
	s.AddLocalEvicted(5)

	c := s.Counters()
	want := Counters{BackendErrors: 3, ScriptLoads: 1, ReloadErrors: 1, LocalKeys: 42, LocalEvicted: 5, ClockSkew: -250 * time.Millisecond, PolicyVersion: "abc123"}
	if c != want {
		t.Errorf("Counters = %+v, want %+v", c, want)
	}
	errs := s.BackendErrors()
	if len(errs) != 2 || errs[0].Key.Kind != "noscript" || errs[0].N != 1 || errs[1].Key.Kind != "timeout" || errs[1].N != 2 {
		t.Errorf("BackendErrors = %+v", errs)
	}
}

func TestSnapshotConcurrent(t *testing.T) {
	t.Parallel()
	s := NewSnapshot()
	var wg sync.WaitGroup
	const workers, per = 8, 1000
	for w := range workers {
		wg.Go(func() {
			for i := range per {
				s.RecordDecision("t", "p"+itoa(w), i%2 == 0, "local", time.Duration(i)*time.Microsecond)
				s.RecordBackendError("local", "x")
				if i%100 == 0 {
					s.SetFallbackActive("redis", i%200 == 0)
					_ = s.Decisions()
					_ = s.Latencies()
				}
			}
		})
	}
	wg.Wait()
	c := s.Counters()
	if c.Allowed+c.Denied != workers*per || c.BackendErrors != workers*per {
		t.Errorf("Counters = %+v", c)
	}
	var sum uint64
	for _, d := range s.Decisions() {
		sum += d.N
	}
	if sum != workers*per {
		t.Errorf("labelled sum = %d, want %d", sum, workers*per)
	}
}
