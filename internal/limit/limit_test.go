package limit

import (
	"errors"
	"testing"
	"time"
)

func TestStrictest(t *testing.T) {
	t.Parallel()
	allow := func(remaining int, retry time.Duration, policy string) Decision {
		return Decision{Allowed: true, Remaining: remaining, RetryAfter: retry, Policy: policy}
	}
	deny := func(remaining int, retry time.Duration, policy string) Decision {
		return Decision{Allowed: false, Remaining: remaining, RetryAfter: retry, Policy: policy}
	}
	tests := []struct {
		name string
		in   []Decision
		want Decision
	}{
		{name: "empty is the zero decision", in: nil, want: Decision{}},
		{name: "single passes through", in: []Decision{allow(3, 0, "a")}, want: allow(3, 0, "a")},
		{name: "deny wins over allow", in: []Decision{allow(0, 0, "a"), deny(5, time.Second, "b")}, want: deny(5, time.Second, "b")},
		{name: "deny wins regardless of order", in: []Decision{deny(5, time.Second, "b"), allow(0, 0, "a")}, want: deny(5, time.Second, "b")},
		{name: "among allows min remaining", in: []Decision{allow(7, 0, "a"), allow(2, 0, "b"), allow(4, 0, "c")}, want: allow(2, 0, "b")},
		{name: "among denies min remaining", in: []Decision{deny(3, time.Second, "a"), deny(1, time.Millisecond, "b")}, want: deny(1, time.Millisecond, "b")},
		{name: "equal remaining then max retry", in: []Decision{deny(0, time.Second, "a"), deny(0, 3*time.Second, "b"), deny(0, 2*time.Second, "c")}, want: deny(0, 3*time.Second, "b")},
		{name: "full tie keeps the first", in: []Decision{deny(0, time.Second, "a"), deny(0, time.Second, "b")}, want: deny(0, time.Second, "a")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Strictest(tc.in); got != tc.want {
				t.Errorf("Strictest() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestKeyAndHashTag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tenant, subject, policy string
		wantTag, wantKey        string
	}{
		{"acme", "k1", "api", "{t=acme|s=k1}", "rl:{t=acme|s=k1}:p=api"},
		{"acme", "10.0.0.1", "per_ip", "{t=acme|s=10.0.0.1}", "rl:{t=acme|s=10.0.0.1}:p=per_ip"},
		{"", "", "", "{t=|s=}", "rl:{t=|s=}:p="},
		{"t", "a:b|c", "p.q-r", "{t=t|s=a:b|c}", "rl:{t=t|s=a:b|c}:p=p.q-r"},
	}
	for _, tc := range tests {
		t.Run(tc.wantKey, func(t *testing.T) {
			t.Parallel()
			if got := HashTag(tc.tenant, tc.subject); got != tc.wantTag {
				t.Errorf("HashTag = %q, want %q", got, tc.wantTag)
			}
			got := Key(tc.tenant, tc.subject, tc.policy)
			if got != tc.wantKey {
				t.Errorf("Key = %q, want %q", got, tc.wantKey)
			}
			if got[:len(KeyPrefix)] != KeyPrefix {
				t.Errorf("Key %q does not start with %q", got, KeyPrefix)
			}
			if got != KeyPrefix+HashTag(tc.tenant, tc.subject)+":p="+tc.policy {
				t.Errorf("Key %q is not prefix+tag+policy", got)
			}
		})
	}
}

func TestNewParams(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		algo         Algorithm
		rate         int64
		period       time.Duration
		burst        int64
		wantEmission int64
		wantErr      bool
	}{
		{name: "gcra 1000/s", algo: GCRA, rate: 1000, period: time.Second, burst: 200, wantEmission: 1000},
		{name: "token bucket 10/s", algo: TokenBucket, rate: 10, period: time.Second, burst: 20, wantEmission: 100_000},
		{name: "sliding window 60/m", algo: SlidingWindow, rate: 60, period: time.Minute, burst: 60, wantEmission: 1_000_000},
		{name: "sliding window exact 3/s", algo: SlidingWindowExact, rate: 3, period: time.Second, burst: 3, wantEmission: 333_333},
		{name: "one per millisecond floor", algo: GCRA, rate: 1, period: time.Millisecond, burst: 1, wantEmission: 1000},
		{name: "one per microsecond is the ceiling", algo: GCRA, rate: 1000, period: time.Millisecond, burst: 1, wantEmission: 1},
		{name: "faster than one per microsecond", algo: GCRA, rate: 1001, period: time.Millisecond, burst: 1, wantErr: true},
		{name: "unspecified algorithm", algo: AlgorithmUnspecified, rate: 1, period: time.Second, burst: 1, wantErr: true},
		{name: "unknown algorithm value", algo: Algorithm(9), rate: 1, period: time.Second, burst: 1, wantErr: true},
		{name: "zero rate", algo: GCRA, rate: 0, period: time.Second, burst: 1, wantErr: true},
		{name: "negative rate", algo: GCRA, rate: -5, period: time.Second, burst: 1, wantErr: true},
		{name: "period below 1ms", algo: GCRA, rate: 1, period: 999 * time.Microsecond, burst: 1, wantErr: true},
		{name: "zero burst", algo: GCRA, rate: 1, period: time.Second, burst: 0, wantErr: true},
		{name: "everything wrong at once", algo: AlgorithmUnspecified, rate: 0, period: 0, burst: 0, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewParams(tc.algo, tc.rate, tc.period, tc.burst)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewParams() = %+v, want error", got)
				}
				if !errors.Is(err, ErrInvalidParams) {
					t.Fatalf("error %v does not wrap ErrInvalidParams", err)
				}
				if got != (Params{}) {
					t.Fatalf("error path returned %+v, want zero Params", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewParams() error: %v", err)
			}
			want := Params{Algo: tc.algo, Rate: tc.rate, Period: tc.period, Burst: tc.burst, EmissionUS: tc.wantEmission}
			if got != want {
				t.Fatalf("NewParams() = %+v, want %+v", got, want)
			}
			if got.EmissionUS == 0 {
				t.Fatal("EmissionUS 0 is invalid")
			}
		})
	}
}

func TestNewParamsEveryAlgorithm(t *testing.T) {
	t.Parallel()
	for a := GCRA; a <= SlidingWindowExact; a++ {
		p, err := NewParams(a, 100, time.Second, 10)
		if err != nil {
			t.Fatalf("%s: %v", a, err)
		}
		if p.Algo != a || p.EmissionUS != 10_000 {
			t.Fatalf("%s: %+v", a, p)
		}
	}
}

func TestAlgorithmRoundTrip(t *testing.T) {
	t.Parallel()
	names := map[Algorithm]string{
		GCRA:               "gcra",
		TokenBucket:        "token_bucket",
		SlidingWindow:      "sliding_window",
		SlidingWindowExact: "sliding_window_exact",
	}
	for a, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := a.String(); got != name {
				t.Errorf("String() = %q, want %q", got, name)
			}
			back, err := ParseAlgorithm(name)
			if err != nil || back != a {
				t.Errorf("ParseAlgorithm(%q) = %v, %v; want %v", name, back, err, a)
			}
		})
	}
	if got := AlgorithmUnspecified.String(); got != "unspecified" {
		t.Errorf("unspecified String() = %q", got)
	}
	if got := Algorithm(42).String(); got != "algorithm(42)" {
		t.Errorf("out-of-range String() = %q", got)
	}
	for _, bad := range []string{"", "unspecified", "GCRA", "leaky_bucket", "algorithm(1)"} {
		a, err := ParseAlgorithm(bad)
		if err == nil || a != AlgorithmUnspecified || !errors.Is(err, ErrInvalidParams) {
			t.Errorf("ParseAlgorithm(%q) = %v, %v; want unspecified + ErrInvalidParams", bad, a, err)
		}
	}
}

func TestSentinelsAreDistinct(t *testing.T) {
	t.Parallel()
	all := []error{ErrUnknownTenant, ErrNoPolicy, ErrInvalidParams, ErrBackendDown, ErrDenied}
	for i, a := range all {
		for j, b := range all {
			if (i == j) != errors.Is(a, b) {
				t.Errorf("errors.Is(%v, %v) = %v", a, b, errors.Is(a, b))
			}
		}
	}
}
