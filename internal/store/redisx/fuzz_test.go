package redisx

import (
	"errors"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// FuzzPackUnpack drives the ARGV wire layout (packArgs, mirrored by
// lua/common.lua) and the reply decoder (unpack) with arbitrary params: pack
// never panics, always emits argvPerKey entries per key plus the flag, every
// entry is an int64 (never a float, never a string except the flag), the
// cost floor is 1, validate agrees with limit.NewParams on the params it
// would refuse, and a reply of the right length round-trips positionally
// while any other length is an error, never a panic or a short slice.
func FuzzPackUnpack(f *testing.F) {
	f.Add(int64(1), int64(1000), int64(1000), int64(200), int64(3), int64(1_700_000_000_000_000), int64(1000), int64(77), true, uint8(2))
	f.Add(int64(4), int64(30), int64(60_000_000), int64(30), int64(0), int64(1<<52), int64(0), int64(0), false, uint8(1))
	f.Add(int64(2), int64(10), int64(1_000_000), int64(20), int64(-5), int64(-1), int64(-1), int64(-1), true, uint8(0))
	f.Add(int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), false, uint8(3))
	f.Add(int64(5), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), int64(1), true, uint8(64))
	f.Add(int64(3), int64(1<<62), int64(1<<62), int64(1<<62), int64(1<<31), int64(1<<62), int64(1<<62), int64(1<<62), true, uint8(2))
	f.Fuzz(func(t *testing.T, algo, rate, periodUS, burst, cost, nowUS, slackMS, nonce int64, commit bool, n uint8) {
		if n > 64 {
			n = 64
		}
		p := limit.Params{Algo: limit.Algorithm(algo), Rate: rate, Period: time.Duration(periodUS) * time.Microsecond, Burst: burst}
		if rate != 0 {
			p.EmissionUS = periodUS / rate
		}
		reqs := make([]limit.Req, n)
		for i := range reqs {
			reqs[i] = limit.Req{Key: limit.Key("t", "s", "p"), Params: p, Cost: int(cost), Policy: "p"}
		}

		args := packArgs(nil, reqs, nowUS, slackMS, nonce, commit)
		if len(args) != len(reqs)*argvPerKey+1 {
			t.Fatalf("len(args) = %d, want %d", len(args), len(reqs)*argvPerKey+1)
		}
		for i, a := range args[:len(args)-1] {
			v, ok := a.(int64)
			if !ok {
				t.Fatalf("args[%d] = %v (%T), want int64", i, a, a)
			}
			switch i % argvPerKey {
			case 0:
				if v != int64(p.Algo) { // Algorithm is a uint8: the wire carries the truncated value validate rejects
					t.Fatalf("args[%d] algo = %d, want %d", i, v, p.Algo)
				}
			case 1:
				if v != nowUS {
					t.Fatalf("args[%d] now = %d, want %d", i, v, nowUS)
				}
			case 4:
				if v < 1 || (cost >= 1 && v != cost) {
					t.Fatalf("args[%d] cost = %d from %d (floor 1)", i, v, cost)
				}
			case 8:
				if v != nonce {
					t.Fatalf("args[%d] nonce = %d, want %d", i, v, nonce)
				}
			}
		}
		if flag := args[len(args)-1]; (commit && flag != "1") || (!commit && flag != "0") {
			t.Fatalf("flag = %v for commit=%v", flag, commit)
		}

		// validate must refuse exactly what the script would: an unknown
		// algorithm or a non-positive emission/rate/burst/period.
		verr := validate(reqs)
		bad := p.Algo == limit.AlgorithmUnspecified || p.Algo > limit.SlidingWindowExact || p.EmissionUS <= 0 || p.Rate <= 0 || p.Burst <= 0 || p.Period <= 0
		if n > 0 && bad != (verr != nil) {
			t.Fatalf("validate = %v, want refusal %v for %+v", verr, bad, p)
		}
		if verr != nil && !errors.Is(verr, limit.ErrInvalidParams) {
			t.Fatalf("validate error %v does not wrap ErrInvalidParams", verr)
		}

		// Round trip: a well-formed reply decodes positionally.
		vals := make([]int64, 0, len(reqs)*replyPerKey)
		for i := range reqs {
			base := int64(i) * 1000
			vals = append(vals, int64(i%2), base+1, base+2, base+3, base+4)
		}
		ds, err := unpack(vals, reqs)
		if err != nil {
			t.Fatalf("unpack: %v", err)
		}
		if len(ds) != len(reqs) {
			t.Fatalf("unpack returned %d decisions for %d reqs", len(ds), len(reqs))
		}
		for i, d := range ds {
			base := int64(i) * 1000
			want := limit.Decision{
				Allowed: i%2 == 1, Remaining: int(base + 1), RetryAfter: time.Duration(base+2) * time.Microsecond,
				ResetAfter: time.Duration(base+3) * time.Microsecond, Limit: int(base + 4), Policy: "p", Backend: limit.BackendRedis,
			}
			if d != want {
				t.Fatalf("decision[%d] = %+v, want %+v", i, d, want)
			}
		}
		for _, wrong := range [][]int64{vals[:len(vals)/2], append(append([]int64(nil), vals...), 0), nil} {
			if len(wrong) == len(vals) {
				continue
			}
			if got, err := unpack(wrong, reqs); err == nil || got != nil {
				t.Fatalf("unpack(%d ints for %d reqs) = %v, %v; want an error and nil", len(wrong), len(reqs), got, err)
			}
		}
	})
}
