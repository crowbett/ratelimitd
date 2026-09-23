package redisx

import (
	"crypto/sha256"
	"embed"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/crowbett/ratelimitd/internal/limit"
)

//go:embed lua/*.lua
var luaFS embed.FS

// luaOrder is the concatenation order: helpers first, the algorithms, then
// the dispatcher (dispatch.lua defines run(KEYS, argv)). All are chunk-level
// locals of ONE chunk, assembled two ways from the same files:
//
//   - Library: a Redis Function library (FUNCTION LOAD, then FCALL). The chunk
//     runs once at load, so the ~20 closures and tables it defines are built
//     once, not on every call. This is the default path (Redis >= 7).
//   - Source: the same chunk as an EVAL script that ends in run(KEYS, ARGV),
//     for a server without FUNCTION (Redis 6). Every EVALSHA re-executes the
//     chunk, definitions included, which measured ~20 % slower per call.
//
// The library and function are named after the content hash, so daemons of
// different versions can share one Redis without replacing each other's code.
var luaOrder = []string{"common.lua", "gcra.lua", "tokenbucket.lua", "slidingwindow.lua", "dispatch.lua"}

var (
	body = mustAssemble()

	// Source is the EVAL form of the script.
	Source = "local KEYS_IN, ARGV_IN = KEYS, ARGV\nlocal ARGV\n" + body + "return run(KEYS_IN, ARGV_IN)\n"

	// FunctionName is the FCALL target, "ratelimitd_<first 12 hex of the body's SHA-256>".
	FunctionName = "ratelimitd_" + fmt.Sprintf("%x", sha256.Sum256([]byte(body)))[:12]

	// Library is the FUNCTION LOAD form: one library holding one function.
	Library = "#!lua name=" + FunctionName + "\nlocal ARGV\n" + body +
		"redis.register_function('" + FunctionName + "', run)\n"
)

// script is the ONE redis.Script (one SHA) the EVALSHA path runs.
var script = redis.NewScript(Source)

func mustAssemble() string {
	var sb strings.Builder
	for _, name := range luaOrder {
		b, err := luaFS.ReadFile("lua/" + name)
		if err != nil {
			panic("redisx: embedded " + name + ": " + err.Error())
		}
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// Wire layout, mirrored by lua/common.lua: argvPerKey ARGV entries per key
// followed by one commit flag; replyPerKey integers per key on the way back.
const (
	argvPerKey  = 9
	replyPerKey = 5
)

// packArgs appends the per-key tuple for every req plus the trailing commit
// flag to dst (cap len(reqs)*argvPerKey+1 avoids a regrowth). Everything is
// an int64 so go-redis writes plain decimal integers; nothing is a float.
//
//	algo, now_us, emission_us, burst, cost, ttl_slack_ms, rate, period_us, nonce
func packArgs(dst []any, reqs []limit.Req, nowUS, slackMS, nonce int64, commit bool) []any {
	for i := range reqs {
		r := &reqs[i]
		dst = append(dst,
			int64(r.Params.Algo),
			nowUS,
			r.Params.EmissionUS,
			r.Params.Burst,
			int64(max(r.Cost, 1)),
			slackMS,
			r.Params.Rate,
			r.Params.Period.Microseconds(),
			nonce,
		)
	}
	if commit {
		return append(dst, "1")
	}
	return append(dst, "0")
}

// validate rejects reqs the script would reject, before a round trip.
func validate(reqs []limit.Req) error {
	for i := range reqs {
		p := &reqs[i].Params
		if p.Algo == limit.AlgorithmUnspecified || p.Algo > limit.SlidingWindowExact || p.EmissionUS <= 0 ||
			p.Rate <= 0 || p.Burst <= 0 || p.Period <= 0 {
			return fmt.Errorf("%w: req %d (%s)", limit.ErrInvalidParams, i, reqs[i].Policy)
		}
	}
	return nil
}

// unpack turns the flat reply {allowed, remaining, retry_us, reset_us,
// limit} * n into one Decision per req, stamped with the policy and "redis".
func unpack(vals []int64, reqs []limit.Req) ([]limit.Decision, error) {
	if len(vals) != len(reqs)*replyPerKey {
		return nil, fmt.Errorf("redisx: reply has %d integers, want %d", len(vals), len(reqs)*replyPerKey)
	}
	out := make([]limit.Decision, len(reqs))
	for i := range reqs {
		v := vals[i*replyPerKey : i*replyPerKey+replyPerKey]
		out[i] = limit.Decision{
			Allowed:    v[0] == 1,
			Remaining:  int(v[1]),
			RetryAfter: time.Duration(v[2]) * time.Microsecond,
			ResetAfter: time.Duration(v[3]) * time.Microsecond,
			Limit:      int(v[4]),
			Policy:     reqs[i].Policy,
			Backend:    limit.BackendRedis,
		}
	}
	return out, nil
}
