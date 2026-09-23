package redisx

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/crowbett/ratelimitd/internal/limit"
)

func TestSourceAssembled(t *testing.T) {
	t.Parallel()
	for _, want := range []string{
		"local NARGV = 9", "local GCRA = {}", "local TOKENBUCKET = {}",
		"local SLIDINGWINDOW = {}", "local SLIDINGWINDOW_EXACT = {}", "return out", "mode == 'L'",
	} {
		if !strings.Contains(Source, want) {
			t.Errorf("Source lacks %q", want)
		}
	}
	if i, j := strings.Index(Source, "local function idiv"), strings.Index(Source, "local nkeys = #KEYS"); i > j {
		t.Error("common.lua must precede dispatch.lua")
	}
	if script.Hash() == "" {
		t.Error("script has no SHA")
	}
	if !strings.HasPrefix(Library, "#!lua name="+FunctionName+"\n") ||
		!strings.HasSuffix(Library, "redis.register_function('"+FunctionName+"', run)\n") {
		t.Error("Library lacks its shebang or registration")
	}
	if !strings.HasSuffix(Source, "return run(KEYS_IN, ARGV_IN)\n") || len(FunctionName) != len("ratelimitd_")+12 {
		t.Errorf("Source tail or FunctionName %q malformed", FunctionName)
	}
}

func TestPackArgs(t *testing.T) {
	t.Parallel()
	g, _ := limit.NewParams(limit.GCRA, 1000, time.Second, 200)
	x, _ := limit.NewParams(limit.SlidingWindowExact, 30, time.Minute, 30)
	reqs := []limit.Req{
		{Key: "a", Params: g, Cost: 3, Policy: "api"},
		{Key: "b", Params: x, Cost: 0, Policy: "exact"}, // cost 0 is sent as 1
	}
	args := packArgs(nil, reqs, 1_700_000_000_000_000, 1000, 77, true)
	want := []any{
		int64(1), int64(1_700_000_000_000_000), int64(1000), int64(200), int64(3), int64(1000), int64(1000), int64(1_000_000), int64(77),
		int64(4), int64(1_700_000_000_000_000), int64(2_000_000), int64(30), int64(1), int64(1000), int64(30), int64(60_000_000), int64(77),
		"1",
	}
	if len(args) != len(want) || len(args) != len(reqs)*argvPerKey+1 {
		t.Fatalf("len(args) = %d, want %d", len(args), len(want))
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("args[%d] = %v (%T), want %v (%T)", i, args[i], args[i], want[i], want[i])
		}
	}
	if last := packArgs(nil, reqs[:1], 1, 0, 0, false)[argvPerKey]; last != "0" {
		t.Errorf("peek flag = %v, want \"0\"", last)
	}
}

func TestUnpack(t *testing.T) {
	t.Parallel()
	reqs := []limit.Req{{Policy: "a"}, {Policy: "b"}}
	vals := []int64{1, 9, 0, 1500, 10, 0, 0, 2500, 7000, 10}
	ds, err := unpack(vals, reqs)
	if err != nil {
		t.Fatal(err)
	}
	want := []limit.Decision{
		{Allowed: true, Remaining: 9, ResetAfter: 1500 * time.Microsecond, Limit: 10, Policy: "a", Backend: limit.BackendRedis},
		{Allowed: false, RetryAfter: 2500 * time.Microsecond, ResetAfter: 7 * time.Millisecond, Limit: 10, Policy: "b", Backend: limit.BackendRedis},
	}
	for i := range want {
		if ds[i] != want[i] {
			t.Errorf("ds[%d] = %+v, want %+v", i, ds[i], want[i])
		}
	}
	if _, err := unpack(vals[:7], reqs); err == nil {
		t.Error("short reply accepted")
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	ok, _ := limit.NewParams(limit.TokenBucket, 10, time.Second, 10)
	tests := []struct {
		name string
		p    limit.Params
		want bool
	}{
		{"ok", ok, true},
		{"unspecified", limit.Params{Rate: 1, Burst: 1, Period: time.Second, EmissionUS: 1}, false},
		{"emission0", limit.Params{Algo: limit.GCRA, Rate: 1, Burst: 1, Period: time.Second}, false},
		{"too-large-algo", limit.Params{Algo: 9, Rate: 1, Burst: 1, Period: time.Second, EmissionUS: 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validate([]limit.Req{{Key: "k", Params: tt.p, Cost: 1}})
			if (err == nil) != tt.want {
				t.Fatalf("validate = %v, want ok=%v", err, tt.want)
			}
			if err != nil && !errors.Is(err, limit.ErrInvalidParams) {
				t.Fatalf("err = %v, want ErrInvalidParams", err)
			}
		})
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		kind string
		down bool
	}{
		{"deadline", context.DeadlineExceeded, KindTimeout, true},
		{"canceled", context.Canceled, KindCanceled, false},
		{"pool", redis.ErrPoolTimeout, KindPool, true},
		{"closed", redis.ErrClosed, KindClosed, true},
		{"net-timeout", &net.OpError{Op: "read", Err: timeoutError{}}, KindTimeout, true},
		{"refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, KindConn, true},
		{"eof", io.EOF, KindConn, true},
		{"script-bug", redis.NewCmdResult(nil, replyError("ERR user_script:1: boom")).Err(), KindReply, false},
		{"crossslot", replyError("CROSSSLOT Keys in request don't hash to the same slot"), KindReply, false},
		{"loading", replyError("LOADING Redis is loading the dataset in memory"), KindDown, true},
		{"readonly", replyError("READONLY You can't write against a read only replica."), KindDown, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			kind, down := classify(tt.err)
			if kind != tt.kind || down != tt.down {
				t.Fatalf("classify(%v) = %q,%v want %q,%v", tt.err, kind, down, tt.kind, tt.down)
			}
		})
	}
}

// replyError mimics go-redis's unexported proto.RedisError.
type replyError string

func (e replyError) Error() string { return string(e) }
func (replyError) RedisError()     {}

var _ net.Error = timeoutError{}
