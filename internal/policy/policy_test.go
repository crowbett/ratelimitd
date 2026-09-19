package policy

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// baseYAML is a valid three-policy tenant plus one global, all tagged by
// {subject} so they share one hash tag.
const baseYAML = `version: 1
defaults: { algorithm: gcra, on_backend_error: local, cost: 1 }
tenants:
  acme:
    default: api
    allow: ["10.0.0.0/8", "trusted-*"]
    deny: ["203.0.113.7", "banned-*"]
    policies:
      - { name: api, algorithm: gcra, rate: 1000, per: 1s, burst: 200, key: "{tenant}:{subject}" }
      - { name: search, algorithm: token_bucket, rate: 10, per: 1s, burst: 20, key: "{tenant}:{subject}:{route}",
          match: { routes: ["/v1/search*"] }, cost: { "/v1/search/bulk*": 10, "/v1/search/*": 2 } }
      - { name: writes, algorithm: sliding_window, rate: 60, per: 1m, burst: 60, key: "{subject}:{method}",
          match: { methods: ["POST", "PUT"] } }
      - { name: off, rate: 1, per: 1s, burst: 1, key: "{subject}", enabled: false }
  other:
    policies:
      - { name: only, rate: 5, per: 1s, burst: 5, key: "{tenant}:{subject}:{apikey}", match: { routes: ["/admin/*"] } }
global:
  - { name: global_ingress, algorithm: gcra, rate: 50000, per: 1s, burst: 5000, key: "{subject}" }
`

func parseYAML(tb testing.TB, y string) Set {
	tb.Helper()
	s, err := Parse(strings.NewReader(y))
	if err != nil {
		tb.Fatalf("Parse: %v", err)
	}
	return s
}

func loadYAML(tb testing.TB, y string) Set {
	tb.Helper()
	s := parseYAML(tb, y)
	if err := s.Validate(); err != nil {
		tb.Fatalf("Validate: %v", err)
	}
	return s
}

func testLogger(tb testing.TB) *slog.Logger {
	tb.Helper()
	return slog.New(slog.NewTextHandler(&testWriter{tb: tb}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ tb testing.TB }

func (w *testWriter) Write(p []byte) (int, error) {
	w.tb.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func TestParseStrict(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{name: "valid", yaml: baseYAML},
		{name: "empty document is a zero set", yaml: ""},
		{name: "unknown top-level field", yaml: "version: 1\nbogus: 1\n", wantErr: "field bogus not found"},
		{name: "unknown policy field", yaml: "version: 1\nglobal:\n  - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\", ratee: 2 }\n", wantErr: "field ratee not found"},
		{name: "unknown match field", yaml: "version: 1\nglobal:\n  - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\", match: { paths: [\"/x\"] } }\n", wantErr: "field paths not found"},
		{name: "not yaml", yaml: "{{{", wantErr: "parse policies"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := Parse(strings.NewReader(tc.yaml))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Parse err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if s.Version == "" || s.LoadedAt.IsZero() {
				t.Fatalf("Version/LoadedAt not stamped: %+v", s)
			}
			for id, tn := range s.Tenants {
				if tn.ID != id {
					t.Errorf("tenant %q ID = %q", id, tn.ID)
				}
			}
		})
	}
}

func TestValidateRules(t *testing.T) {
	t.Parallel()
	pol := func(fields string) string {
		return "version: 1\ntenants:\n  t:\n    policies:\n      - { name: a, rate: 10, per: 1s, burst: 10, " + fields + " }\n"
	}
	two := func(a, b string) string {
		return "version: 1\ntenants:\n  t:\n    policies:\n      - { name: a, rate: 10, per: 1s, burst: 10, " + a + " }\n      - { name: b, rate: 10, per: 1s, burst: 10, " + b + " }\n"
	}
	tests := []struct {
		name    string
		yaml    string
		wantErr string // "" = valid
	}{
		{name: "base is valid", yaml: baseYAML},
		{name: "unknown placeholder", yaml: pol(`key: "{tenant}:{user_id}"`), wantErr: "unknown placeholder {user_id}"},
		{name: "descriptor declared under match resolves", yaml: pol(`key: "{tenant}:{user_id}", match: { descriptors: { user_id: "*" } }`)},
		{name: "builtin descriptors resolve", yaml: pol(`key: "{apikey}:{ip}:{route}:{method}"`)},
		{name: "tenant only drops the subject", yaml: pol(`key: "{tenant}"`), wantErr: "needs {subject} or a descriptor placeholder"},
		{name: "literal only", yaml: pol(`key: "global"`), wantErr: "needs {subject} or a descriptor placeholder"},
		{name: "unmatched brace", yaml: pol(`key: "{subject"`), wantErr: "unmatched '{'"},
		{name: "stray close brace", yaml: pol(`key: "subject}"`), wantErr: "unmatched '}'"},
		{name: "empty placeholder", yaml: pol(`key: "{}"`), wantErr: "bad placeholder {}"},
		{name: "empty key", yaml: pol(`key: ""`), wantErr: "key template is required"},
		{name: "hash tag conflict subject vs apikey", yaml: two(`key: "{subject}"`, `key: "{apikey}"`), wantErr: "hash to different tags {subject} vs {apikey}"},
		{name: "hash tag conflict apikey vs ip (the example file)", yaml: two(`key: "{tenant}:{apikey}"`, `key: "{ip}"`), wantErr: "hash to different tags {apikey} vs {ip}"},
		{name: "same tag with extras is fine", yaml: two(`key: "{tenant}:{apikey}"`, `key: "{tenant}:{apikey}:{route}"`)},
		{name: "different tags but disjoint literal routes", yaml: two(`key: "{subject}", match: { routes: ["/a"] }`, `key: "{apikey}", match: { routes: ["/b"] }`)},
		{name: "different tags but disjoint methods", yaml: two(`key: "{subject}", match: { methods: ["GET"] }`, `key: "{apikey}", match: { methods: ["POST"] }`)},
		{name: "different tags but disjoint descriptor values", yaml: two(`key: "{subject}", match: { descriptors: { plan: free } }`, `key: "{apikey}", match: { descriptors: { plan: pro } }`)},
		{name: "different tags with overlapping globs", yaml: two(`key: "{subject}", match: { routes: ["/v1/*"] }`, `key: "{apikey}", match: { routes: ["/v1/x"] }`), wantErr: "hash to different tags"},
		{name: "different tags with any-value descriptor still overlap", yaml: two(`key: "{subject}", match: { descriptors: { plan: "*" } }`, `key: "{apikey}", match: { descriptors: { plan: pro } }`), wantErr: "hash to different tags"},
		{name: "disabled policy is exempt", yaml: two(`key: "{subject}"`, `key: "{apikey}", enabled: false`)},
		{name: "global conflicts with tenant tag", yaml: "version: 1\ntenants:\n  t:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{apikey}\" }\nglobal:\n  - { name: g, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n", wantErr: "hash to different tags {apikey} vs {subject}"},
		{name: "global shadows tenant name", yaml: "version: 1\ntenants:\n  t:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\nglobal:\n  - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n", wantErr: `global policy "a" shadows tenant "t"`},
		{name: "sliding window burst below rate", yaml: "version: 1\nglobal:\n  - { name: a, algorithm: sliding_window, rate: 10, per: 1s, burst: 5, key: \"{subject}\" }\n", wantErr: "ignores burst, which must be >= rate"},
		{name: "bad route glob", yaml: pol(`key: "{subject}", match: { routes: ["/v1/[a-"] }`), wantErr: "match.routes: glob"},
		{name: "bad cost glob", yaml: pol(`key: "{subject}", cost: { "[": 2 }`), wantErr: "cost: glob"},
		{name: "cost below one", yaml: pol(`key: "{subject}", cost: { "/x": 0 }`), wantErr: "must be >= 1"},
		{name: "bad allow entry", yaml: "version: 1\ntenants:\n  t:\n    allow: [\"[\"]\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n", wantErr: `allow "["`},
		{name: "bad deny entry", yaml: "version: 1\ntenants:\n  t:\n    deny: [\"\"]\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n", wantErr: `deny ""`},
		{name: "empty method", yaml: pol(`key: "{subject}", match: { methods: [""] }`), wantErr: "empty method"},
		{name: "duplicate policy", yaml: two(`key: "{subject}"`, `key: "{subject}"`) + "      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n", wantErr: `duplicate policy "a"`},
		{name: "default not found", yaml: "version: 1\ntenants:\n  t:\n    default: nope\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n", wantErr: `default policy "nope" not found`},
		{name: "wrong schema version", yaml: "version: 2\n", wantErr: "version must be 1"},
		{name: "numeric rule still reported", yaml: "version: 1\nglobal:\n  - { name: a, rate: 0, per: 1s, burst: 1, key: \"{subject}\" }\n", wantErr: "rate must be > 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := parseYAML(t, tc.yaml)
			err := s.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()
	r := NewRegistry(loadYAML(t, baseYAML), testLogger(t))
	ds := func(kv ...string) []Descriptor {
		out := make([]Descriptor, 0, len(kv)/2)
		for i := 0; i+1 < len(kv); i += 2 {
			out = append(out, Descriptor{Key: kv[i], Value: kv[i+1]})
		}
		return out
	}
	type want struct {
		key, policy string
		cost        int
		algo        limit.Algorithm
	}
	tests := []struct {
		name                           string
		tenant, subject, route, method string
		ds                             []Descriptor
		want                           []want
		wantDecision                   *limit.Decision
		wantErr                        error
	}{
		{
			name: "plain GET matches api and global", tenant: "acme", subject: "k1", route: "/v1/users", method: "GET",
			want: []want{
				{"rl:{t=acme|s=k1}:p=api", "api", 1, limit.GCRA},
				{"rl:{t=acme|s=k1}:p=global_ingress", "global_ingress", 1, limit.GCRA},
			},
		},
		{
			name: "search route adds the search policy with its route extra and glob cost", tenant: "acme", subject: "k1", route: "/v1/search/bulk/run", method: "GET",
			want: []want{
				{"rl:{t=acme|s=k1}:p=api", "api", 1, limit.GCRA},
				{"rl:{t=acme|s=k1}:p=search:/v1/search/bulk/run", "search", 10, limit.TokenBucket},
				{"rl:{t=acme|s=k1}:p=global_ingress", "global_ingress", 1, limit.GCRA},
			},
		},
		{
			name: "less specific cost glob", tenant: "acme", subject: "k1", route: "/v1/search/x", method: "GET",
			want: []want{
				{"rl:{t=acme|s=k1}:p=api", "api", 1, limit.GCRA},
				{"rl:{t=acme|s=k1}:p=search:/v1/search/x", "search", 2, limit.TokenBucket},
				{"rl:{t=acme|s=k1}:p=global_ingress", "global_ingress", 1, limit.GCRA},
			},
		},
		{
			name: "cost table miss falls back to defaults.cost", tenant: "acme", subject: "k1", route: "/v1/search", method: "GET",
			want: []want{
				{"rl:{t=acme|s=k1}:p=api", "api", 1, limit.GCRA},
				{"rl:{t=acme|s=k1}:p=search:/v1/search", "search", 1, limit.TokenBucket},
				{"rl:{t=acme|s=k1}:p=global_ingress", "global_ingress", 1, limit.GCRA},
			},
		},
		{
			name: "POST adds the method-matched policy", tenant: "acme", subject: "k1", route: "/v1/users", method: "POST",
			want: []want{
				{"rl:{t=acme|s=k1}:p=api", "api", 1, limit.GCRA},
				{"rl:{t=acme|s=k1}:p=writes:POST", "writes", 1, limit.SlidingWindow},
				{"rl:{t=acme|s=k1}:p=global_ingress", "global_ingress", 1, limit.GCRA},
			},
		},
		{
			name: "deny list by IP subject", tenant: "acme", subject: "203.0.113.7", route: "/", method: "GET",
			wantDecision: &limit.Decision{Allowed: false, Policy: "deny"},
		},
		{
			name: "deny list by ip descriptor beats allow", tenant: "acme", subject: "trusted-1", route: "/", method: "GET", ds: ds("ip", "203.0.113.7"),
			wantDecision: &limit.Decision{Allowed: false, Policy: "deny"},
		},
		{
			name: "deny glob", tenant: "acme", subject: "banned-42", route: "/", method: "GET",
			wantDecision: &limit.Decision{Allowed: false, Policy: "deny"},
		},
		{
			name: "allow CIDR by subject", tenant: "acme", subject: "10.1.2.3", route: "/", method: "GET",
			wantDecision: &limit.Decision{Allowed: true, Policy: "allow"},
		},
		{
			name: "allow CIDR by ip descriptor", tenant: "acme", subject: "k1", route: "/", method: "GET", ds: ds("ip", "10.9.9.9"),
			wantDecision: &limit.Decision{Allowed: true, Policy: "allow"},
		},
		{
			name: "allow glob", tenant: "acme", subject: "trusted-ops", route: "/", method: "GET",
			wantDecision: &limit.Decision{Allowed: true, Policy: "allow"},
		},
		{
			name: "descriptor template with a value", tenant: "other", subject: "s", route: "/admin/users", method: "GET", ds: ds("apikey", "K9"),
			want: []want{
				{"rl:{t=other|s=s}:p=only:K9", "only", 1, limit.GCRA},
				{"rl:{t=other|s=s}:p=global_ingress", "global_ingress", 1, limit.GCRA},
			},
		},
		{
			name: "missing descriptor renders empty", tenant: "other", subject: "s", route: "/admin/users", method: "GET",
			want: []want{
				{"rl:{t=other|s=s}:p=only:", "only", 1, limit.GCRA},
				{"rl:{t=other|s=s}:p=global_ingress", "global_ingress", 1, limit.GCRA},
			},
		},
		{
			name: "no tenant match without a default leaves the global", tenant: "other", subject: "s", route: "/v1/x", method: "GET",
			want: []want{{"rl:{t=other|s=s}:p=global_ingress", "global_ingress", 1, limit.GCRA}},
		},
		{name: "unknown tenant", tenant: "nope", subject: "s", route: "/", method: "GET", wantErr: limit.ErrUnknownTenant},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reqs, dec, err := r.Resolve(tc.tenant, tc.subject, tc.ds, tc.route, tc.method)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if tc.wantDecision != nil {
				if reqs != nil || dec == nil || *dec != *tc.wantDecision {
					t.Fatalf("reqs=%v dec=%+v, want short-circuit %+v", reqs, dec, tc.wantDecision)
				}
				return
			}
			if dec != nil {
				t.Fatalf("unexpected short-circuit %+v", dec)
			}
			if len(reqs) != len(tc.want) {
				t.Fatalf("got %d reqs %+v, want %d", len(reqs), reqs, len(tc.want))
			}
			for i, w := range tc.want {
				got := reqs[i]
				if got.Key != w.key || got.Policy != w.policy || got.Cost != w.cost || got.Params.Algo != w.algo {
					t.Errorf("req[%d] = {%s %s cost=%d algo=%s}, want {%s %s cost=%d algo=%s}", i, got.Key, got.Policy, got.Cost, got.Params.Algo, w.key, w.policy, w.cost, w.algo)
				}
				if got.Params.EmissionUS == 0 {
					t.Errorf("req[%d] Params not precomputed: %+v", i, got.Params)
				}
			}
		})
	}
}

func TestResolveDefaultAndNoPolicy(t *testing.T) {
	t.Parallel()
	y := `version: 1
tenants:
  t:
    default: fallback
    policies:
      - { name: fallback, rate: 1, per: 1s, burst: 1, key: "{subject}", match: { routes: ["/never/*"] } }
      - { name: admin, rate: 2, per: 1s, burst: 2, key: "{subject}", match: { routes: ["/admin/*"] } }
  bare:
    policies:
      - { name: p, rate: 2, per: 1s, burst: 2, key: "{subject}", match: { routes: ["/admin/*"] } }
`
	r := NewRegistry(loadYAML(t, y), testLogger(t))
	reqs, _, err := r.Resolve("t", "s", nil, "/admin/x", "GET")
	if err != nil || len(reqs) != 1 || reqs[0].Policy != "admin" {
		t.Fatalf("matched: %+v %v", reqs, err)
	}
	reqs, _, err = r.Resolve("t", "s", nil, "/other", "GET")
	if err != nil || len(reqs) != 1 || reqs[0].Policy != "fallback" {
		t.Fatalf("default: %+v %v", reqs, err)
	}
	if _, _, err = r.Resolve("bare", "s", nil, "/other", "GET"); !errors.Is(err, limit.ErrNoPolicy) {
		t.Fatalf("no policy err = %v", err)
	}
}

func TestResolveKeysShareOneTag(t *testing.T) {
	t.Parallel()
	r := NewRegistry(loadYAML(t, baseYAML), testLogger(t))
	reqs, _, err := r.Resolve("acme", "k1", nil, "/v1/search/bulk", "POST")
	if err != nil || len(reqs) != 4 {
		t.Fatalf("%+v %v", reqs, err)
	}
	tag := limit.HashTag("acme", "k1")
	for _, q := range reqs {
		if !strings.HasPrefix(q.Key, limit.KeyPrefix+tag+":p=") {
			t.Errorf("key %q is outside tag %s", q.Key, tag)
		}
	}
}

func TestWriteRoundTrip(t *testing.T) {
	t.Parallel()
	s := loadYAML(t, baseYAML)
	var buf bytes.Buffer
	if err := Write(&buf, s); err != nil {
		t.Fatalf("Write: %v", err)
	}
	back, err := Parse(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Parse(Write): %v\n%s", err, buf.String())
	}
	if err := back.Validate(); err != nil {
		t.Fatalf("Validate(Write): %v", err)
	}
	if len(back.Tenants) != 2 || len(back.Tenants["acme"].Policies) != 4 || len(back.Global) != 1 {
		t.Fatalf("round trip lost content: %+v", back)
	}
	if back.Tenants["acme"].Policies[1].Per != time.Second || back.Tenants["acme"].Policies[2].Per != time.Minute {
		t.Fatalf("durations did not round trip: %+v", back.Tenants["acme"].Policies)
	}
	if e := back.Tenants["acme"].Policies[3].Enabled; e == nil || *e {
		t.Fatalf("enabled: false did not round trip")
	}
	var again bytes.Buffer
	if err := Write(&again, back); err != nil {
		t.Fatalf("Write again: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), again.Bytes()) {
		t.Fatalf("Write is not deterministic:\n%s\n---\n%s", buf.String(), again.String())
	}
	if strings.Contains(buf.String(), "loadedat") || strings.Contains(buf.String(), "LoadedAt") {
		t.Fatalf("LoadedAt leaked into the file:\n%s", buf.String())
	}
}

func TestConcurrentReadersDuringUpsert(t *testing.T) {
	t.Parallel()
	r := NewRegistry(loadYAML(t, baseYAML), slog.New(slog.NewTextHandler(nopWriter{}, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				snap := r.Snapshot()
				reqs, dec, err := r.Resolve("acme", "k1", nil, "/v1/search/bulk", "POST")
				if err != nil || dec != nil {
					t.Errorf("Resolve: %v %+v", err, dec)
					return
				}
				if len(reqs) < 4 || len(reqs) > 5 {
					t.Errorf("got %d reqs", len(reqs))
					return
				}
				if snap == nil || snap.Version == "" {
					t.Error("bad snapshot")
					return
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		burst := int64(10 + i%5)
		p := Policy{Name: "extra", Rate: 100, Per: time.Second, Burst: burst, Key: "{subject}:{route}"}
		if i%2 == 1 {
			if _, err := r.Delete(ctx, "acme", "extra"); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			continue
		}
		if _, err := r.Upsert(ctx, "acme", p); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	cancel()
	wg.Wait()
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func writeFile(tb testing.TB, path, content string) {
	tb.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		tb.Fatalf("write %s: %v", path, err)
	}
}

func waitFor(tb testing.TB, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for %s", what)
}

func TestWatchReloadsAndKeepsLastGood(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "policies.yaml")
	writeFile(t, path, baseYAML)
	initial, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	r := NewRegistry(initial, testLogger(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Watch(ctx, path, 10*time.Millisecond) }()

	v1 := r.Snapshot().Version
	// Same content, new mtime: hash matches -> no reload.
	time.Sleep(30 * time.Millisecond)
	writeFile(t, path, baseYAML)
	time.Sleep(60 * time.Millisecond)
	if r.Reloads() != 0 || r.Snapshot().Version != v1 {
		t.Fatalf("identical rewrite reloaded: reloads=%d version=%s", r.Reloads(), r.Snapshot().Version)
	}

	// Real change: reload.
	v2yaml := strings.Replace(baseYAML, "burst: 200", "burst: 300", 1)
	writeFile(t, path, v2yaml)
	waitFor(t, "reload", func() bool { return r.Reloads() == 1 })
	if r.Snapshot().Version == v1 || r.Snapshot().Tenants["acme"].Policies[0].Burst != 300 {
		t.Fatalf("v2 not live: %+v", r.Snapshot().Tenants["acme"].Policies[0])
	}
	v2 := r.Snapshot().Version

	// Bad file: counted, last good stays.
	writeFile(t, path, "version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 0, per: 1s, burst: 1, key: \"{subject}\" }\n")
	waitFor(t, "reload error", func() bool { return r.ReloadErrors() >= 1 })
	if r.Snapshot().Version != v2 {
		t.Fatalf("bad file swapped in")
	}
	// Unparseable file: same.
	writeFile(t, path, "version: 1\nbogus: true\n")
	waitFor(t, "second reload error", func() bool { return r.ReloadErrors() >= 2 })
	if r.Snapshot().Version != v2 || r.Reloads() != 1 {
		t.Fatalf("bad file swapped in")
	}
	// Restore v2 content: hash equals the live version -> no reload counted.
	writeFile(t, path, v2yaml)
	time.Sleep(60 * time.Millisecond)
	if r.Reloads() != 1 || r.Snapshot().Version != v2 {
		t.Fatalf("restoring the live content reloaded: reloads=%d", r.Reloads())
	}
	// Missing file: counted, last good stays.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	errs := r.ReloadErrors()
	waitFor(t, "stat error", func() bool { return r.ReloadErrors() > errs })
	if r.Snapshot().Version != v2 {
		t.Fatalf("missing file changed the snapshot")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Watch returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return on ctx cancel")
	}
}

func TestUpsertRewritesFileAndWatcherNoOps(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "policies.yaml")
	writeFile(t, path, baseYAML)
	initial, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	r := NewRegistry(initial, testLogger(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Watch(ctx, path, 10*time.Millisecond) }()
	time.Sleep(30 * time.Millisecond) // let the first poll record the file

	p := Policy{Name: "uploads", Algorithm: "token_bucket", Rate: 5, Per: time.Second, Burst: 10, Key: "{subject}:{route}", Match: Match{Routes: []string{"/upload/*"}}}
	set, err := r.Upsert(ctx, "acme", p)
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if set != r.Snapshot() || !hasPolicy(set.Tenants["acme"].Policies, "uploads") {
		t.Fatalf("Upsert did not swap: %+v", set)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "uploads") || contentHash(raw) != set.Version {
		t.Fatalf("file not rewritten to the swapped version:\n%s", raw)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(files) != 0 {
		t.Fatalf("temp files left behind: %v", files)
	}
	back, err := Load(path)
	if err != nil {
		t.Fatalf("rewritten file does not load: %v", err)
	}
	if back.Tenants["acme"].Policies[4].Match.Routes[0] != "/upload/*" {
		t.Fatalf("policy not persisted: %+v", back.Tenants["acme"].Policies)
	}
	reqs, _, err := r.Resolve("acme", "k1", nil, "/upload/a", "GET")
	if err != nil || len(reqs) != 3 || reqs[1].Policy != "uploads" || reqs[1].Params.Algo != limit.TokenBucket {
		t.Fatalf("new policy not resolved: %+v %v", reqs, err)
	}

	// The watcher sees its own write and no-ops.
	time.Sleep(80 * time.Millisecond)
	if r.Reloads() != 0 || r.ReloadErrors() != 0 || r.Snapshot() != set {
		t.Fatalf("watcher reacted to the registry's own write: reloads=%d errors=%d", r.Reloads(), r.ReloadErrors())
	}

	// Invalid upsert: rejected, nothing changes.
	bad := Policy{Name: "bad", Rate: 1, Per: time.Second, Burst: 1, Key: "{apikey}"} // tag conflict
	if _, err := r.Upsert(ctx, "acme", bad); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("invalid upsert err = %v", err)
	}
	if r.Snapshot() != set {
		t.Fatal("invalid upsert swapped")
	}
	if _, err := r.Upsert(ctx, "acme", Policy{Name: "BAD NAME", Rate: 1, Per: time.Second, Burst: 1, Key: "{subject}"}); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("bad name err = %v", err)
	}

	// New tenant and a global.
	if _, err := r.Upsert(ctx, "newco", Policy{Name: "n", Rate: 1, Per: time.Second, Burst: 1, Key: "{subject}"}); err != nil {
		t.Fatalf("new tenant: %v", err)
	}
	if _, err := r.Upsert(ctx, "", Policy{Name: "g2", Rate: 1, Per: time.Second, Burst: 1, Key: "{subject}"}); err != nil {
		t.Fatalf("global upsert: %v", err)
	}
	if reqs, _, err := r.Resolve("newco", "s", nil, "/", "GET"); err != nil || len(reqs) != 3 {
		t.Fatalf("newco: %+v %v", reqs, err)
	}

	// Delete.
	if _, err := r.Delete(ctx, "acme", "uploads"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Delete(ctx, "acme", "uploads"); !errors.Is(err, limit.ErrNoPolicy) {
		t.Fatalf("second delete err = %v", err)
	}
	if _, err := r.Delete(ctx, "ghost", "x"); !errors.Is(err, limit.ErrUnknownTenant) {
		t.Fatalf("unknown tenant delete err = %v", err)
	}
	if _, err := r.Delete(ctx, "", "g2"); err != nil {
		t.Fatalf("global delete: %v", err)
	}
	if _, err := r.Delete(ctx, "", "g2"); !errors.Is(err, limit.ErrNoPolicy) {
		t.Fatalf("global second delete err = %v", err)
	}
	// Deleting the default clears it.
	if _, err := r.Delete(ctx, "acme", "api"); err != nil {
		t.Fatalf("delete default: %v", err)
	}
	if r.Snapshot().Tenants["acme"].Default != "" {
		t.Fatal("default not cleared")
	}
	raw, _ = os.ReadFile(path)
	if strings.Contains(string(raw), "name: api\n") || strings.Contains(string(raw), "g2") {
		t.Fatalf("deletes not persisted:\n%s", raw)
	}
	time.Sleep(60 * time.Millisecond)
	if r.Reloads() != 0 || r.ReloadErrors() != 0 {
		t.Fatalf("watcher reacted to registry writes: reloads=%d errors=%d", r.Reloads(), r.ReloadErrors())
	}
	cancelled, c2 := context.WithCancel(ctx)
	c2()
	if _, err := r.Upsert(cancelled, "acme", p); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx err = %v", err)
	}
}

func TestUpsertWithoutWatchIsInMemory(t *testing.T) {
	t.Parallel()
	r := NewRegistry(loadYAML(t, baseYAML), testLogger(t))
	set, err := r.Upsert(context.Background(), "acme", Policy{Name: "mem", Rate: 1, Per: time.Second, Burst: 1, Key: "{subject}"})
	if err != nil || !hasPolicy(set.Tenants["acme"].Policies, "mem") {
		t.Fatalf("%v %+v", err, set)
	}
}

func TestNewRegistryWithUncompilableSet(t *testing.T) {
	t.Parallel()
	s := parseYAML(t, "version: 1\ntenants:\n  t:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{nope}\" }\n")
	r := NewRegistry(s, testLogger(t))
	if _, _, err := r.Resolve("t", "s", nil, "/", "GET"); !errors.Is(err, limit.ErrUnknownTenant) {
		t.Fatalf("err = %v", err)
	}
	if r.Snapshot() == nil {
		t.Fatal("nil snapshot")
	}
}

func BenchmarkResolve(b *testing.B) {
	y := `version: 1
tenants:
  acme:
    policies:
      - { name: api, algorithm: gcra, rate: 1000, per: 1s, burst: 200, key: "{tenant}:{apikey}" }
      - { name: search, algorithm: token_bucket, rate: 10, per: 1s, burst: 20, key: "{tenant}:{apikey}:{route}",
          match: { routes: ["/v1/search*"] }, cost: { "/v1/search/bulk*": 10 } }
      - { name: per_ip, algorithm: sliding_window, rate: 60, per: 1m, burst: 60, key: "{apikey}:{ip}" }
`
	r := NewRegistry(loadYAML(b, y), slog.New(slog.NewTextHandler(nopWriter{}, nil)))
	ds := []Descriptor{{Key: "apikey", Value: "k-0123456789"}, {Key: "ip", Value: "198.51.100.23"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqs, _, err := r.Resolve("acme", "k-0123456789", ds, "/v1/search/bulk", "POST")
		if err != nil || len(reqs) != 3 {
			b.Fatalf("%v %d", err, len(reqs))
		}
	}
}
