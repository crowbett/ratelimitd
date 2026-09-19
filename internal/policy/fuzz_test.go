package policy

import (
	"bytes"
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// Fuzz targets for every boundary where bytes from outside the process
// become a structure: the YAML policy file (Parse + Validate + Write round
// trip), key templates, route globs, allow/deny entries and the rendered
// keys of Resolve. Each asserts "never panics" plus the invariant the
// callers rely on. Run for 60 s each:
//
//	go test -run '^$' -fuzz '^FuzzParse$' -fuzztime 60s ./internal/policy/

// maxPolicyFile bounds what one Parse call may consume from the fuzzer: the
// operator's file is trusted, but the admin Upsert path re-parses what it
// wrote, and a 1 MiB policy set is already absurd.
const maxPolicyFile = 1 << 20

// FuzzParse: any bytes -> Parse never panics; a Set that validates compiles,
// resolves, renders deterministically (Write twice -> same bytes) and
// survives a Write -> Parse -> Validate round trip with the same version.
func FuzzParse(f *testing.F) {
	f.Add([]byte(baseYAML))
	f.Add([]byte("version: 1\n"))
	f.Add([]byte(""))
	f.Add([]byte("version: 2\ntenants: {}\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n      - { name: b, rate: 1, per: 1s, burst: 1, key: \"{ip}\" }\n"))
	f.Add([]byte("version: 1\nglobal:\n  - { name: g, algorithm: sliding_window, rate: 60, per: 1m, burst: 60, key: \"{subject}:{route}\", match: { routes: [\"/v1/*\"] }, cost: { \"/v1/x*\": 3 } }\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    default: nope\n    allow: [\"10.0.0.0/8\"]\n    deny: [\"[\"]\n    policies: []\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\", enabled: false }\n"))
	f.Add([]byte("version: 1\nunknown: 1\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, algorithm: token_bucket, rate: 1, per: 24h, burst: 1, key: \"{subject}\" }\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 9007199254740993, key: \"{subject}\" }\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\", match: { descriptors: { k: \"*\" } } }\n      - { name: b, rate: 1, per: 1s, burst: 1, key: \"{k}\", match: { descriptors: { k: v } } }\n"))
	f.Add([]byte("version: 1\ntenants:\n  \"bad name!\":\n    policies: []\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject\" }\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\", match: { routes: [\"[\"] } }\n"))
	f.Add([]byte("version: 1\ntenants:\n  acme:\n    policies:\n      - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\nglobal:\n  - { name: a, rate: 1, per: 1s, burst: 1, key: \"{subject}\" }\n"))
	f.Add([]byte("- 1\n- 2\n"))
	f.Add([]byte("version: !!binary AAAA\n"))
	f.Add([]byte("version: 1\ntenants: &a\n  acme: *a\n"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > maxPolicyFile {
			t.Skip()
		}
		set, err := Parse(bytes.NewReader(raw))
		if err != nil {
			if set.Version != "" || set.Tenants != nil {
				t.Fatalf("Parse returned a Set alongside error %v", err)
			}
			return
		}
		if set.Version == "" || set.LoadedAt.IsZero() {
			t.Fatalf("Parse stamped Version %q LoadedAt %v", set.Version, set.LoadedAt)
		}
		for id, tn := range set.Tenants {
			if tn.ID != id {
				t.Fatalf("tenant %q has ID %q", id, tn.ID)
			}
		}
		if err := set.Validate(); err != nil {
			return // a rejected set is never compiled; Validate's strictness is table-tested
		}
		snap, err := compile(&set)
		if err != nil {
			t.Fatalf("valid set does not compile: %v\n%s", err, raw)
		}
		checkResolve(t, snap, set)

		var a, b bytes.Buffer
		if err := Write(&a, set); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := Write(&b, set); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if !bytes.Equal(a.Bytes(), b.Bytes()) {
			t.Fatalf("Write is not deterministic:\n%s\n---\n%s", a.Bytes(), b.Bytes())
		}
		again, err := Parse(bytes.NewReader(a.Bytes()))
		if err != nil {
			t.Fatalf("re-parse of Write output: %v\n%s", err, a.Bytes())
		}
		if err := again.Validate(); err != nil {
			t.Fatalf("re-parsed set is invalid: %v\n%s", err, a.Bytes())
		}
		var c bytes.Buffer
		if err := Write(&c, again); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if !bytes.Equal(a.Bytes(), c.Bytes()) {
			t.Fatalf("Write -> Parse -> Write is not a fixed point:\n%s\n---\n%s", a.Bytes(), c.Bytes())
		}
		if again.Version != contentHash(a.Bytes()) {
			t.Fatalf("Version %q is not the content hash of the bytes parsed", again.Version)
		}
	})
}

// checkResolve drives every tenant of a compiled set through resolve with a
// few request shapes and asserts the key invariants: prefix, one hash tag
// per call, policy name in the key, cost and emission >= 1.
func checkResolve(t *testing.T, snap *snapshot, set Set) {
	t.Helper()
	shapes := []request{
		{subject: "s1"},
		{subject: "10.0.0.1", route: "/v1/search/bulk", method: "POST", ds: []Descriptor{{Key: "ip", Value: "203.0.113.7"}, {Key: "apikey", Value: "k"}}},
		{subject: "banned-x", route: "/admin/x", method: "PUT", ds: []Descriptor{{Key: "k", Value: "v"}}},
		{subject: "a}b{c", route: "}{", method: ""},
	}
	tenants := make([]string, 0, len(set.Tenants)+1)
	for id := range set.Tenants {
		tenants = append(tenants, id)
	}
	tenants = append(tenants, "no-such-tenant")
	for _, tenant := range tenants {
		for _, sh := range shapes {
			r := sh
			r.tenant = tenant
			reqs, short, err := snap.resolve(&r)
			switch {
			case err != nil:
				if !errors.Is(err, limit.ErrUnknownTenant) && !errors.Is(err, limit.ErrNoPolicy) {
					t.Fatalf("resolve(%q): unexpected error %v", tenant, err)
				}
				if reqs != nil || short != nil {
					t.Fatalf("resolve(%q): error %v with reqs %v short %v", tenant, err, reqs, short)
				}
			case short != nil:
				if reqs != nil || (short.Policy != "deny" && short.Policy != "allow") {
					t.Fatalf("resolve(%q): short-circuit %+v with reqs %v", tenant, short, reqs)
				}
			default:
				if len(reqs) == 0 {
					t.Fatalf("resolve(%q): no reqs, no short, no error", tenant)
				}
				tag := ""
				for _, rq := range reqs {
					if !strings.HasPrefix(rq.Key, limit.KeyPrefix+"{t="+tenant+"|s=") {
						t.Fatalf("key %q lacks the tenant prefix", rq.Key)
					}
					if !strings.Contains(rq.Key, "}:p="+rq.Policy) {
						t.Fatalf("key %q does not carry policy %q", rq.Key, rq.Policy)
					}
					if rq.Cost < 1 || rq.Params.EmissionUS < 1 {
						t.Fatalf("req %+v has cost %d emission %d", rq, rq.Cost, rq.Params.EmissionUS)
					}
					ht := redisHashTag(rq.Key)
					if tag == "" {
						tag = ht
					} else if ht != tag {
						t.Fatalf("keys of one call straddle hash tags %q vs %q: %v", tag, ht, reqs)
					}
				}
			}
		}
	}
}

// redisHashTag is Redis Cluster's rule: the first '{' and the first '}' after
// it, when non-empty; else the whole key.
func redisHashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	closeAt := strings.IndexByte(key[open+1:], '}')
	if closeAt < 0 || closeAt == 0 {
		return key
	}
	return key[open+1 : open+1+closeAt]
}

// FuzzTemplate: parseTemplate never panics; an accepted template has
// balanced braces, only valid placeholder names, and renders a key whose
// placeholder values appear in order.
func FuzzTemplate(f *testing.F) {
	for _, s := range []string{
		"{tenant}:{subject}", "{subject}", "{ip}", "{apikey}:{route}:{method}", "{k}", "", "{", "}", "{}", "{{subject}}",
		"{subject", "subject}", "a{subject}b{route}c", "{sub ject}", "{tenant}", "{route}", "{subject}{subject}",
		"{a.b-c_D9}", "{ünïcode}", "{subject}}{", "x", "{{", "}}",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, tmpl string) {
		phs, err := parseTemplate(tmpl)
		if err != nil {
			if phs != nil {
				t.Fatalf("error %v with placeholders %v", err, phs)
			}
			return
		}
		if strings.Count(tmpl, "{") != len(phs) || strings.Count(tmpl, "}") != len(phs) {
			t.Fatalf("template %q accepted with %d placeholders but braces %d/%d", tmpl, len(phs), strings.Count(tmpl, "{"), strings.Count(tmpl, "}"))
		}
		for _, p := range phs {
			if !validPlaceholderName(p.name) {
				t.Fatalf("template %q: accepted invalid placeholder name %q", tmpl, p.name)
			}
			if !strings.Contains(tmpl, "{"+p.name+"}") {
				t.Fatalf("template %q: placeholder %q not literally present", tmpl, p.name)
			}
			if p.kind == phDescriptor && (p.key != p.name || p.key == "") {
				t.Fatalf("descriptor placeholder %+v has key %q", p, p.key)
			}
		}
		if len(phs) == 0 {
			return
		}
		// A policy built on this template must round-trip through
		// checkTemplate when every descriptor placeholder is matched.
		p := &Policy{Name: "p", Rate: 1, Per: time.Second, Burst: 1, Key: tmpl, Match: Match{Descriptors: map[string]string{}}}
		for _, ph := range phs {
			if ph.kind == phDescriptor {
				p.Match.Descriptors[ph.key] = AnyValue
			}
		}
		got, err := checkTemplate(p)
		if err != nil {
			if tagOf(phs) == "" {
				return // no {subject}/descriptor: rejected by design
			}
			t.Fatalf("checkTemplate(%q) with every descriptor matched: %v", tmpl, err)
		}
		if len(got) != len(phs) {
			t.Fatalf("checkTemplate returned %d placeholders, parseTemplate %d", len(got), len(phs))
		}
	})
}

// FuzzGlobMatch: globMatch never panics on any (pattern, name); for a
// pattern checkGlob accepts it agrees with path.Match whenever neither side
// contains '/' (the one documented divergence is '*' spanning '/'), and a
// literal pattern matches exactly itself.
func FuzzGlobMatch(f *testing.F) {
	seeds := [][2]string{
		{"/v1/search*", "/v1/search/bulk"},
		{"*", ""},
		{"?", "a"},
		{"[a-z]", "q"},
		{"[^a-z]", "Q"},
		{"[]", "x"},
		{"[", "x"},
		{`\*`, "*"},
		{`\`, `\`},
		{"a*b*c", "aXbYc"},
		{"a*b*c", "ac"},
		{"[a-]", "-"},
		{"[-a]", "-"},
		{"[\\]]", "]"},
		{"[a-\\]]", "b"},
		{"**", "anything/at/all"},
		{"[z-a]", "m"},
		{"[", ""},
		{"", ""},
		{"abc", "abc"},
		{"a?c", "a/c"},
		{"[[]", "["},
		{"[]a]", "]"},
		{"[^]a]", "b"},
		{"*[", "x["},
		{"x\\", "x"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, pattern, name string) {
		if len(pattern) > 256 || len(name) > 256 {
			t.Skip()
		}
		got := globMatch(pattern, name) // must not panic even on a malformed pattern
		if err := checkGlob("fuzz", pattern); err != nil {
			return
		}
		if !hasGlobMeta(pattern) && got != (pattern == name) {
			t.Fatalf("literal glob %q vs %q = %v", pattern, name, got)
		}
		if strings.ContainsRune(pattern, '/') || strings.ContainsRune(name, '/') {
			return
		}
		want, err := path.Match(pattern, name)
		if err != nil {
			t.Fatalf("checkGlob accepted %q but path.Match rejects it: %v", pattern, err)
		}
		if got != want {
			t.Fatalf("globMatch(%q, %q) = %v, path.Match = %v", pattern, name, got, want)
		}
	})
}

// FuzzListRule: parseListRule never panics; an accepted CIDR/IP entry is
// masked and matches its own address, an accepted glob matches like
// globMatch; matches never panics on any subject/descriptor.
func FuzzListRule(f *testing.F) {
	for _, s := range []string{"10.0.0.0/8", "203.0.113.7", "::1", "2001:db8::/32", "trusted-*", "", "[", "10.0.0.1/33", "1.2.3", "*", "?", "10.0.0.1/8"} {
		f.Add(s, "10.0.0.1", "203.0.113.7")
	}
	f.Add("banned-*", "banned-x", "")
	f.Add("2001:db8::/32", "2001:db8::1", "x")
	f.Fuzz(func(t *testing.T, entry, subject, ip string) {
		if len(entry) > 256 || len(subject) > 256 || len(ip) > 256 {
			t.Skip()
		}
		r, err := parseListRule(entry)
		if err != nil {
			if r != (listRule{}) {
				t.Fatalf("error %v with rule %+v", err, r)
			}
			return
		}
		ds := []Descriptor{{Key: "ip", Value: ip}, {Key: "other", Value: subject}}
		_ = r.matches(subject, ds)
		_ = r.matches(subject, nil)
		if r.cidr {
			if r.prefix != r.prefix.Masked() {
				t.Fatalf("CIDR rule %q not masked: %v", entry, r.prefix)
			}
			if !r.matches(r.prefix.Addr().String(), nil) {
				t.Fatalf("CIDR rule %q does not match its own base address %s", entry, r.prefix.Addr())
			}
			if !r.matches("not-an-ip", []Descriptor{{Key: "ip", Value: r.prefix.Addr().String()}}) {
				t.Fatalf("CIDR rule %q ignores the ip descriptor", entry)
			}
			return
		}
		if r.glob != entry || r.matches(subject, ds) != globMatch(entry, subject) {
			t.Fatalf("glob rule %q: %+v, matches(%q) = %v, globMatch = %v", entry, r, subject, r.matches(subject, ds), globMatch(entry, subject))
		}
	})
}

// FuzzResolve: arbitrary request fields against a fixed valid set (one
// policy per placeholder kind, a default, allow/deny lists, a global) never
// panic and always yield keys with the documented shape, one hash tag per
// call, and every rendered placeholder value in template order.
func FuzzResolve(f *testing.F) {
	set := parseYAML(f, `version: 1
defaults: { algorithm: gcra, cost: 2 }
tenants:
  acme:
    default: api
    allow: ["10.0.0.0/8", "trusted-*"]
    deny: ["203.0.113.7", "banned-*"]
    policies:
      - { name: api, rate: 1000, per: 1s, burst: 200, key: "{tenant}:{subject}" }
      - { name: search, algorithm: token_bucket, rate: 10, per: 1s, burst: 20, key: "{subject}:{route}:{method}",
          match: { routes: ["/v1/search*"] }, cost: { "/v1/search/bulk*": 10 } }
      - { name: keyed, rate: 5, per: 1s, burst: 5, key: "{subject}:{apikey}:{ip}:{k}", match: { descriptors: { k: "*" } } }
global:
  - { name: global_ingress, rate: 50000, per: 1s, burst: 5000, key: "{subject}" }
`)
	if err := set.Validate(); err != nil {
		f.Fatal(err)
	}
	snap, err := compile(&set)
	if err != nil {
		f.Fatal(err)
	}
	f.Add("acme", "s1", "/v1/search/bulk", "POST", "k", "v")
	f.Add("acme", "10.1.2.3", "", "", "", "")
	f.Add("acme", "banned-1", "/x", "GET", "ip", "1.2.3.4")
	f.Add("acme", "a}b{c|d", "}{:", "::", "apikey", "}")
	f.Add("other", "s", "", "", "", "")
	f.Add("", "", "", "", "", "")
	f.Add("acme", "s", "/v1/search", "GET", "k", "")
	f.Fuzz(func(t *testing.T, tenant, subject, route, method, dkey, dval string) {
		if len(tenant)+len(subject)+len(route)+len(method)+len(dkey)+len(dval) > 4096 {
			t.Skip()
		}
		ds := []Descriptor{{Key: dkey, Value: dval}, {Key: "ip", Value: dval}}
		r := request{tenant: tenant, subject: subject, route: route, method: method, ds: ds}
		reqs, short, err := snap.resolve(&r)
		if err != nil {
			if tenant == "acme" {
				t.Fatalf("resolve for a known tenant with a default: %v", err)
			}
			if !errors.Is(err, limit.ErrUnknownTenant) {
				t.Fatalf("unexpected error %v", err)
			}
			return
		}
		if short != nil {
			if !anyRule(snap.tenants[tenant].deny, subject, ds) && !anyRule(snap.tenants[tenant].allow, subject, ds) {
				t.Fatalf("short-circuit %+v without a list hit", short)
			}
			return
		}
		tag := ""
		for _, rq := range reqs {
			want := limit.KeyPrefix + "{t=" + tenant + "|s=" + subject + "}:p=" + rq.Policy
			if !strings.HasPrefix(rq.Key, want) {
				t.Fatalf("key %q, want prefix %q", rq.Key, want)
			}
			extra := rq.Key[len(want):]
			switch rq.Policy {
			case "api", "global_ingress":
				if extra != "" {
					t.Fatalf("key %q has extras %q", rq.Key, extra)
				}
			case "search":
				if extra != ":"+route+":"+method {
					t.Fatalf("key %q extras %q, want route+method", rq.Key, extra)
				}
				if !globMatch("/v1/search*", route) {
					t.Fatalf("search matched route %q", route)
				}
			case "keyed":
				if !descriptorPresent(ds, "k") {
					t.Fatalf("keyed matched without descriptor k: %v", ds)
				}
				if extra != ":"+descriptorValue(ds, "apikey")+":"+dval+":"+descriptorValue(ds, "k") {
					t.Fatalf("key %q extras %q", rq.Key, extra)
				}
			default:
				t.Fatalf("unknown policy %q in %+v", rq.Policy, rq)
			}
			ht := redisHashTag(rq.Key)
			if tag == "" {
				tag = ht
			} else if ht != tag {
				t.Fatalf("keys straddle hash tags %q vs %q", tag, ht)
			}
			if rq.Cost < 1 {
				t.Fatalf("cost %d", rq.Cost)
			}
		}
		// api has no match rule, so it (and the global) are in every call;
		// search and keyed exactly when their rule hits.
		got := map[string]bool{}
		for _, rq := range reqs {
			got[rq.Policy] = true
		}
		wantSearch, wantKeyed := globMatch("/v1/search*", route), descriptorPresent(ds, "k")
		if !got["api"] || !got["global_ingress"] || got["search"] != wantSearch || got["keyed"] != wantKeyed || len(reqs) != 2+btoi(wantSearch)+btoi(wantKeyed) {
			t.Fatalf("policies %v for route %q descriptors %v: %+v", got, route, ds, reqs)
		}
	})
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
