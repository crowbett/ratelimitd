package policy

import (
	"path"
	"testing"
)

func TestGlobMatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern, name string
		want          bool
	}{
		{"/v1/search*", "/v1/search", true},
		{"/v1/search*", "/v1/search/bulk", true}, // '*' spans '/', unlike path.Match
		{"/v1/search*", "/v1/sear", false},
		{"/v1/search/bulk*", "/v1/search/bulk/run", true},
		{"/v1/search/bulk*", "/v1/search/x", false},
		{"*", "", true},
		{"*", "anything/at/all", true},
		{"", "", true},
		{"", "x", false},
		{"a*b*c", "abc", true},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXbYY", false},
		{"a*b*c", "ac", false},
		{"*.json", "config.json", true},
		{"*.json", "config.jsonx", false},
		{"/users/?", "/users/1", true},
		{"/users/?", "/users/12", false},
		{"/users/?", "/users/", false},
		{"/users/[0-9]*", "/users/42/orders", true},
		{"/users/[0-9]*", "/users/x", false},
		{"/users/[^0-9]*", "/users/x", true},
		{"/users/[^0-9]*", "/users/4", false},
		{"[abc]", "b", true},
		{"[abc]", "d", false},
		{"[a-cx-z]", "y", true},
		{"[a-cx-z]", "m", false},
		{`\*`, "*", true},
		{`\*`, "x", false},
		{`[\]]`, "]", true},
		{"trusted-*", "trusted-ops", true},
		{"trusted-*", "untrusted-ops", false},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"*/end", "x/y/end", true},
		{"*/end", "x/y/end/", false},
		{"**", "a/b/c", true},
	}
	for _, tc := range tests {
		t.Run(tc.pattern+"~"+tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := path.Match(tc.pattern, ""); err != nil {
				t.Fatalf("pattern %q is not valid path.Match syntax: %v", tc.pattern, err)
			}
			if got := globMatch(tc.pattern, tc.name); got != tc.want {
				t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
			}
		})
	}
}

// TestGlobMatchAgreesWithPathMatch: for names without '/', the two matchers
// are the same function.
func TestGlobMatchAgreesWithPathMatch(t *testing.T) {
	t.Parallel()
	patterns := []string{"*", "a*", "*a", "a*b", "?", "a?", "[abc]*", "[^a]*", "*[0-9]", `a\*`, "abc", "", "a**b", "*?*"}
	names := []string{"", "a", "b", "ab", "ba", "abc", "a*", "a1", "1", "xyz", "aab", "abb"}
	for _, p := range patterns {
		for _, n := range names {
			want, err := path.Match(p, n)
			if err != nil {
				t.Fatalf("path.Match(%q): %v", p, err)
			}
			if got := globMatch(p, n); got != want {
				t.Errorf("globMatch(%q, %q) = %v, path.Match = %v", p, n, got, want)
			}
		}
	}
}

func TestRouteGlobsDisjoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a, b string
		want bool
	}{
		{"/a", "/b", true},
		{"/a", "/a", false},
		{"/a/*", "/a/x", false},
		{"/a/*", "/b/x", true},
		{"/a/*", "/b/*", false}, // two patterns: assumed to overlap
		{"/v1/search*", "/v1/search/bulk", false},
	}
	for _, tc := range tests {
		if got := routeGlobsDisjoint(tc.a, tc.b); got != tc.want {
			t.Errorf("routeGlobsDisjoint(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
		if got := routeGlobsDisjoint(tc.b, tc.a); got != tc.want {
			t.Errorf("routeGlobsDisjoint(%q, %q) = %v, want %v", tc.b, tc.a, got, tc.want)
		}
	}
}

func TestSortedCosts(t *testing.T) {
	t.Parallel()
	got := sortedCosts(map[string]int{"/a/*": 1, "/a/b/*": 2, "/z/*": 3, "*": 4})
	want := []costRule{{"/a/b/*", 2}, {"/a/*", 1}, {"/z/*", 3}, {"*", 4}}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if sortedCosts(nil) != nil {
		t.Fatal("nil map should give nil")
	}
}

func TestListRules(t *testing.T) {
	t.Parallel()
	tests := []struct {
		entry   string
		subject string
		ds      []Descriptor
		want    bool
		wantErr bool
	}{
		{entry: "10.0.0.0/8", subject: "10.200.1.1", want: true},
		{entry: "10.0.0.0/8", subject: "11.0.0.1", want: false},
		{entry: "10.0.0.0/8", subject: "k1", ds: []Descriptor{{Key: "ip", Value: "10.1.1.1"}}, want: true},
		{entry: "10.0.0.0/8", subject: "k1", ds: []Descriptor{{Key: "ip", Value: "not-an-ip"}}, want: false},
		{entry: "203.0.113.7", subject: "203.0.113.7", want: true},
		{entry: "203.0.113.7", subject: "203.0.113.8", want: false},
		{entry: "2001:db8::/32", subject: "2001:db8::1", want: true},
		{entry: "banned-*", subject: "banned-1", want: true},
		{entry: "banned-*", subject: "ok-1", want: false},
		{entry: "exact", subject: "exact", want: true},
		{entry: "exact", subject: "exactly", want: false},
		{entry: "", wantErr: true},
		{entry: "[", wantErr: true},
	}
	for _, tc := range tests {
		r, err := parseListRule(tc.entry)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseListRule(%q) accepted", tc.entry)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseListRule(%q): %v", tc.entry, err)
			continue
		}
		if got := r.matches(tc.subject, tc.ds); got != tc.want {
			t.Errorf("rule %q matches(%q, %v) = %v, want %v", tc.entry, tc.subject, tc.ds, got, tc.want)
		}
	}
}
