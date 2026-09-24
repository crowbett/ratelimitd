// Package policy is the policy model: the policies.yaml schema, strict
// parsing, validation, key templates, matching, allow/deny lists and the
// Registry (atomic snapshot, mtime-poll watcher, admin upsert).
//
// Owner: workstream A (algorithms & policy).
package policy

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// NameRE bounds policy and tenant names so a metric label cannot be injected.
var NameRE = regexp.MustCompile(`^[a-z0-9._-]{1,32}$`)

// Descriptor is the Envoy RLS key/value shape.
type Descriptor struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

// Match narrows which requests a policy applies to; every listed field must match.
type Match struct {
	Routes      []string          `yaml:"routes,omitempty"`      // path globs, e.g. "/v1/search*"
	Methods     []string          `yaml:"methods,omitempty"`     // HTTP methods or gRPC full methods
	Descriptors map[string]string `yaml:"descriptors,omitempty"` // key -> exact value
}

// Policy is one limiter rule as written in policies.yaml.
type Policy struct {
	Name      string         `yaml:"name"`
	Algorithm string         `yaml:"algorithm,omitempty"` // gcra | token_bucket | sliding_window | sliding_window_exact; "" = defaults.algorithm
	Rate      int64          `yaml:"rate"`
	Per       time.Duration  `yaml:"per"`
	Burst     int64          `yaml:"burst"`
	Key       string         `yaml:"key"` // template: "{tenant}:{apikey}" / "{ip}" / "{route}:{method}"
	Match     Match          `yaml:"match,omitempty"`
	Cost      map[string]int `yaml:"cost,omitempty"`    // route glob -> cost, default defaults.cost
	Enabled   *bool          `yaml:"enabled,omitempty"` // nil = true
	// Shared makes the policy ONE counter for every subject (per tenant when
	// the key template is "{tenant}", else one per policy) instead of one
	// per subject. Its key cannot share the subject's hash slot, so it never
	// joins the subject's script call: nodes lease chunks of its budget from
	// Redis (internal/store/lease). Templates may use only {tenant} and
	// literal text; sliding_window_exact is not supported.
	Shared bool `yaml:"shared,omitempty"`
}

// IsEnabled reports whether the policy is active (Enabled nil means true).
func (p Policy) IsEnabled() bool { return p.Enabled == nil || *p.Enabled }

// Params normalises the policy into limit.Params using algo when
// p.Algorithm is empty.
func (p Policy) Params(defaultAlgo string) (limit.Params, error) {
	name := p.Algorithm
	if name == "" {
		name = defaultAlgo
	}
	algo, err := limit.ParseAlgorithm(name)
	if err != nil {
		return limit.Params{}, fmt.Errorf("policy %q: %w", p.Name, err)
	}
	params, err := limit.NewParams(algo, p.Rate, p.Per, p.Burst)
	if err != nil {
		return limit.Params{}, fmt.Errorf("policy %q: %w", p.Name, err)
	}
	return params, nil
}

// Tenant groups the policies of one tenant. Allow and Deny are CIDRs or
// subject globs; a hit short-circuits before any backend is touched.
type Tenant struct {
	ID       string   `yaml:"-"` // the map key in policies.yaml
	Default  string   `yaml:"default,omitempty"`
	Allow    []string `yaml:"allow,omitempty"`
	Deny     []string `yaml:"deny,omitempty"`
	Policies []Policy `yaml:"policies"`
}

// Defaults are file-wide fallbacks.
type Defaults struct {
	Algorithm      string `yaml:"algorithm,omitempty"`        // default "gcra"
	OnBackendError string `yaml:"on_backend_error,omitempty"` // local | fail_open | fail_closed
	Cost           int    `yaml:"cost,omitempty"`             // default 1
}

// Set is one parsed policies.yaml. Version is the content hash of the bytes
// it was parsed from; LoadedAt is when.
type Set struct {
	SchemaVersion int               `yaml:"version"`
	Defaults      Defaults          `yaml:"defaults,omitempty"`
	Tenants       map[string]Tenant `yaml:"tenants,omitempty"`
	Global        []Policy          `yaml:"global,omitempty"`
	Version       string            `yaml:"-"`
	LoadedAt      time.Time         `yaml:"-"`
}

// Validate reports every problem at once: schema version 1, names match
// NameRE, rate > 0, burst >= 1, per >= 1ms, a known algorithm, tenant
// defaults that exist, costs >= 1, well-formed route/cost globs and
// allow/deny entries, sliding-window burst >= rate, global names that do not
// shadow tenant policy names, key templates whose placeholders resolve to
// {tenant} {subject} {apikey} {ip} {route} {method} or a key under
// match.descriptors, and the hash-tag invariant: every pair of enabled
// policies that can match one request of a tenant (tenant policies plus
// globals) must use the same tag placeholder (see template.go), so a
// multi-policy Apply never straddles Redis Cluster slots (CROSSSLOT).
func (s Set) Validate() error {
	var errs []error
	if s.SchemaVersion != 1 {
		errs = append(errs, fmt.Errorf("version must be 1, got %d", s.SchemaVersion))
	}
	algo := s.Defaults.Algorithm
	if algo == "" {
		algo = limit.GCRA.String()
	}
	if _, err := limit.ParseAlgorithm(algo); err != nil {
		errs = append(errs, fmt.Errorf("defaults.algorithm: %w", err))
	}
	switch s.Defaults.OnBackendError {
	case "", "local", "fail_open", "fail_closed":
	default:
		errs = append(errs, fmt.Errorf("defaults.on_backend_error must be local, fail_open or fail_closed, got %q", s.Defaults.OnBackendError))
	}
	if s.Defaults.Cost < 0 {
		errs = append(errs, fmt.Errorf("defaults.cost must be >= 1, got %d", s.Defaults.Cost))
	}
	tenantIDs := make([]string, 0, len(s.Tenants))
	for id := range s.Tenants {
		tenantIDs = append(tenantIDs, id)
	}
	sort.Strings(tenantIDs)
	for _, id := range tenantIDs {
		t := s.Tenants[id]
		if !NameRE.MatchString(id) {
			errs = append(errs, fmt.Errorf("tenant %q: name must match %s", id, NameRE))
		}
		errs = append(errs, validatePolicies("tenant "+id, t.Policies, algo)...)
		if t.Default != "" && !hasPolicy(t.Policies, t.Default) {
			errs = append(errs, fmt.Errorf("tenant %q: default policy %q not found", id, t.Default))
		}
		for _, e := range t.Allow {
			if _, err := parseListRule(e); err != nil {
				errs = append(errs, fmt.Errorf("tenant %q: allow %q: %w", id, e, err))
			}
		}
		for _, e := range t.Deny {
			if _, err := parseListRule(e); err != nil {
				errs = append(errs, fmt.Errorf("tenant %q: deny %q: %w", id, e, err))
			}
		}
		for i := range s.Global {
			if hasPolicy(t.Policies, s.Global[i].Name) {
				errs = append(errs, fmt.Errorf("global policy %q shadows tenant %q policy of the same name", s.Global[i].Name, id))
			}
		}
		errs = append(errs, validateHashTags("tenant "+id, t.Policies, s.Global)...)
	}
	errs = append(errs, validatePolicies("global", s.Global, algo)...)
	errs = append(errs, validateHashTags("global", nil, s.Global)...)
	return errors.Join(errs...)
}

// validateHashTags is the hash-tag invariant: among the enabled policies
// that can co-match one request (own policies plus globals), every pair
// that is not provably disjoint must share the tag placeholder.
func validateHashTags(scope string, own, global []Policy) []error {
	type tagged struct {
		p   *Policy
		tag string
	}
	all := make([]tagged, 0, len(own)+len(global))
	for _, ps := range [][]Policy{own, global} {
		for i := range ps {
			p := &ps[i]
			if !p.IsEnabled() || p.Shared {
				continue // a shared policy never joins the subject's call
			}
			phs, err := parseTemplate(p.Key)
			if err != nil {
				continue // reported by validatePolicies
			}
			if tag := tagOf(phs); tag != "" {
				all = append(all, tagged{p: p, tag: tag})
			}
		}
	}
	var errs []error
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			a, b := all[i], all[j]
			if a.tag == b.tag || disjoint(&a.p.Match, &b.p.Match) {
				continue
			}
			errs = append(errs, fmt.Errorf("%s: policies %q (key %q) and %q (key %q) can match one request but hash to different tags {%s} vs {%s}; use the same {subject}/descriptor placeholder or make their match rules disjoint",
				scope, a.p.Name, a.p.Key, b.p.Name, b.p.Key, a.tag, b.tag))
		}
	}
	return errs
}

func validatePolicies(scope string, ps []Policy, defaultAlgo string) []error {
	var errs []error
	seen := make(map[string]bool, len(ps))
	for i := range ps {
		p := &ps[i]
		if !NameRE.MatchString(p.Name) {
			errs = append(errs, fmt.Errorf("%s: policy name %q must match %s", scope, p.Name, NameRE))
		}
		if seen[p.Name] {
			errs = append(errs, fmt.Errorf("%s: duplicate policy %q", scope, p.Name))
		}
		seen[p.Name] = true
		if _, err := p.Params(defaultAlgo); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", scope, err))
		}
		if _, err := checkTemplate(p); err != nil {
			errs = append(errs, fmt.Errorf("%s: policy %q: %w", scope, p.Name, err))
		}
		if params, err := p.Params(defaultAlgo); err == nil &&
			(params.Algo == limit.SlidingWindow || params.Algo == limit.SlidingWindowExact) && p.Burst < p.Rate {
			errs = append(errs, fmt.Errorf("%s: policy %q: %s ignores burst, which must be >= rate (got %d < %d)", scope, p.Name, params.Algo, p.Burst, p.Rate))
		}
		if params, err := p.Params(defaultAlgo); err == nil && p.Shared && params.Algo == limit.SlidingWindowExact {
			errs = append(errs, fmt.Errorf("%s: policy %q: shared policies support gcra, token_bucket and sliding_window, not %s", scope, p.Name, params.Algo))
		}
		for _, g := range p.Match.Routes {
			if err := checkGlob("match.routes", g); err != nil {
				errs = append(errs, fmt.Errorf("%s: policy %q: %w", scope, p.Name, err))
			}
		}
		for _, m := range p.Match.Methods {
			if m == "" {
				errs = append(errs, fmt.Errorf("%s: policy %q: match.methods: empty method", scope, p.Name))
			}
		}
		for k := range p.Match.Descriptors {
			if k == "" {
				errs = append(errs, fmt.Errorf("%s: policy %q: match.descriptors: empty key", scope, p.Name))
			}
		}
		for glob, c := range p.Cost {
			if err := checkGlob("cost", glob); err != nil {
				errs = append(errs, fmt.Errorf("%s: policy %q: %w", scope, p.Name, err))
			}
			if c < 1 {
				errs = append(errs, fmt.Errorf("%s: policy %q: cost for %q must be >= 1, got %d", scope, p.Name, glob, c))
			}
		}
	}
	return errs
}

func hasPolicy(ps []Policy, name string) bool {
	for i := range ps {
		if ps[i].Name == name {
			return true
		}
	}
	return false
}
