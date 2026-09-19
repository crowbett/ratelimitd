// Package policy is the policy model: the policies.yaml schema, strict
// parsing, validation, key templates, matching, allow/deny lists and the
// Registry (atomic snapshot, mtime-poll watcher, admin upsert).
//
// Owner: workstream A (algorithms & policy). The YAML schema types, Parse and
// the numeric part of Validate are real; template resolution, matching, the
// hash-tag invariant check, Resolve, Watch and Upsert are stubs.
package policy

import (
	"errors"
	"fmt"
	"regexp"
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
// defaults that exist, costs >= 1.
//
// PARTIAL: two rules from docs/PLAN.md are not yet enforced and belong to
// workstream A: key templates must reference only {tenant} {subject} {ip}
// {route} {method} or a descriptor key, and every policy that can match one
// tenant request must render into one hash tag (the "Cluster hash tags" risk).
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
	for id, t := range s.Tenants {
		if !NameRE.MatchString(id) {
			errs = append(errs, fmt.Errorf("tenant %q: name must match %s", id, NameRE))
		}
		errs = append(errs, validatePolicies("tenant "+id, t.Policies, algo)...)
		if t.Default != "" && !hasPolicy(t.Policies, t.Default) {
			errs = append(errs, fmt.Errorf("tenant %q: default policy %q not found", id, t.Default))
		}
	}
	errs = append(errs, validatePolicies("global", s.Global, algo)...)
	return errors.Join(errs...)
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
		if p.Key == "" {
			errs = append(errs, fmt.Errorf("%s: policy %q: key template is required", scope, p.Name))
		}
		for glob, c := range p.Cost {
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
