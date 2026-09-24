package policy

import (
	"fmt"
	"strings"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// snapshot is a Set plus everything Resolve needs precomputed: parsed
// templates, limit.Params, sorted cost tables and compiled allow/deny lists.
// It is immutable once built and published through Registry.snap.
type snapshot struct {
	set         *Set
	tenants     map[string]*compiledTenant
	global      []compiledPolicy
	defaultCost int
}

type compiledTenant struct {
	policies []compiledPolicy
	def      int // index of Tenant.Default in policies, -1 when unset
	allow    []listRule
	deny     []listRule
}

type compiledPolicy struct {
	name         string
	params       limit.Params
	placeholders []placeholder
	tag          int // index of the tag placeholder; -1 for a shared policy
	shared       bool
	routes       []string
	methods      []string
	descriptors  map[string]string
	costs        []costRule
}

// compile precomputes s for Resolve. It assumes s passed Validate and
// returns an error only for what Validate would have rejected.
func compile(s *Set) (*snapshot, error) {
	algo := s.Defaults.Algorithm
	if algo == "" {
		algo = limit.GCRA.String()
	}
	out := &snapshot{set: s, tenants: make(map[string]*compiledTenant, len(s.Tenants)), defaultCost: s.Defaults.Cost}
	if out.defaultCost < 1 {
		out.defaultCost = 1
	}
	var err error
	if out.global, err = compilePolicies(s.Global, algo); err != nil {
		return nil, fmt.Errorf("global: %w", err)
	}
	for id, t := range s.Tenants {
		ct := &compiledTenant{def: -1}
		if ct.policies, err = compilePolicies(t.Policies, algo); err != nil {
			return nil, fmt.Errorf("tenant %s: %w", id, err)
		}
		if t.Default != "" {
			ct.def = indexOf(ct.policies, t.Default) // -1 when disabled
		}
		if ct.allow, err = compileList(t.Allow); err != nil {
			return nil, fmt.Errorf("tenant %s: allow: %w", id, err)
		}
		if ct.deny, err = compileList(t.Deny); err != nil {
			return nil, fmt.Errorf("tenant %s: deny: %w", id, err)
		}
		out.tenants[id] = ct
	}
	return out, nil
}

func indexOf(ps []compiledPolicy, name string) int {
	for i := range ps {
		if ps[i].name == name {
			return i
		}
	}
	return -1
}

// compilePolicies keeps enabled policies only, in file order.
func compilePolicies(ps []Policy, defaultAlgo string) ([]compiledPolicy, error) {
	out := make([]compiledPolicy, 0, len(ps))
	for i := range ps {
		p := &ps[i]
		if !p.IsEnabled() {
			continue
		}
		params, err := p.Params(defaultAlgo)
		if err != nil {
			return nil, err
		}
		phs, err := checkTemplate(p)
		if err != nil {
			return nil, fmt.Errorf("policy %q: %w", p.Name, err)
		}
		cp := compiledPolicy{
			name:         p.Name,
			params:       params,
			placeholders: phs,
			tag:          -1,
			routes:       p.Match.Routes,
			methods:      p.Match.Methods,
			descriptors:  p.Match.Descriptors,
			costs:        sortedCosts(p.Cost),
			shared:       p.Shared,
		}
		for j := range phs {
			if phs[j].tagPlaceholder() {
				cp.tag = j
				break
			}
		}
		out = append(out, cp)
	}
	return out, nil
}

func compileList(entries []string) ([]listRule, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make([]listRule, 0, len(entries))
	for _, e := range entries {
		r, err := parseListRule(e)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// matches reports whether p applies to the request.
func (p *compiledPolicy) matches(ds []Descriptor, route, method string) bool {
	if len(p.routes) > 0 {
		hit := false
		for _, g := range p.routes {
			if globMatch(g, route) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	if len(p.methods) > 0 {
		hit := false
		for _, m := range p.methods {
			if m == method {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	for k, want := range p.descriptors {
		if want == AnyValue {
			if !descriptorPresent(ds, k) {
				return false
			}
			continue
		}
		if descriptorValue(ds, k) != want {
			return false
		}
	}
	return true
}

// cost picks the route's cost from the table, else the file default.
func (p *compiledPolicy) cost(route string, def int) int {
	for i := range p.costs {
		if globMatch(p.costs[i].glob, route) {
			return p.costs[i].cost
		}
	}
	return def
}

// request is one Resolve call's inputs; value lookups read from it.
type request struct {
	tenant, subject, route, method string
	ds                             []Descriptor
}

func (r *request) value(ph *placeholder) string {
	switch ph.kind {
	case phTenant:
		return r.tenant
	case phSubject:
		return r.subject
	case phRoute:
		return r.route
	case phMethod:
		return r.method
	case phDescriptor:
		return descriptorValue(r.ds, ph.key)
	}
	return ""
}

// keyLen is the rendered key length, so all keys of one Resolve fit one buffer.
func (p *compiledPolicy) keyLen(r *request) int {
	if p.shared {
		n := len(limit.KeyPrefix) + len("{g=") + len(p.name) + len("}:p=") + len(p.name)
		if len(p.placeholders) > 0 { // {tenant} only (checkTemplate)
			n += len("|t=") + len(r.tenant)
		}
		return n
	}
	n := len(limit.KeyPrefix) + len("{t=") + len(r.tenant) + len("|s=") + len(r.value(&p.placeholders[p.tag])) + len("}:p=") + len(p.name)
	for j := range p.placeholders {
		if j == p.tag || p.placeholders[j].kind == phTenant {
			continue
		}
		n += 1 + len(r.value(&p.placeholders[j]))
	}
	return n
}

// writeKey appends the rendered key to b. A subject policy renders
// rl:{t=<tenant>|s=<tag>}:p=<policy>[:extras]; a shared policy renders
// rl:{g=<policy>[|t=<tenant>]}:p=<policy>, its own slot.
func (p *compiledPolicy) writeKey(b *strings.Builder, r *request) {
	if p.shared {
		b.WriteString(limit.KeyPrefix)
		b.WriteString("{g=")
		b.WriteString(p.name)
		if len(p.placeholders) > 0 {
			b.WriteString("|t=")
			b.WriteString(r.tenant)
		}
		b.WriteString("}:p=")
		b.WriteString(p.name)
		return
	}
	b.WriteString(limit.KeyPrefix)
	b.WriteString("{t=")
	b.WriteString(r.tenant)
	b.WriteString("|s=")
	b.WriteString(r.value(&p.placeholders[p.tag]))
	b.WriteString("}:p=")
	b.WriteString(p.name)
	for j := range p.placeholders {
		if j == p.tag || p.placeholders[j].kind == phTenant {
			continue
		}
		b.WriteByte(':')
		b.WriteString(r.value(&p.placeholders[j]))
	}
}

// resolve is Registry.Resolve against one snapshot. It allocates the []Req
// and one buffer holding every key (each Key is a substring of it; the
// buffer is grown once to the exact size so earlier substrings stay valid).
func (s *snapshot) resolve(r *request) ([]limit.Req, *limit.Decision, error) {
	ct, ok := s.tenants[r.tenant]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %q", limit.ErrUnknownTenant, r.tenant)
	}
	if anyRule(ct.deny, r.subject, r.ds) {
		return nil, &limit.Decision{Allowed: false, Policy: "deny"}, nil
	}
	if anyRule(ct.allow, r.subject, r.ds) {
		return nil, &limit.Decision{Allowed: true, Policy: "allow"}, nil
	}
	// Pass 1: count matches and size the key buffer (no allocation).
	n, total := 0, 0
	tenantHit := false
	for i := range ct.policies {
		if ct.policies[i].matches(r.ds, r.route, r.method) {
			n++
			total += ct.policies[i].keyLen(r)
			tenantHit = true
		}
	}
	useDefault := !tenantHit && ct.def >= 0
	if useDefault {
		n++
		total += ct.policies[ct.def].keyLen(r)
	}
	for i := range s.global {
		if s.global[i].matches(r.ds, r.route, r.method) {
			n++
			total += s.global[i].keyLen(r)
		}
	}
	if n == 0 {
		return nil, nil, limit.ErrNoPolicy
	}
	// Pass 2: render.
	reqs := make([]limit.Req, 0, n)
	var b strings.Builder
	b.Grow(total)
	if useDefault {
		reqs = s.appendReq(reqs, &b, &ct.policies[ct.def], r)
	} else {
		for i := range ct.policies {
			if ct.policies[i].matches(r.ds, r.route, r.method) {
				reqs = s.appendReq(reqs, &b, &ct.policies[i], r)
			}
		}
	}
	for i := range s.global {
		if s.global[i].matches(r.ds, r.route, r.method) {
			reqs = s.appendReq(reqs, &b, &s.global[i], r)
		}
	}
	return reqs, nil, nil
}

// appendReq renders p's key into b and appends the Req; the key is a view of
// b's buffer, which never reallocates because resolve grew it to size.
func (s *snapshot) appendReq(reqs []limit.Req, b *strings.Builder, p *compiledPolicy, r *request) []limit.Req {
	start := b.Len()
	p.writeKey(b, r)
	return append(reqs, limit.Req{
		Key:    b.String()[start:],
		Params: p.params,
		Cost:   p.cost(r.route, s.defaultCost),
		Policy: p.name,
		Shared: p.shared,
	})
}
