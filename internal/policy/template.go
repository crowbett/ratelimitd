package policy

import (
	"errors"
	"fmt"
	"strings"
)

// Key templates.
//
// A template is literal text with "{name}" placeholders. Placeholders resolve
// against the request:
//
//	{tenant} {subject} {route} {method}  request fields
//	{apikey} {ip} {<key>}                the descriptor with that key ("" when absent)
//
// The key a policy renders is a pure function of the placeholder values;
// literal text is a separator and does not reach the key. The first {subject}
// or descriptor placeholder is the policy's TAG placeholder: its value is the
// subject half of the Redis hash tag, so every request lands in
// rl:{t=<tenant>|s=<tag value>}:p=<policy>[:<extra>...], where extras are the
// remaining placeholder values in template order. Validate requires every
// policy that can co-match a request to use the same tag placeholder, which
// is what keeps a multi-policy Apply in one slot (docs/PLAN.md, "Cluster hash
// tags"); {tenant} adds nothing (t= is always the request tenant) and is
// accepted for readability.

// Placeholder kinds.
const (
	phTenant uint8 = iota + 1
	phSubject
	phRoute
	phMethod
	phDescriptor
)

// Descriptor keys that are always known to a template.
var builtinDescriptors = map[string]bool{"apikey": true, "ip": true}

type placeholder struct {
	kind uint8
	key  string // descriptor key for phDescriptor
	name string // as written, for messages and identity
}

// tagPlaceholder reports whether p can carry the subject half of the tag.
func (p placeholder) tagPlaceholder() bool { return p.kind == phSubject || p.kind == phDescriptor }

// parseTemplate splits t into its placeholders, in order. Literal text is
// validated (no stray braces) and dropped.
func parseTemplate(t string) ([]placeholder, error) {
	if t == "" {
		return nil, errors.New("key template is required")
	}
	var out []placeholder
	rest := t
	for {
		open := strings.IndexByte(rest, '{')
		if open < 0 {
			if strings.IndexByte(rest, '}') >= 0 {
				return nil, fmt.Errorf("key template %q: unmatched '}'", t)
			}
			break
		}
		if strings.IndexByte(rest[:open], '}') >= 0 {
			return nil, fmt.Errorf("key template %q: unmatched '}'", t)
		}
		closeAt := strings.IndexByte(rest[open:], '}')
		if closeAt < 0 {
			return nil, fmt.Errorf("key template %q: unmatched '{'", t)
		}
		name := rest[open+1 : open+closeAt]
		if !validPlaceholderName(name) {
			return nil, fmt.Errorf("key template %q: bad placeholder {%s}", t, name)
		}
		out = append(out, newPlaceholder(name))
		rest = rest[open+closeAt+1:]
	}
	return out, nil
}

func validPlaceholderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-', c == '.':
		default:
			return false
		}
	}
	return true
}

func newPlaceholder(name string) placeholder {
	switch name {
	case "tenant":
		return placeholder{kind: phTenant, name: name}
	case "subject":
		return placeholder{kind: phSubject, name: name}
	case "route":
		return placeholder{kind: phRoute, name: name}
	case "method":
		return placeholder{kind: phMethod, name: name}
	}
	return placeholder{kind: phDescriptor, key: name, name: name}
}

// tagOf returns the tag placeholder name of a parsed template, or "".
func tagOf(phs []placeholder) string {
	for _, p := range phs {
		if p.tagPlaceholder() {
			return p.name
		}
	}
	return ""
}

// checkTemplate validates one policy's template against its known set: the
// builtin placeholders plus the descriptor keys the policy matches on.
func checkTemplate(p *Policy) ([]placeholder, error) {
	phs, err := parseTemplate(p.Key)
	if err != nil {
		return nil, err
	}
	if p.Shared {
		// One counter across subjects: only the tenant may split it (a
		// bounded set, so the per-node lease pools stay bounded too).
		for _, ph := range phs {
			if ph.kind != phTenant {
				return nil, fmt.Errorf("key template %q: a shared policy may only use {tenant}, not {%s}", p.Key, ph.name)
			}
		}
		return phs, nil
	}
	for _, ph := range phs {
		if ph.kind != phDescriptor || builtinDescriptors[ph.key] {
			continue
		}
		if _, ok := p.Match.Descriptors[ph.key]; !ok {
			return nil, fmt.Errorf("key template %q: unknown placeholder {%s} (known: {tenant} {subject} {apikey} {ip} {route} {method} or a key under match.descriptors)", p.Key, ph.name)
		}
	}
	if tagOf(phs) == "" {
		return nil, fmt.Errorf("key template %q: needs {subject} or a descriptor placeholder to form the hash tag", p.Key)
	}
	return phs, nil
}
