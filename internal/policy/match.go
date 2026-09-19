package policy

import (
	"errors"
	"fmt"
	"net/netip"
	"path"
	"sort"
	"strings"
	"unicode/utf8"
)

// Matching.
//
// Routes are globs in path.Match syntax ('*', '?', '[a-z]', backslash escapes)
// except that '*' also spans '/', so "/v1/search*" covers "/v1/search/bulk"
// as docs/PLAN.md's example intends. Methods are exact strings, descriptors
// are key -> exact value with "*" meaning "present with any value". Cost
// tables are route glob -> cost, most specific (longest glob) first.
// Allow/deny entries are CIDRs, single IPs (checked against the subject and
// the "ip" descriptor) or subject globs.

// AnyValue in match.descriptors accepts any value for the key.
const AnyValue = "*"

// costRule is one entry of a policy's cost table.
type costRule struct {
	glob string
	cost int
}

// sortedCosts orders a cost table longest-glob first so the most specific
// entry wins, then lexically for determinism.
func sortedCosts(m map[string]int) []costRule {
	if len(m) == 0 {
		return nil
	}
	out := make([]costRule, 0, len(m))
	for g, c := range m {
		out = append(out, costRule{glob: g, cost: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].glob) != len(out[j].glob) {
			return len(out[i].glob) > len(out[j].glob)
		}
		return out[i].glob < out[j].glob
	})
	return out
}

// checkGlob rejects a malformed pattern at load time (path.Match syntax).
func checkGlob(what, g string) error {
	if g == "" {
		return fmt.Errorf("%s: empty glob", what)
	}
	if _, err := path.Match(g, ""); err != nil {
		return fmt.Errorf("%s: glob %q: %w", what, g, err)
	}
	return nil
}

// globMatch matches name against a validated pattern without allocating:
// '*' matches any run of bytes (including '/'), '?' one UTF-8 rune, '[...]'
// a class of runes with ranges and '^' negation, a backslash escapes the
// next byte. Apart from '*' spanning '/', it is path.Match rune for rune
// (FuzzGlobMatch holds it to that oracle); it is the linear backtracking
// matcher (one pending '*' at a time).
func globMatch(pattern, name string) bool {
	px, nx := 0, 0
	nextPx, nextNx := 0, 0
	for px < len(pattern) || nx < len(name) {
		if px < len(pattern) {
			switch c := pattern[px]; c {
			case '?':
				if nx < len(name) {
					_, w := utf8.DecodeRuneInString(name[nx:])
					px++
					nx += w
					continue
				}
			case '*':
				nextPx, nextNx = px, nx+1
				px++
				continue
			case '[':
				if nx < len(name) {
					r, w := utf8.DecodeRuneInString(name[nx:])
					if n, ok := matchClass(pattern[px:], r); ok {
						px += n
						nx += w
						continue
					}
				}
			case '\\':
				if px+1 < len(pattern) && nx < len(name) && name[nx] == pattern[px+1] {
					px += 2
					nx++
					continue
				}
			default:
				if nx < len(name) && name[nx] == c {
					px++
					nx++
					continue
				}
			}
		}
		if nextNx > 0 && nextNx <= len(name) {
			px, nx = nextPx, nextNx
			continue
		}
		return false
	}
	return true
}

// matchClass matches r against the class at the start of p ("[...]") and
// returns the class length in bytes. It mirrors path.Match: the class is a
// sequence of runes or lo-hi ranges (each side may be backslash-escaped),
// '^' right after '[' negates, and ']' closes only after at least one range.
// A class checkGlob would reject (empty, unterminated, a stray '-' or ']')
// is treated as no match.
func matchClass(p string, r rune) (int, bool) {
	i := 1
	negate := false
	if i < len(p) && p[i] == '^' {
		negate = true
		i++
	}
	matched := false
	nrange := 0
	for {
		if i < len(p) && p[i] == ']' && nrange > 0 {
			i++
			break
		}
		lo, n, ok := classRune(p[i:])
		if !ok {
			return 0, false
		}
		i += n
		hi := lo
		if i < len(p) && p[i] == '-' {
			if hi, n, ok = classRune(p[i+1:]); !ok {
				return 0, false
			}
			i += 1 + n
		}
		if lo <= r && r <= hi {
			matched = true
		}
		nrange++
	}
	return i, matched != negate
}

// classRune decodes one class member at the start of s (an optional
// backslash, then one rune) and returns it with the bytes consumed; a
// leading '-' or ']', an empty s or an invalid encoding is not a member.
func classRune(s string) (r rune, n int, ok bool) {
	if s == "" || s[0] == '-' || s[0] == ']' {
		return 0, 0, false
	}
	esc := 0
	if s[0] == '\\' {
		s = s[1:]
		esc = 1
		if s == "" {
			return 0, 0, false
		}
	}
	r, n = utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && n == 1 {
		return 0, 0, false
	}
	return r, esc + n, true
}

func hasGlobMeta(g string) bool { return strings.ContainsAny(g, `*?[\`) }

// routeGlobsDisjoint reports whether two route globs provably never match
// the same route. Two patterns with metacharacters are assumed to overlap
// (the conservative answer for a load-time rejection).
func routeGlobsDisjoint(a, b string) bool {
	am, bm := hasGlobMeta(a), hasGlobMeta(b)
	switch {
	case !am && !bm:
		return a != b
	case !am:
		return !globMatch(b, a)
	case !bm:
		return !globMatch(a, b)
	}
	return false
}

// disjoint reports whether two matchers can never both match one request.
func disjoint(a, b *Match) bool {
	if len(a.Routes) > 0 && len(b.Routes) > 0 {
		all := true
		for _, ga := range a.Routes {
			for _, gb := range b.Routes {
				if !routeGlobsDisjoint(ga, gb) {
					all = false
				}
			}
		}
		if all {
			return true
		}
	}
	if len(a.Methods) > 0 && len(b.Methods) > 0 {
		common := false
		for _, ma := range a.Methods {
			for _, mb := range b.Methods {
				if ma == mb {
					common = true
				}
			}
		}
		if !common {
			return true
		}
	}
	for k, va := range a.Descriptors {
		if vb, ok := b.Descriptors[k]; ok && va != AnyValue && vb != AnyValue && va != vb {
			return true
		}
	}
	return false
}

// listRule is one compiled allow/deny entry.
type listRule struct {
	prefix netip.Prefix
	cidr   bool
	glob   string
}

// parseListRule accepts "10.0.0.0/8", "203.0.113.7" or a subject glob.
func parseListRule(s string) (listRule, error) {
	if s == "" {
		return listRule{}, errors.New("empty entry")
	}
	if p, err := netip.ParsePrefix(s); err == nil {
		return listRule{prefix: p.Masked(), cidr: true}, nil
	}
	if a, err := netip.ParseAddr(s); err == nil {
		return listRule{prefix: netip.PrefixFrom(a, a.BitLen()), cidr: true}, nil
	}
	if err := checkGlob("entry", s); err != nil {
		return listRule{}, err
	}
	return listRule{glob: s}, nil
}

// matches reports whether the rule hits subject or the ip descriptor.
func (r listRule) matches(subject string, ds []Descriptor) bool {
	if r.cidr {
		if a, err := netip.ParseAddr(subject); err == nil && r.prefix.Contains(a) {
			return true
		}
		for i := range ds {
			if ds[i].Key == "ip" {
				if a, err := netip.ParseAddr(ds[i].Value); err == nil && r.prefix.Contains(a) {
					return true
				}
			}
		}
		return false
	}
	return globMatch(r.glob, subject)
}

func anyRule(rules []listRule, subject string, ds []Descriptor) bool {
	for i := range rules {
		if rules[i].matches(subject, ds) {
			return true
		}
	}
	return false
}

// descriptorValue returns the value of key in ds, "" when absent.
func descriptorValue(ds []Descriptor, key string) string {
	for i := range ds {
		if ds[i].Key == key {
			return ds[i].Value
		}
	}
	return ""
}

// descriptorPresent reports whether key is in ds.
func descriptorPresent(ds []Descriptor, key string) bool {
	for i := range ds {
		if ds[i].Key == key {
			return true
		}
	}
	return false
}
