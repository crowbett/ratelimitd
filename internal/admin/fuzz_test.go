package admin

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// FuzzMetricsLabels feeds arbitrary strings into every labelled family
// (tenant, policy, backend, error kind, policy version, build labels) and
// asserts the rendered exposition stays parseable text format 0.0.4: every
// line is a comment or `name{k="v",...} value`, every label value decodes
// back to the recorded string through the three documented escapes, no
// value carries a raw quote or newline, and the series cap holds.
func FuzzMetricsLabels(f *testing.F) {
	f.Add("acme", "api", "redis", "timeout", "6f1d2c3b", "v1.2.3")
	f.Add("", "", "", "", "", "")
	f.Add(`a"b`, `c\d`, "e\nf", "g}h", `"`, `\`)
	f.Add("tenant=\"x\"} 1\nrl_build_info", "p", "b", "k", "v", "ver")
	f.Add("ü", "日本", "\x00", "\xff", "\t", "\r")
	f.Add(strings.Repeat("x", 4096), "p", "b", "k", "v", "ver")
	f.Fuzz(func(t *testing.T, tenant, policy, backend, kind, version, build string) {
		if len(tenant)+len(policy)+len(backend)+len(kind)+len(version)+len(build) > 16<<10 {
			t.Skip()
		}
		s := NewSnapshot()
		s.now = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }
		s.RecordDecision(tenant, policy, true, backend, 50*time.Microsecond)
		s.RecordDecision(tenant, policy, false, backend, 5*time.Millisecond)
		s.RecordBackendError(backend, kind)
		s.SetFallbackActive(backend, true)
		s.SetPolicyVersion(version)
		h := Metrics(s, map[string]string{"version": build, "go_version": "go1.27.0"})
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
		if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != ContentType {
			t.Fatalf("status %d content-type %q", rec.Code, rec.Header().Get("Content-Type"))
		}
		if cl, _ := strconv.Atoi(rec.Header().Get("Content-Length")); cl != rec.Body.Len() {
			t.Fatalf("Content-Length %d, body %d", cl, rec.Body.Len())
		}
		wantBackend := backend
		if wantBackend == "" {
			wantBackend = "none"
		}
		seen := map[string]bool{}
		for _, line := range bytes.Split(bytes.TrimSuffix(rec.Body.Bytes(), []byte("\n")), []byte("\n")) {
			if len(line) == 0 {
				t.Fatalf("empty line in exposition:\n%s", rec.Body.Bytes())
			}
			if line[0] == '#' {
				continue
			}
			name, labels, ok := parseSample(t, string(line))
			if !ok {
				t.Fatalf("unparseable sample %q", line)
			}
			for k, v := range labels {
				seen[name+"/"+k+"="+v] = true
			}
		}
		for _, want := range []string{
			"rl_decisions_total/tenant=" + tenant, "rl_decisions_total/policy=" + policy, "rl_decisions_total/backend=" + wantBackend,
			"rl_backend_errors_total/kind=" + kind, "rl_backend_errors_total/backend=" + backend,
			"rl_policy_version_info/version=" + version, "rl_build_info/version=" + build,
			"rl_decision_latency_seconds_bucket/backend=" + wantBackend, "rl_fallback_active/backend=" + backend,
		} {
			if !seen[want] {
				t.Fatalf("label %q not recovered from the exposition:\n%s", want, rec.Body.Bytes())
			}
		}
	})
}

// parseSample splits `name{k="v",...} value` (or `name value`) and
// unescapes every label value with the text-format rules; it fails the test
// on a raw quote or newline inside a value or a value that is not a number.
func parseSample(t *testing.T, line string) (name string, labels map[string]string, ok bool) {
	t.Helper()
	labels = map[string]string{}
	rest := line
	if i := strings.IndexByte(rest, '{'); i >= 0 {
		name = rest[:i]
		rest = rest[i+1:]
		for {
			eq := strings.IndexByte(rest, '=')
			if eq < 0 || len(rest) < eq+2 || rest[eq+1] != '"' {
				return "", nil, false
			}
			key := rest[:eq]
			rest = rest[eq+2:]
			var val strings.Builder
			closed := false
			for j := 0; j < len(rest); j++ {
				c := rest[j]
				switch c {
				case '\\':
					if j+1 >= len(rest) {
						return "", nil, false
					}
					j++
					switch rest[j] {
					case '\\':
						val.WriteByte('\\')
					case '"':
						val.WriteByte('"')
					case 'n':
						val.WriteByte('\n')
					default:
						t.Fatalf("unknown escape \\%c in %q", rest[j], line)
					}
				case '"':
					closed = true
					rest = rest[j+1:]
				case '\n':
					t.Fatalf("raw newline inside a label value: %q", line)
				default:
					val.WriteByte(c)
				}
				if closed {
					break
				}
			}
			if !closed {
				return "", nil, false
			}
			labels[key] = val.String()
			if strings.HasPrefix(rest, ",") {
				rest = rest[1:]
				continue
			}
			if !strings.HasPrefix(rest, "} ") {
				return "", nil, false
			}
			rest = rest[2:]
			break
		}
	} else {
		sp := strings.IndexByte(rest, ' ')
		if sp < 0 {
			return "", nil, false
		}
		name, rest = rest[:sp], rest[sp+1:]
	}
	if _, err := strconv.ParseFloat(rest, 64); err != nil {
		t.Fatalf("sample value %q is not a number in %q", rest, line)
	}
	return name, labels, true
}

// FuzzWriteEscaped: the label escaper is total and invertible for the three
// escapes it emits, and never lets a raw quote or newline through.
func FuzzWriteEscaped(f *testing.F) {
	f.Add("")
	f.Add(`a"b\c` + "\n")
	f.Add("\x00\xff日本")
	f.Fuzz(func(t *testing.T, v string) {
		var b bytes.Buffer
		writeEscaped(&b, v)
		out := b.String()
		for i := 0; i < len(out); i++ {
			switch out[i] {
			case '\n':
				t.Fatalf("raw newline in %q", out)
			case '"':
				if i == 0 || out[i-1] != '\\' {
					t.Fatalf("raw quote in %q", out)
				}
			}
		}
		var got strings.Builder
		for i := 0; i < len(out); i++ {
			if out[i] != '\\' {
				got.WriteByte(out[i])
				continue
			}
			i++
			switch out[i] {
			case '\\':
				got.WriteByte('\\')
			case '"':
				got.WriteByte('"')
			case 'n':
				got.WriteByte('\n')
			default:
				t.Fatalf("unknown escape \\%c in %q", out[i], out)
			}
		}
		if got.String() != v {
			t.Fatalf("unescape(escape(%q)) = %q", v, got.String())
		}
	})
}
