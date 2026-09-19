package admin

import (
	"bytes"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Metrics serves the Snapshot in Prometheus text format 0.0.4, written by
// hand on a pooled buffer (no client library). build becomes the labels of
// rl_build_info; "go_version" defaults to runtime.Version() and "version" to
// "dev". A nil Snapshot renders only rl_build_info.
//
// Families (docs/PLAN.md, "Observability"): rl_decisions_total,
// rl_decision_latency_seconds, rl_backend_errors_total, rl_fallback_active,
// rl_fallback_seconds_total, rl_redis_script_loads_total,
// rl_policy_version_info, rl_policy_reload_errors_total, rl_clock_skew_seconds,
// rl_local_keys, rl_local_evicted_total, rl_build_info.
func Metrics(s *Snapshot, build map[string]string) http.Handler {
	return &metricsHandler{snap: s, buildInfo: renderBuildInfo(build)}
}

type metricsHandler struct {
	snap      *Snapshot
	buildInfo []byte // pre-rendered "rl_build_info{...} 1\n" block
}

var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func (h *metricsHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	b, _ := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufPool.Put(b)

	h.render(b)

	w.Header().Set("Content-Type", ContentType)
	w.Header().Set("Content-Length", strconv.Itoa(b.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}

// bucketLabels are the pre-rendered `le` values of LatencyBucketsUS in seconds.
var bucketLabels = func() [numLatencyBuckets]string {
	var out [numLatencyBuckets]string
	for i, us := range latencyBucketsUS {
		out[i] = strconv.FormatFloat(float64(us)/1e6, 'g', -1, 64)
	}
	return out
}()

// render writes the whole exposition into b.
func (h *metricsHandler) render(b *bytes.Buffer) {
	e := exposer{b: b}
	if h.snap != nil {
		h.renderSnapshot(&e)
	}
	b.Write(h.buildInfo)
}

func (h *metricsHandler) renderSnapshot(e *exposer) {
	s := h.snap
	c := s.Counters()

	e.header("rl_decisions_total", "Decisions by tenant, policy, outcome and backend.", "counter")
	for _, d := range s.Decisions() {
		e.open("rl_decisions_total", "tenant", d.Key.Tenant, "policy", d.Key.Policy, "allowed", strconv.FormatBool(d.Key.Allowed), "backend", d.Key.Backend)
		e.uint(d.N)
	}

	e.header("rl_decision_latency_seconds", "Decision latency by backend (validate + resolve + store).", "histogram")
	for _, l := range s.Latencies() {
		for i, le := range bucketLabels {
			e.open("rl_decision_latency_seconds_bucket", "backend", l.Backend, "le", le)
			e.uint(l.Cumulative[i])
		}
		e.open("rl_decision_latency_seconds_bucket", "backend", l.Backend, "le", "+Inf")
		e.uint(l.Count)
		e.open("rl_decision_latency_seconds_sum", "backend", l.Backend)
		e.float(float64(l.SumUS) / 1e6)
		e.open("rl_decision_latency_seconds_count", "backend", l.Backend)
		e.uint(l.Count)
	}

	e.header("rl_backend_errors_total", "Store errors by backend and kind.", "counter")
	for _, be := range s.BackendErrors() {
		e.open("rl_backend_errors_total", "backend", be.Key.Backend, "kind", be.Key.Kind)
		e.uint(be.N)
	}

	fbs := s.Fallbacks()
	e.header("rl_fallback_active", "1 while the breaker is open and the degraded path answers.", "gauge")
	for _, f := range fbs {
		e.open("rl_fallback_active", "backend", f.Backend)
		e.uint(boolU(f.Active))
	}
	e.header("rl_fallback_seconds_total", "Seconds spent with the breaker open.", "counter")
	for _, f := range fbs {
		e.open("rl_fallback_seconds_total", "backend", f.Backend)
		e.float(f.Total.Seconds())
	}

	e.counter("rl_redis_script_loads_total", "Lua script (re)loads after NOSCRIPT.", c.ScriptLoads)
	e.header("rl_policy_version_info", "Content hash of the active policy set; always 1.", "gauge")
	e.open("rl_policy_version_info", "version", c.PolicyVersion)
	e.uint(1)
	e.counter("rl_policy_reload_errors_total", "Policy file reloads rejected by parse or validate.", c.ReloadErrors)
	e.gauge("rl_clock_skew_seconds", "Local clock minus Redis TIME, last sample.", c.ClockSkew.Seconds())
	e.header("rl_local_keys", "Keys held by the local backend.", "gauge")
	e.open("rl_local_keys")
	e.int(c.LocalKeys)
	e.counter("rl_local_evicted_total", "Keys evicted by the local backend's sweeper.", c.LocalEvicted)
}

func boolU(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// exposer is a tiny append-only writer for the text format.
type exposer struct {
	b   *bytes.Buffer
	tmp [32]byte
}

func (e *exposer) header(name, help, kind string) {
	e.b.WriteString("# HELP ")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.b.WriteString(help)
	e.b.WriteString("\n# TYPE ")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.b.WriteString(kind)
	e.b.WriteByte('\n')
}

func (e *exposer) float(v float64) {
	e.b.Write(strconv.AppendFloat(e.tmp[:0], v, 'g', -1, 64))
	e.b.WriteByte('\n')
}

func (e *exposer) uint(v uint64) {
	e.b.Write(strconv.AppendUint(e.tmp[:0], v, 10))
	e.b.WriteByte('\n')
}

func (e *exposer) gauge(name, help string, v float64) {
	e.header(name, help, "gauge")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.float(v)
}

func (e *exposer) counter(name, help string, v uint64) {
	e.header(name, help, "counter")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.uint(v)
}

func (e *exposer) int(v int64) {
	e.b.Write(strconv.AppendInt(e.tmp[:0], v, 10))
	e.b.WriteByte('\n')
}

// open writes `name{k1="v1",k2="v2"} ` (or `name ` without labels) straight
// into the buffer from alternating key/value pairs, values escaped per the
// text format (backslash, double quote and newline). The value follows.
func (e *exposer) open(name string, kv ...string) {
	e.b.WriteString(name)
	if len(kv) == 0 {
		e.b.WriteByte(' ')
		return
	}
	e.b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			e.b.WriteByte(',')
		}
		e.b.WriteString(kv[i])
		e.b.WriteString(`="`)
		writeEscaped(e.b, kv[i+1])
		e.b.WriteByte('"')
	}
	e.b.WriteString(`} `)
}

// byteWriter is what writeEscaped needs from bytes.Buffer and strings.Builder.
type byteWriter interface {
	WriteByte(c byte) error
	WriteString(s string) (int, error)
}

func writeEscaped(w byteWriter, v string) {
	for i := 0; i < len(v); i++ {
		switch c := v[i]; c {
		case '\\':
			_, _ = w.WriteString(`\\`)
		case '"':
			_, _ = w.WriteString(`\"`)
		case '\n':
			_, _ = w.WriteString(`\n`)
		default:
			_ = w.WriteByte(c)
		}
	}
}

// renderBuildInfo pre-renders the constant rl_build_info block with labels in
// sorted key order.
func renderBuildInfo(info map[string]string) []byte {
	labels := make(map[string]string, len(info)+2)
	for k, v := range info {
		labels[k] = v
	}
	if _, ok := labels["go_version"]; !ok {
		labels["go_version"] = runtime.Version()
	}
	if _, ok := labels["version"]; !ok {
		labels["version"] = "dev"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString("# HELP rl_build_info Build metadata; always 1.\n# TYPE rl_build_info gauge\nrl_build_info{")
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteString(`="`)
		writeEscaped(&sb, labels[k])
		sb.WriteByte('"')
	}
	sb.WriteString("} 1\n")
	return []byte(sb.String())
}
