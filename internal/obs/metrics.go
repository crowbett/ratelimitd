package obs

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/crowbett/ratelimitd/internal/admin"
)

// RegisterSnapshot exposes s as observable instruments sampled at every
// export, reading the same counters the /metrics exposer renders:
// rl_decisions_total{tenant,policy,allowed,backend},
// rl_backend_errors_total{backend,kind}, rl_fallback_active{backend},
// rl_fallback_seconds_total{backend}, rl_redis_script_loads_total,
// rl_policy_reload_errors_total, rl_clock_skew_seconds, rl_local_keys,
// rl_local_evicted_total. With OTel disabled the global no-op meter makes
// this free. A nil Snapshot registers nothing.
func RegisterSnapshot(s *admin.Snapshot) error {
	if s == nil {
		return nil
	}
	m := otel.Meter(MeterName)
	var errs []error
	mk := func(name, desc, unit string) metric.Int64ObservableCounter {
		c, err := m.Int64ObservableCounter(name, metric.WithDescription(desc), metric.WithUnit(unit))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
		return c
	}
	mkGauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := m.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
		return g
	}
	decisions := mk("rl_decisions_total", "Decisions by tenant, policy, outcome and backend", "{decision}")
	backendErrors := mk("rl_backend_errors_total", "Store errors by backend and kind", "{error}")
	fallbackActive := mkGauge("rl_fallback_active", "1 while the breaker is open", "1")
	fallbackSeconds, err := m.Float64ObservableCounter("rl_fallback_seconds_total",
		metric.WithDescription("Seconds spent with the breaker open"), metric.WithUnit("s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("rl_fallback_seconds_total: %w", err))
	}
	scriptLoads := mk("rl_redis_script_loads_total", "Lua script (re)loads after NOSCRIPT", "{load}")
	reloadErrors := mk("rl_policy_reload_errors_total", "Policy file reloads rejected by parse or validate", "{error}")
	clockSkew, err := m.Float64ObservableGauge("rl_clock_skew_seconds",
		metric.WithDescription("Local clock minus Redis TIME, last sample"), metric.WithUnit("s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("rl_clock_skew_seconds: %w", err))
	}
	localKeys := mkGauge("rl_local_keys", "Keys held by the local backend", "{key}")
	localEvicted := mk("rl_local_evicted_total", "Keys evicted by the local backend's sweeper", "{key}")
	if len(errs) > 0 {
		return errs[0]
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, d := range s.Decisions() {
			o.ObserveInt64(decisions, clamp(d.N), metric.WithAttributes(
				attribute.String("tenant", d.Key.Tenant),
				attribute.String("policy", d.Key.Policy),
				attribute.Bool("allowed", d.Key.Allowed),
				attribute.String("backend", d.Key.Backend),
			))
		}
		for _, e := range s.BackendErrors() {
			o.ObserveInt64(backendErrors, clamp(e.N), metric.WithAttributes(
				attribute.String("backend", e.Key.Backend),
				attribute.String("kind", e.Key.Kind),
			))
		}
		for _, f := range s.Fallbacks() {
			attrs := metric.WithAttributes(attribute.String("backend", f.Backend))
			var active int64
			if f.Active {
				active = 1
			}
			o.ObserveInt64(fallbackActive, active, attrs)
			o.ObserveFloat64(fallbackSeconds, f.Total.Seconds(), attrs)
		}
		c := s.Counters()
		o.ObserveInt64(scriptLoads, clamp(c.ScriptLoads))
		o.ObserveInt64(reloadErrors, clamp(c.ReloadErrors))
		o.ObserveFloat64(clockSkew, c.ClockSkew.Seconds())
		o.ObserveInt64(localKeys, c.LocalKeys)
		o.ObserveInt64(localEvicted, clamp(c.LocalEvicted))
		return nil
	}, decisions, backendErrors, fallbackActive, fallbackSeconds, scriptLoads, reloadErrors, clockSkew, localKeys, localEvicted)
	if err != nil {
		return fmt.Errorf("register snapshot callback: %w", err)
	}
	return nil
}

const maxInt64 = uint64(1<<63 - 1)

func clamp(v uint64) int64 { return int64(min(v, maxInt64)) }
