package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.GRPCAddr != ":50053" || cfg.HTTPAddr != ":8093" || cfg.AdminAddr != "127.0.0.1:9130" {
		t.Errorf("listener defaults: %+v", cfg)
	}
	if cfg.AdminGRPCAddr != "127.0.0.1:50054" {
		t.Errorf("ADMIN_GRPC_ADDR default = %q, want loopback 127.0.0.1:50054", cfg.AdminGRPCAddr)
	}
	if cfg.Redis.Addr != "127.0.0.1:6380" || cfg.Redis.PoolSize != 64 || cfg.Redis.Cluster || cfg.Redis.Timeout != 50*time.Millisecond {
		t.Errorf("redis defaults: %+v", cfg.Redis)
	}
	if cfg.Policy.File != "policies.yaml" || cfg.Policy.Poll != 2*time.Second {
		t.Errorf("policy defaults: %+v", cfg.Policy)
	}
	if cfg.Fallback.Mode != "local" || cfg.Fallback.Divisor != 1 {
		t.Errorf("fallback defaults: %+v", cfg.Fallback)
	}
	if cfg.Breaker.Ratio != 0.5 || cfg.Breaker.Min != 20 || cfg.Breaker.OpenFor != 5*time.Second || cfg.Breaker.Probe != 500*time.Millisecond {
		t.Errorf("breaker defaults: %+v", cfg.Breaker)
	}
	if cfg.Local.Shards != 256 || cfg.Local.IdleTTL != 10*time.Minute || cfg.Local.MaxKeys != 1_000_000 {
		t.Errorf("local defaults: %+v", cfg.Local)
	}
	if cfg.MaxClockSkew != 2*time.Second || cfg.TrustClientClock || cfg.StopTimeout != 10*time.Second || cfg.OTel.Enabled {
		t.Errorf("lifecycle defaults: skew=%s trust=%v stop=%s otel=%+v", cfg.MaxClockSkew, cfg.TrustClientClock, cfg.StopTimeout, cfg.OTel)
	}
}

func TestLoadTable(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string // substring of the error; empty means success
		check   func(t *testing.T, c Config)
	}{
		{
			name: "overrides",
			env: map[string]string{
				"REDIS_ADDR": "redis:6379", "REDIS_POOL_SIZE": "128", "REDIS_CLUSTER": "true", "REDIS_TIMEOUT": "20ms",
				"FALLBACK_MODE": "FAIL_OPEN", "FALLBACK_DIVISOR": "3", "BREAKER_RATIO": "0.25", "LOCAL_SHARDS": "64",
				"TRUST_CLIENT_CLOCK": "1", "LOG_LEVEL": "debug",
			},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if c.Redis.Addr != "redis:6379" || c.Redis.PoolSize != 128 || !c.Redis.Cluster || c.Redis.Timeout != 20*time.Millisecond {
					t.Errorf("redis overrides not applied: %+v", c.Redis)
				}
				if c.Fallback.Mode != "fail_open" || c.Fallback.Divisor != 3 || c.Breaker.Ratio != 0.25 || c.Local.Shards != 64 {
					t.Errorf("overrides not applied: %+v %+v %+v", c.Fallback, c.Breaker, c.Local)
				}
				if !c.TrustClientClock || c.LogLevel.String() != "DEBUG" {
					t.Errorf("trust=%v level=%s", c.TrustClientClock, c.LogLevel)
				}
			},
		},
		{name: "bad fallback mode", env: map[string]string{"FALLBACK_MODE": "panic"}, wantErr: "FALLBACK_MODE"},
		{name: "divisor zero", env: map[string]string{"FALLBACK_DIVISOR": "0"}, wantErr: "FALLBACK_DIVISOR"},
		{name: "lease span too small", env: map[string]string{"LEASE_SPAN": "10us"}, wantErr: "LEASE_SPAN"},
		{name: "lease ttl too long", env: map[string]string{"LEASE_TTL": "1m"}, wantErr: "LEASE_TTL"},
		{name: "lease span not a duration", env: map[string]string{"LEASE_SPAN": "soon"}, wantErr: "LEASE_SPAN"},
		{
			name: "lease overrides and defaults", env: map[string]string{"LEASE_SPAN": "5ms"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if c.Lease.Span != 5*time.Millisecond || c.Lease.TTL != 100*time.Millisecond {
					t.Errorf("lease = %+v", c.Lease)
				}
			},
		},
		{name: "ratio out of range", env: map[string]string{"BREAKER_RATIO": "1.5"}, wantErr: "BREAKER_RATIO"},
		{name: "probe slower than open", env: map[string]string{"BREAKER_OPEN_FOR": "1s", "BREAKER_PROBE": "2s"}, wantErr: "BREAKER_PROBE"},
		{name: "shards not power of two", env: map[string]string{"LOCAL_SHARDS": "100"}, wantErr: "LOCAL_SHARDS"},
		{name: "bad duration", env: map[string]string{"POLICY_POLL": "soon"}, wantErr: "POLICY_POLL"},
		{name: "bad int", env: map[string]string{"REDIS_POOL_SIZE": "many"}, wantErr: "REDIS_POOL_SIZE"},
		{name: "bad bool", env: map[string]string{"REDIS_CLUSTER": "maybe"}, wantErr: "REDIS_CLUSTER"},
		{name: "bad float", env: map[string]string{"BREAKER_RATIO": "half"}, wantErr: "BREAKER_RATIO"},
		{
			name: "admin grpc addr override", env: map[string]string{"ADMIN_GRPC_ADDR": "127.0.0.1:6000"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if c.AdminGRPCAddr != "127.0.0.1:6000" {
					t.Errorf("ADMIN_GRPC_ADDR = %q", c.AdminGRPCAddr)
				}
			},
		},
		{name: "admin grpc shares the data plane", env: map[string]string{"GRPC_ADDR": "127.0.0.1:7000", "ADMIN_GRPC_ADDR": "127.0.0.1:7000"}, wantErr: "ADMIN_GRPC_ADDR must differ"},
		{name: "admin token too short", env: map[string]string{"ADMIN_TOKEN": "hunter2"}, wantErr: "ADMIN_TOKEN"},
		{
			name: "admin token accepted verbatim", env: map[string]string{"ADMIN_TOKEN": " 0123456789abcdef "},
			check: func(t *testing.T, c Config) {
				t.Helper()
				if c.AdminToken != " 0123456789abcdef " {
					t.Errorf("ADMIN_TOKEN = %q, want it untrimmed and unmodified", c.AdminToken)
				}
			},
		},
		{name: "several at once", env: map[string]string{"LOG_FORMAT": "yaml", "REDIS_TIMEOUT": "1us", "LOCAL_MAX_KEYS": "0"}, wantErr: "LOCAL_MAX_KEYS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				tt.check(t, cfg)
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateJoinsEveryError(t *testing.T) {
	t.Parallel()
	var c Config // every zero value is invalid; OTel enabled without an endpoint is only reachable here (Load defaults it)
	c.OTel.Enabled = true
	err := c.Validate()
	if err == nil {
		t.Fatal("zero Config validated")
	}
	for _, want := range []string{"GRPC_ADDR", "HTTP_ADDR", "ADMIN_ADDR", "ADMIN_GRPC_ADDR", "LOG_FORMAT", "SHUTDOWN_TIMEOUT", "REDIS_ADDR", "POLICY_FILE", "FALLBACK_MODE", "BREAKER_RATIO", "LOCAL_SHARDS", "OTEL_EXPORTER_OTLP_ENDPOINT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error lacks %s: %v", want, err)
		}
	}
}
