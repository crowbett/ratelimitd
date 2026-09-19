package redisx

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Error kinds reported to admin.Snapshot.RecordBackendError.
const (
	KindTimeout  = "timeout"
	KindConn     = "conn"
	KindPool     = "pool_timeout"
	KindClosed   = "closed"
	KindCanceled = "canceled"
	KindReply    = "reply"       // an error reply from a healthy server (script bug, CROSSSLOT, WRONGTYPE)
	KindDown     = "unavailable" // LOADING / CLUSTERDOWN / MASTERDOWN / READONLY / NOSCRIPT after reload
)

// unavailablePrefixes are server error replies that mean "not usable now"
// rather than "your call was wrong".
var unavailablePrefixes = []string{"LOADING", "CLUSTERDOWN", "MASTERDOWN", "READONLY", "TRYAGAIN", "NOSCRIPT"}

// classify buckets err into a metric kind and whether it should surface as
// limit.ErrBackendDown (true for anything the fallback breaker should count
// as the backend being unreachable).
func classify(err error) (kind string, down bool) {
	var (
		netErr   net.Error
		redisErr redis.Error
	)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout, true
	case errors.Is(err, context.Canceled):
		return KindCanceled, false
	case errors.Is(err, redis.ErrPoolTimeout):
		return KindPool, true
	case errors.Is(err, redis.ErrClosed):
		return KindClosed, true
	case errors.As(err, &netErr) && netErr.Timeout():
		return KindTimeout, true
	case errors.As(err, &redisErr):
		msg := redisErr.Error()
		for _, p := range unavailablePrefixes {
			if strings.HasPrefix(msg, p) {
				return KindDown, true
			}
		}
		return KindReply, false
	default: // io.EOF, reset/refused connections, anything else from the transport
		return KindConn, true
	}
}
