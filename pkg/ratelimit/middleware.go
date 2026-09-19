package ratelimit

import (
	"net/http"
)

// KeyFunc derives the Request from an incoming HTTP request (tenant, subject
// such as an API key or client IP, route, method, descriptors).
type KeyFunc func(*http.Request) Request

// Middleware checks every request through c. On every response it sets
// RateLimit-Limit, RateLimit-Remaining and RateLimit-Reset (seconds, rounded
// up); on a denial it sets Retry-After (seconds, rounded up, min 1) and
// answers 429 (or the WithDenyHandler). A client error applies WithOnError
// (default FailOpen, logged). STUB: passes every request through untouched.
func Middleware(_ Client, _ KeyFunc, _ ...MWOption) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler { return next }
}
