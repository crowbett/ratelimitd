package limit

// KeyPrefix is the namespace every limiter key starts with.
const KeyPrefix = "rl:"

// HashTag returns the Redis Cluster hash tag shared by every key of one
// subject, "{t=<tenant>|s=<subject>}", so a multi-policy Apply stays in one
// slot (docs/PLAN.md, Decision 4 and the "Cluster hash tags" risk).
func HashTag(tenant, subject string) string {
	return "{t=" + tenant + "|s=" + subject + "}"
}

// Key builds the state key for one (tenant, subject, policy) triple:
// "rl:{t=<tenant>|s=<subject>}:p=<policy>". Store implementations may add
// their own outer prefix (redisx.Config.KeyPrefix) in front of it.
func Key(tenant, subject, policy string) string {
	return KeyPrefix + HashTag(tenant, subject) + ":p=" + policy
}
