package limit

import "testing"

func TestIsVariant(t *testing.T) {
	t.Parallel()
	key := Key("t", "s", "api")
	for _, tc := range []struct {
		k    string
		want bool
	}{
		{key + ":r=/a", true},
		{key + ":", true},
		{key, false},
		{Key("t", "s", "api2"), false},
		{Key("t", "s", "api2") + ":r=/a", false},
		{"rl:{t=t|s=s}:p=ap", false},
	} {
		if got := IsVariant(tc.k, key); got != tc.want {
			t.Errorf("IsVariant(%q) = %v, want %v", tc.k, got, tc.want)
		}
	}
}
