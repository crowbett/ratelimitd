package limit

import "testing"

func TestGrant(t *testing.T) {
	t.Parallel()
	// A counter with avail units: cost fits iff cost <= avail; a denial
	// reports Remaining = avail (the largest cost that fits), like every
	// algorithm here.
	step := func(avail int) func(int) (int, Decision) {
		return func(cost int) (int, Decision) {
			if cost <= avail {
				return avail - cost, Decision{Allowed: true, Remaining: avail - cost}
			}
			return avail, Decision{Remaining: avail, RetryAfter: 7}
		}
	}
	tests := []struct {
		name               string
		avail, lo, want    int
		wantGot, wantState int
		wantAllowed        bool
	}{
		{"want fits", 100, 1, 10, 10, 90, true},
		{"partial grant", 6, 1, 10, 6, 0, true},
		{"partial grant at exactly lo", 3, 3, 10, 3, 0, true},
		{"below lo denies", 2, 3, 10, 0, 2, false},
		{"empty denies", 0, 1, 10, 0, 0, false},
		{"want below lo is raised to lo", 5, 4, 1, 4, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			next, d, got := Grant(step(tt.avail), tt.lo, tt.want)
			if got != tt.wantGot || d.Allowed != tt.wantAllowed || (got > 0 && next != tt.wantState) {
				t.Fatalf("Grant = next %d, %+v, got %d; want got %d state %d allowed %v", next, d, got, tt.wantGot, tt.wantState, tt.wantAllowed)
			}
			if !d.Allowed && d.RetryAfter != 7 {
				t.Fatalf("denial lost the retry of the lo evaluation: %+v", d)
			}
		})
	}
}
