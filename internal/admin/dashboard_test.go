package admin

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestGrafanaDashboard keeps deploy/grafana/ratelimitd.json valid JSON, with
// the five panels docs/PLAN.md asks for, and every rl_* series it queries
// present in the /metrics exposition.
func TestGrafanaDashboard(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "deploy", "grafana", "ratelimitd.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(raw) {
		t.Fatalf("%s is not valid JSON", path)
	}
	var dash struct {
		UID    string `json:"uid"`
		Title  string `json:"title"`
		Panels []struct {
			Title   string `json:"title"`
			Type    string `json:"type"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatal(err)
	}
	if dash.UID != "ratelimitd" || dash.Title == "" {
		t.Errorf("uid/title = %q/%q", dash.UID, dash.Title)
	}
	wantTitles := []string{"Decisions/s by outcome", "Denial ratio by tenant", "p99 latency by backend", "Breaker state", "Top tenants by denials"}
	titles := make([]string, 0, len(dash.Panels))
	for _, p := range dash.Panels {
		titles = append(titles, p.Title)
	}
	for _, w := range wantTitles {
		found := false
		for _, tt := range titles {
			found = found || strings.HasPrefix(tt, w)
		}
		if !found {
			t.Errorf("panel %q missing; have %v", w, titles)
		}
	}

	var b bytes.Buffer
	mh, _ := Metrics(fixture(t), nil).(*metricsHandler)
	mh.render(&b)
	exposed := b.String()
	metricRE := regexp.MustCompile(`\brl_[a-z_]+`)
	for _, p := range dash.Panels {
		if len(p.Targets) == 0 {
			t.Errorf("panel %q has no targets", p.Title)
		}
		for _, tg := range p.Targets {
			for _, m := range metricRE.FindAllString(tg.Expr, -1) {
				if !strings.Contains(exposed, "\n"+m+"{") && !strings.Contains(exposed, "\n"+m+" ") {
					t.Errorf("panel %q queries %s, which /metrics does not expose", p.Title, m)
				}
			}
		}
	}
}
