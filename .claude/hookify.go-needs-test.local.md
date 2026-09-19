---
name: warn-go-file-without-test
enabled: true
event: file
action: warn
tool_matcher: Write
conditions:
  - field: file_path
    operator: regex_match
    pattern: (?<!_test)\.go$
  - field: file_path
    operator: regex_match
    pattern: [\\/]internal[\\/]
---

**New `.go` file under `internal/` - does its package have a `_test.go`?**

Convention here: every non-trivial package ships table-driven tests
(`t.Run` subtests, `t.Parallel()`, `t.Helper()` in helpers).

If the package has no `_test.go` yet, add one alongside this file before calling
the change done (`/go-test` runs the suite with `-race`). Pure wiring files
(`main.go`, DI glue) are the accepted exception - say so in the report.
