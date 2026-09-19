---
name: block-non-conventional-commit
enabled: true
event: bash
action: block
conditions:
  - field: command
    operator: regex_match
    pattern: \bgit\s+commit\b
  - field: command
    operator: regex_match
    pattern: git\s+commit\b(?:(?!\s(?:-[a-zA-Z]*m|--message)[\s=])[\s\S])*\s(?:-[a-zA-Z]*m|--message)[\s=]+(?!(?:@?["'])?\s*(?:\$\(cat\s*<<-?\s*["']?\w+["']?\s*)?(?:feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(?:\([^)]+\))?!?:\s\S)
---

**Commit message is not Conventional-Commit shaped. Blocked.**

Required form: `<type>(<optional scope>)!?: <subject>` where type is one of
`feat fix docs style refactor perf test build ci chore revert`.

Examples: `feat(auth): add refresh tokens`, `fix(db): close rows on error`,
`chore: bump golangci-lint`, `refactor!: drop legacy config loader`.

Re-run the commit with a conforming `-m` message (or use `/commit`, which writes one from the diff).
Only the first `-m` is checked, so extra `-m "body"` paragraphs are fine.
