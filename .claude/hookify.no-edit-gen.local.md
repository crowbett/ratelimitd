---
name: block-edit-generated-pb
enabled: true
event: file
action: block
conditions:
  - field: file_path
    operator: regex_match
    pattern: [\\/]gen[\\/].*\.pb\.go$
---

**gen/ is generated - edit proto/ and run `task gen`.**

`gen/**/*.pb.go` is protoc-gen-go / protoc-gen-go-grpc output from `proto/**/*.proto`
(`buf generate`). It is committed so `go build` works without buf, and CI regenerates it
and fails on any diff, so a hand edit is overwritten or breaks the build.

Change the `.proto` (messages, fields, rpcs, `go_package`), then `task gen`, then commit
the proto and the regenerated `gen/` together.
