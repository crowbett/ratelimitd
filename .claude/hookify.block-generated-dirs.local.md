---
name: block-generated-dirs
enabled: true
event: file
action: block
conditions:
  - field: file_path
    operator: regex_match
    pattern: [\\/](vendor|node_modules|target|build)[\\/]
---

**Edit to a generated / dependency directory blocked.**

`vendor/`, `node_modules/`, `target/` and `build/` are build output or vendored
dependencies. Changes there are overwritten by the next `go mod vendor`,
`npm install`, `cargo build` or `cmake --build`.

Fix the source instead: `go.mod` / `package.json` / `Cargo.toml` / `CMakeLists.txt`,
or the upstream file the artefact is generated from.
