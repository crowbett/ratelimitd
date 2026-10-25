//go:build !(js && wasm)

package main

import (
	"fmt"
	"os"
)

// main exists so the package builds natively for vet, lint and tests; the
// demo itself runs only as js/wasm.
func main() {
	fmt.Fprintln(os.Stderr, "webdemo runs only as js/wasm: GOOS=js GOARCH=wasm go build ./cmd/webdemo")
	os.Exit(2)
}
