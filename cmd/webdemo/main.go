//go:build js && wasm

package main

import (
	"encoding/json"
	"syscall/js"
)

func main() {
	js.Global().Set("__demo", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 2 {
			return encode(map[string]string{"error": "want (method, paramsJSON)"})
		}
		return encode(Handle(args[0].String(), []byte(args[1].String())))
	}))
	js.Global().Call("__demoReady")
	select {}
}

func encode(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"error":"encode failed"}`
	}
	return string(b)
}
