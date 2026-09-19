package policy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Parse decodes one policies.yaml strictly (unknown fields are an error),
// fills Tenant.ID from the map key, stamps Version with the content hash and
// LoadedAt with now. It does not call Validate; Load does.
func Parse(r io.Reader) (Set, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return Set{}, fmt.Errorf("read policies: %w", err)
	}
	var s Set
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil && !errors.Is(err, io.EOF) {
		return Set{}, fmt.Errorf("parse policies: %w", err)
	}
	for id, t := range s.Tenants {
		t.ID = id
		s.Tenants[id] = t
	}
	s.Version = contentHash(raw)
	s.LoadedAt = time.Now()
	return s, nil
}

// contentHash is Set.Version for the bytes a Set was parsed from (or will
// be written as): the first 8 bytes of SHA-256, hex.
func contentHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

// Load reads, parses and validates the policy file at path.
func Load(path string) (Set, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied path from POLICY_FILE
	if err != nil {
		return Set{}, fmt.Errorf("open policies: %w", err)
	}
	defer f.Close()
	s, err := Parse(f)
	if err != nil {
		return Set{}, err
	}
	if err := s.Validate(); err != nil {
		return Set{}, fmt.Errorf("validate %s: %w", path, err)
	}
	return s, nil
}

// Write renders s back to YAML (the admin Upsert path rewrites the file with
// this: temp file + rename). Output is deterministic: map keys are sorted,
// durations use their Go spelling ("1s"), Version and LoadedAt are not
// written. Two Sets with equal content render to equal bytes, which is what
// lets the watcher recognise the registry's own write by hash.
func Write(w io.Writer, s Set) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	if err := enc.Encode(s); err != nil {
		return fmt.Errorf("render policies: %w", err)
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("render policies: %w", err)
	}
	return nil
}
