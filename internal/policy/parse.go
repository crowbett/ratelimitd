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
	sum := sha256.Sum256(raw)
	s.Version = hex.EncodeToString(sum[:8])
	s.LoadedAt = time.Now()
	return s, nil
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
// this: temp file + rename). STUB.
func Write(_ io.Writer, _ Set) error {
	return errors.New("stub: policy.Write")
}
