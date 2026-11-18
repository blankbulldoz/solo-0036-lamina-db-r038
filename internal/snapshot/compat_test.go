package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"lamina/internal/catalog"
	"lamina/internal/versions"
	"testing"
)

func digestHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestOldImageDerivesLogicalBoundary(t *testing.T) {
	rel := catalog.Relation{Name: "spectra", Primary: "sample", Created: 1,
		Columns: []catalog.Column{{Name: "sample", Type: "int"}}}
	cat := catalog.New()
	cat.Relations["spectra"] = rel
	store := versions.New()
	store.Ensure("spectra")
	old := Image{Format: 1, Sequence: 3, Horizon: 2, Catalog: cat, Store: store}
	data, err := Encode(old)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite the envelope payload stripping the fields unknown to the old
	// format, then recompute the digest exactly as an old binary would.
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(env.Payload, &raw); err != nil {
		t.Fatal(err)
	}
	delete(raw, "retain_from")
	delete(raw, "policy")
	payload, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	env.Payload = payload
	env.Digest = digestHex(payload)
	downgraded, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	image, err := Decode(downgraded)
	if err != nil {
		t.Fatalf("old-format image rejected: %v", err)
	}
	if image.RetainFrom != 2 {
		t.Fatalf("retain_from = %d, want derived horizon 2", image.RetainFrom)
	}
}
