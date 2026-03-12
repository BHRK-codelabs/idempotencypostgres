package idempotencypostgres

import (
	"testing"

	"github.com/BHRK-codelabs/corekit/configkit"
)

func TestTableSchema(t *testing.T) {
	t.Parallel()

	if got := tableSchema("platform.idempotency_records"); got != "platform" {
		t.Fatalf("unexpected schema: %q", got)
	}
	if got := tableSchema("idempotency_records"); got != "" {
		t.Fatalf("expected empty schema, got %q", got)
	}
}

func TestNewModuleRequiresDatabaseURL(t *testing.T) {
	t.Parallel()

	cfg := configkit.New()
	cfg.Database.URL = ""

	if _, err := NewModule(cfg); err == nil {
		t.Fatal("expected error when database url is empty")
	}
}
