package main

import (
	"testing"

	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
)

func TestManifestDeclaresPerConnectionFloppyServer(t *testing.T) {
	t.Parallel()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	capabilities := parsed.GetCapabilities()
	if len(capabilities) != 1 {
		t.Fatalf("capabilities = %d, want 1", len(capabilities))
	}
	schemas := capabilities[0].GetConfigSchema()
	if len(schemas) != 1 || schemas[0].GetKey() != "floppy" || !schemas[0].GetRequired() {
		t.Fatalf("connection config schemas = %#v", schemas)
	}
	if len(parsed.GetGlobalConfigSchema()) != 0 {
		t.Fatalf("global config schemas = %#v, want none", parsed.GetGlobalConfigSchema())
	}
}
