package main

import (
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
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

func TestManifestAdvertisesMovieAndSeriesRatings(t *testing.T) {
	t.Parallel()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	descriptor := parsed.GetCapabilities()[0].GetWatchSyncProvider()
	if !descriptor.GetImportRatings() || !descriptor.GetExportRatings() {
		t.Fatalf("ratings = import %t export %t, want both", descriptor.GetImportRatings(), descriptor.GetExportRatings())
	}
	if descriptor.GetImportFavorites() || descriptor.GetExportFavorites() || descriptor.GetImportWatchlist() || descriptor.GetExportWatchlist() {
		t.Fatalf("descriptor advertises favorites or watchlist: %v", descriptor)
	}
	media := map[pluginv1.WatchSyncMediaType]bool{}
	for _, mediaType := range descriptor.GetSupportedMediaTypes() {
		media[mediaType] = true
	}
	if !media[pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE] || !media[pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES] ||
		!media[pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE] {
		t.Fatalf("supported media types = %v", descriptor.GetSupportedMediaTypes())
	}
}
