package provider

import (
	"maps"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestScrobblePayloadIdentifiesAnEpisodeByOneSeriesID(t *testing.T) {
	t.Parallel()
	episode := pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE
	for _, test := range []struct {
		name      string
		mediaType pluginv1.WatchSyncMediaType
		ids       map[string]string
		seriesIDs map[string]string
		want      map[string]string
	}{
		{
			// Attack on Titan: TVDB episode 267440 is Strong Medicine S5E2.
			name: "TMDB first", mediaType: episode,
			ids:       map[string]string{"tmdb": "1235"},
			seriesIDs: map[string]string{"tmdb": "1429", "tvdb": "267440", "imdb": "tt2560140"},
			want:      map[string]string{"tmdb": "1429"},
		},
		{
			name: "IMDb before TVDB", mediaType: episode,
			seriesIDs: map[string]string{"tvdb": "267440", "imdb": "tt2560140"},
			want:      map[string]string{"imdb": "tt2560140"},
		},
		{
			name: "TVDB alone", mediaType: episode,
			seriesIDs: map[string]string{"tvdb": "267440"},
			want:      map[string]string{"tvdb": "267440"},
		},
		{
			name: "episode IDs without series IDs", mediaType: episode,
			ids:  map[string]string{"tvdb": "4567", "imdb": "tt7654"},
			want: map[string]string{"tvdb": "4567", "imdb": "tt7654"},
		},
		{
			name: "movie", mediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
			ids:  map[string]string{"tmdb": "603", "imdb": "tt0133093"},
			want: map[string]string{"tmdb": "603", "imdb": "tt0133093"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			payload, _, fault := payloadFromEvent(&pluginv1.WatchSyncEvent{
				EventId:   "event-1",
				Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
				Media: &pluginv1.WatchSyncMedia{
					MediaType: test.mediaType, ExternalIds: test.ids, SeriesExternalIds: test.seriesIDs,
					SeasonNumber: 1, EpisodeNumber: 13,
				},
			})
			if fault != nil {
				t.Fatalf("fault = %v", fault)
			}
			if !maps.Equal(payload.IDs, test.want) {
				t.Fatalf("ids = %v, want %v", payload.IDs, test.want)
			}
		})
	}
}
