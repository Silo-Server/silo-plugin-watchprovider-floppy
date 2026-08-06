package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExchangeAPIKeyValidatesAndReturnsHostOwnedCredentials(t *testing.T) {
	t.Parallel()
	var authorization string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/listenbrainz/1/validate-token" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		authorization = r.Header.Get("Authorization")
		writeJSON(t, w, map[string]any{"valid": true, "user_name": "quick"})
	}))
	defer upstream.Close()

	response, err := NewServer(upstream.Client()).ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{
		CapabilityId:   capabilityID,
		ProviderConfig: providerConfig(upstream.URL),
		ApiKey:         "secret-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil {
		t.Fatalf("fault = %v", response.GetFault())
	}
	if authorization != "Token secret-token" {
		t.Fatalf("Authorization = %q", authorization)
	}
	if response.GetCredentials().GetAccessToken() != "secret-token" || response.GetCredentials().GetTokenType() != "Bearer" {
		t.Fatalf("credentials = %#v", response.GetCredentials())
	}
	if response.GetAccount().GetExternalSubject() != "quick" {
		t.Fatalf("account = %#v", response.GetAccount())
	}
}

func TestApplyWatchedIsIdempotentAcrossAtLeastOnceDelivery(t *testing.T) {
	t.Parallel()
	occurredAt := time.Date(2026, time.August, 5, 12, 34, 56, 0, time.UTC)
	var mu sync.Mutex
	watched := false
	scrobbleCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("Authorization = %q", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/history/":
			mu.Lock()
			isWatched := watched
			mu.Unlock()
			results := []any{}
			if isWatched {
				results = append(results, historyDayPayload(occurredAt))
			}
			writeJSON(t, w, map[string]any{
				"pagination": map[string]any{"total": len(results), "limit": 10, "offset": 0, "next": nil},
				"results":    results,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/scrobble/":
			var payload scrobblePayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.Action != "stop" || payload.MediaType != "episode" || payload.IDs["tmdb"] != "1668" ||
				payload.SeasonNumber == nil || *payload.SeasonNumber != 1 || payload.EpisodeNumber == nil || *payload.EpisodeNumber != 2 ||
				payload.Completed == nil || !*payload.Completed || payload.PlayedAt != occurredAt.Format(time.RFC3339Nano) {
				t.Fatalf("payload = %#v", payload)
			}
			mu.Lock()
			watched = true
			scrobbleCalls++
			mu.Unlock()
			writeJSON(t, w, map[string]any{"detail": "accepted"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	event := &pluginv1.WatchSyncEvent{
		EventId:    "history-1",
		Operation:  pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		OccurredAt: timestamppb.New(occurredAt),
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeriesExternalIds: map[string]string{"tmdb": "1668"},
			SeasonNumber:      1,
			EpisodeNumber:     2,
		},
	}
	server := NewServer(upstream.Client())
	for index, expected := range []pluginv1.WatchSyncApplyStatus{
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
	} {
		response, err := server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
			Context: authenticatedContext(upstream.URL), Events: []*pluginv1.WatchSyncEvent{event},
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.GetFault() != nil || len(response.GetResults()) != 1 || response.GetResults()[0].GetStatus() != expected {
			t.Fatalf("attempt %d response = %#v", index+1, response)
		}
	}
	if scrobbleCalls != 1 {
		t.Fatalf("scrobble calls = %d, want 1", scrobbleCalls)
	}
}

func TestListWatchedUsesStableTraversalAndReturnsIncrementalCursor(t *testing.T) {
	t.Parallel()
	highWater := time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC)
	playedAt := highWater.Add(-time.Hour)
	var queries []url.Values
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		offset := r.URL.Query().Get("offset")
		if offset == "0" {
			writeJSON(t, w, map[string]any{
				"pagination": map[string]any{"total": 2, "limit": 1, "offset": 0, "next": "next"},
				"results":    []any{historyDayPayload(playedAt)},
			})
			return
		}
		writeJSON(t, w, map[string]any{
			"pagination": map[string]any{"total": 2, "limit": 1, "offset": 1, "next": nil},
			"results":    []any{},
		})
	}))
	defer upstream.Close()

	server := NewServer(upstream.Client())
	server.now = func() time.Time { return highWater }
	first, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authenticatedContext(upstream.URL), PageSize: 1,
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !first.GetCompleteSnapshot() || first.GetNextPageToken() == "" || first.GetNextCursor() != "" || len(first.GetItems()) != 1 {
		t.Fatalf("first = %#v", first)
	}
	item := first.GetItems()[0]
	if item.GetMedia().GetSeriesExternalIds()["tmdb"] != "1668" || item.GetMedia().GetSeasonNumber() != 1 || item.GetWatched().GetPlayCount() != 1 {
		t.Fatalf("item = %#v", item)
	}
	second, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authenticatedContext(upstream.URL), PageSize: 1, PageToken: first.GetNextPageToken(),
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.GetNextPageToken() != "" || second.GetNextCursor() != highWater.Format(time.RFC3339Nano) || !second.GetCompleteSnapshot() {
		t.Fatalf("second = %#v", second)
	}
	if len(queries) != 2 || queries[0].Get("end_date") != "2026-08-06" || queries[1].Get("offset") != "1" {
		t.Fatalf("queries = %#v", queries)
	}
}

func TestListProgressMapsResumeState(t *testing.T) {
	t.Parallel()
	highWater := time.Date(2026, time.August, 5, 15, 0, 0, 0, time.UTC)
	updatedAt := highWater.Add(-time.Minute)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/playback/progress/" || r.URL.Query().Get("completed") != "false" {
			t.Fatalf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		writeJSON(t, w, map[string]any{
			"pagination": map[string]any{"total": 1, "limit": 50, "offset": 0, "next": nil},
			"results": []any{map[string]any{
				"media_type": "movie", "source": "tmdb", "media_id": "603",
				"ids": map[string]any{"imdb": "tt0133093"}, "title": "The Matrix",
				"position_seconds": 2040, "duration_seconds": 8160, "completed": false,
				"updated_at": updatedAt.Format(time.RFC3339Nano),
			}},
		})
	}))
	defer upstream.Close()

	server := NewServer(upstream.Client())
	server.now = func() time.Time { return highWater }
	response, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context:    authenticatedContext(upstream.URL),
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil || len(response.GetItems()) != 1 {
		t.Fatalf("response = %#v", response)
	}
	item := response.GetItems()[0]
	if item.GetMedia().GetExternalIds()["tmdb"] != "603" || item.GetMedia().GetExternalIds()["imdb"] != "tt0133093" || item.GetProgress().GetProgressPercent() != 25 {
		t.Fatalf("item = %#v", item)
	}
}

func TestApplyRateLimitIsConnectionWide(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/scrobble/" {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeJSON(t, w, map[string]any{"pagination": map[string]any{"total": 0, "limit": 10, "offset": 0}, "results": []any{}})
	}))
	defer upstream.Close()
	response, err := NewServer(upstream.Client()).ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authenticatedContext(upstream.URL),
		Events: []*pluginv1.WatchSyncEvent{{
			EventId: "start-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
			Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: map[string]string{"tmdb": "603"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED || response.GetFault().GetRetryAfter().AsDuration() != 30*time.Second || len(response.GetResults()) != 0 {
		t.Fatalf("response = %#v", response)
	}
}

func providerConfig(baseURL string) *pluginv1.WatchSyncProviderConfig {
	return &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configBaseURL: baseURL}}
}

func authenticatedContext(baseURL string) *pluginv1.WatchSyncAuthenticatedContext {
	return &pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId:   capabilityID,
		ProviderConfig: providerConfig(baseURL),
		Credentials:    credentials("token"),
	}
}

func historyDayPayload(playedAt time.Time) map[string]any {
	return map[string]any{
		"date": playedAt.Format(time.DateOnly),
		"entries": []any{map[string]any{
			"media_type": "episode", "title": "The One", "display_title": "The One",
			"status": "Completed", "played_at_local": playedAt.Format(time.RFC3339Nano), "play_count": 1, "instance_id": 42,
			"item": map[string]any{
				"media_type": "episode", "media_id": "1668", "source": "tmdb", "title": "Friends",
				"season_number": 1, "episode_number": 2,
				"provider_external_ids": map[string]any{"tmdb_id": "1668", "tvdb_id": "79168"},
			},
		}},
	}
}

func TestCompletedHistoryEntry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		status string
		want   bool
	}{
		{status: "Completed", want: true},
		{status: " completed ", want: true},
		{status: "In progress", want: false},
		{status: "", want: false},
	} {
		if got := completedHistoryEntry(historyEntry{Status: test.status}); got != test.want {
			t.Errorf("completedHistoryEntry(%q) = %t, want %t", test.status, got, test.want)
		}
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func TestBaseURLRejectsEmbeddedCredentials(t *testing.T) {
	t.Parallel()
	_, err := newAPIClient("https://user:pass@example.com", "token", nil)
	if err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("err = %v", err)
	}
}
