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
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/listenbrainz/1/validate-token" {
			t.Errorf("path = %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
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
	if response.GetCredentials().GetSecretAttributes()[configBaseURL] != upstream.URL {
		t.Fatalf("connection base URL = %q", response.GetCredentials().GetSecretAttributes()[configBaseURL])
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
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/history/":
			mu.Lock()
			isWatched := watched
			mu.Unlock()
			results := []any{}
			if isWatched {
				if r.URL.Query().Get("offset") == "50" {
					results = append(results, historyDayPayload(occurredAt))
				} else {
					results = append(results, historyDayPayload(occurredAt.Add(-time.Minute)))
				}
			}
			next := any(nil)
			total := len(results)
			offset := 0
			if isWatched && r.URL.Query().Get("offset") != "50" {
				next = upstream.URL + "/api/v1/history/?offset=50"
				total = 51
			} else if r.URL.Query().Get("offset") == "50" {
				offset = 50
			}
			writeJSON(t, w, map[string]any{
				"pagination": map[string]any{"total": total, "limit": 50, "offset": offset, "next": next},
				"results":    results,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/scrobble/":
			var payload scrobblePayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode payload: %v", err)
				http.Error(w, "invalid payload", http.StatusBadRequest)
				return
			}
			if payload.Action != "stop" || payload.MediaType != "episode" || payload.IDs["tmdb"] != "1668" ||
				payload.SeasonNumber == nil || *payload.SeasonNumber != 1 || payload.EpisodeNumber == nil || *payload.EpisodeNumber != 2 ||
				payload.Completed == nil || !*payload.Completed || payload.PlayedAt != occurredAt.Format(time.RFC3339Nano) {
				t.Errorf("payload = %#v", payload)
				http.Error(w, "unexpected payload", http.StatusBadRequest)
				return
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
			ExternalIds:       map[string]string{"tmdb": "episode-9999"},
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
	mu.Lock()
	gotScrobbleCalls := scrobbleCalls
	mu.Unlock()
	if gotScrobbleCalls != 1 {
		t.Fatalf("scrobble calls = %d, want 1", gotScrobbleCalls)
	}
}

func TestListWatchedUsesStableTraversalAndReturnsIncrementalCursor(t *testing.T) {
	t.Parallel()
	playedAt := time.Date(2026, time.August, 5, 14, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var queries []url.Values
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.Query())
		mu.Unlock()
		offset := r.URL.Query().Get("offset")
		if offset == "0" {
			writeJSON(t, w, map[string]any{
				"pagination": map[string]any{"total": 2, "limit": 1, "offset": 0, "next": nil},
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
	first, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authenticatedContext(upstream.URL), PageSize: 100,
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !first.GetCompleteSnapshot() || first.GetNextPageToken() == "" || first.GetNextCursor() != "" || len(first.GetItems()) != 1 {
		t.Fatalf("first = %#v", first)
	}
	item := first.GetItems()[0]
	if item.GetMedia().GetSeriesExternalIds()["tmdb"] != "1668" || len(item.GetMedia().GetExternalIds()) != 0 ||
		item.GetMedia().GetMediaItemId() != "tmdb:1668:1:2" || item.GetMedia().GetSeasonNumber() != 1 || item.GetWatched().GetPlayCount() != 1 {
		t.Fatalf("item = %#v", item)
	}
	second, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authenticatedContext(upstream.URL), PageSize: 100, PageToken: first.GetNextPageToken(),
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.GetNextPageToken() != "" || second.GetNextCursor() != playedAt.Add(-providerCursorOverlap).Format(time.RFC3339Nano) || !second.GetCompleteSnapshot() {
		t.Fatalf("second = %#v", second)
	}
	mu.Lock()
	gotQueries := append([]url.Values(nil), queries...)
	mu.Unlock()
	if len(gotQueries) != 2 || gotQueries[0].Get("end_date") != "" || gotQueries[1].Get("end_date") != "2026-08-06" || gotQueries[1].Get("offset") != "1" {
		t.Fatalf("queries = %#v", gotQueries)
	}
}

func TestListProgressMapsResumeState(t *testing.T) {
	t.Parallel()
	updatedAt := time.Date(2026, time.August, 5, 14, 59, 0, 0, time.UTC)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/playback/progress/" || r.URL.Query().Get("completed") != "false" {
			t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
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
	if response.GetNextCursor() != updatedAt.Add(-providerCursorOverlap).Format(time.RFC3339Nano) {
		t.Fatalf("next cursor = %q", response.GetNextCursor())
	}
	item := response.GetItems()[0]
	if item.GetMedia().GetExternalIds()["tmdb"] != "603" || item.GetMedia().GetExternalIds()["imdb"] != "tt0133093" || item.GetProgress().GetProgressPercent() != 25 {
		t.Fatalf("item = %#v", item)
	}
}

func TestListProgressPageTokenSurvivesEarlierItemDeletion(t *testing.T) {
	t.Parallel()
	firstUpdatedAt := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	secondUpdatedAt := firstUpdatedAt.Add(time.Minute)
	var mu sync.Mutex
	entries := []any{
		progressPayload("603", firstUpdatedAt),
		progressPayload("604", secondUpdatedAt),
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		results := append([]any(nil), entries...)
		mu.Unlock()
		writeJSON(t, w, map[string]any{
			"pagination": map[string]any{"total": len(results), "limit": 200, "offset": 0, "next": nil},
			"results":    results,
		})
	}))
	defer upstream.Close()

	server := NewServer(upstream.Client())
	request := &pluginv1.WatchSyncListRemoteStateRequest{
		Context:    authenticatedContext(upstream.URL),
		PageSize:   1,
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS},
	}
	first, err := server.ListRemoteState(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetFault() != nil || len(first.GetItems()) != 1 || first.GetItems()[0].GetMedia().GetExternalIds()["tmdb"] != "603" || first.GetNextPageToken() == "" {
		t.Fatalf("first = %#v", first)
	}

	mu.Lock()
	entries = entries[1:]
	mu.Unlock()
	request.PageToken = first.GetNextPageToken()
	second, err := server.ListRemoteState(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if second.GetFault() != nil || len(second.GetItems()) != 1 || second.GetItems()[0].GetMedia().GetExternalIds()["tmdb"] != "604" || second.GetNextPageToken() != "" {
		t.Fatalf("second = %#v", second)
	}
}

func TestRequestTimeoutIsTemporary(t *testing.T) {
	t.Parallel()
	fault := faultForHTTPResponse(&http.Response{StatusCode: http.StatusRequestTimeout, Header: make(http.Header)})
	if fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("fault = %#v", fault)
	}
}

func TestProgressEpisodeKeepsSeriesIDsOutOfEpisodeIdentity(t *testing.T) {
	t.Parallel()
	season, episode := int32(2), int32(3)
	updatedAt := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	state := progressState(progressEntry{
		MediaType: "episode", Source: "tmdb", MediaID: json.RawMessage(`1668`),
		SeasonNumber: &season, EpisodeNumber: &episode, IDs: map[string]any{"tmdb": "1668", "tvdb": "79168"},
		Title: "The One", SeriesTitle: "Friends", Position: 600, Duration: 2400,
		UpdatedAt: updatedAt.Format(time.RFC3339Nano),
	}, updatedAt)
	if state == nil || state.GetMedia().GetMediaItemId() != "tmdb:1668:2:3" || len(state.GetMedia().GetExternalIds()) != 0 ||
		state.GetMedia().GetSeriesExternalIds()["tmdb"] != "1668" || state.GetMedia().GetSeriesExternalIds()["tvdb"] != "79168" {
		t.Fatalf("state = %#v", state)
	}
}

func TestNullHistoryInstanceIDsRemainDistinct(t *testing.T) {
	t.Parallel()
	watchedAt := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	entry := historyEntry{
		MediaType: "movie", Status: "Completed", PlayedAt: watchedAt.Format(time.RFC3339Nano),
		InstanceID: json.RawMessage(`null`),
		Item:       historyItem{MediaType: "movie", Source: "tmdb", MediaID: json.RawMessage(`603`)},
	}
	first := watchedState(entry, watchedAt)
	entry.Item.MediaID = json.RawMessage(`604`)
	second := watchedState(entry, watchedAt)
	if first == nil || second == nil || first.GetProviderItemKey() == second.GetProviderItemKey() {
		t.Fatalf("provider keys = %q and %q", first.GetProviderItemKey(), second.GetProviderItemKey())
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
		Credentials:    credentials("token", baseURL),
	}
}

func TestAuthenticatedClientPrefersConnectionURLAndSupportsLegacyFallback(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	connectionClient, fault := server.authenticatedClient(&pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId:   capabilityID,
		ProviderConfig: providerConfig("https://legacy.example.com"),
		Credentials:    credentials("token", "https://personal.example.com"),
	})
	if fault != nil || connectionClient.baseURL.String() != "https://personal.example.com" {
		t.Fatalf("connection client = %#v, fault = %#v", connectionClient, fault)
	}
	legacyClient, fault := server.authenticatedClient(&pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId:   capabilityID,
		ProviderConfig: providerConfig("https://legacy.example.com"),
		Credentials:    &pluginv1.WatchSyncCredentials{AccessToken: "token"},
	})
	if fault != nil || legacyClient.baseURL.String() != "https://legacy.example.com" {
		t.Fatalf("legacy client = %#v, fault = %#v", legacyClient, fault)
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

func progressPayload(mediaID string, updatedAt time.Time) map[string]any {
	return map[string]any{
		"media_type": "movie", "source": "tmdb", "media_id": mediaID,
		"ids": map[string]any{"tmdb": mediaID}, "title": "Movie " + mediaID,
		"position_seconds": 600, "duration_seconds": 2400, "completed": false,
		"updated_at": updatedAt.Format(time.RFC3339Nano),
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
		t.Errorf("encode response: %v", err)
	}
}

func TestBaseURLRejectsEmbeddedCredentials(t *testing.T) {
	t.Parallel()
	_, err := newAPIClient("https://user:pass@example.com", "token", nil)
	if err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("err = %v", err)
	}
}

func TestRawStringDecodesEscapesAndNull(t *testing.T) {
	t.Parallel()
	if got := rawString(json.RawMessage(`"show\/name\u0031"`)); got != "show/name1" {
		t.Fatalf("rawString escaped value = %q", got)
	}
	if got := rawString(json.RawMessage(`null`)); got != "" {
		t.Fatalf("rawString null = %q", got)
	}
}
