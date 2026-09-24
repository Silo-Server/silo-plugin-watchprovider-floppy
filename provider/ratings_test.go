package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

var ratingKinds = []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING}

func TestListRatingsPagesMoviesThenTVAsOneCompleteSnapshot(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var requests []string
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer token" ||
			query.Get("rating") != "rated" || query.Get("status") != "0,1,2,3,4,no_status" ||
			query.Get("sort") != "id" || query.Get("direction") != "asc" {
			t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		page := r.URL.Path + "@" + query.Get("offset") + "/" + query.Get("limit")
		mu.Lock()
		requests = append(requests, page)
		mu.Unlock()
		switch page {
		case "/api/v1/media/movie/@0/2":
			writeJSON(t, w, ratedPage(2, 3, 0, upstream.URL+"/api/v1/media/movie/?offset=2",
				ratedEntry(1, 7.5, ratedItem("movie", "603", "The Matrix", "1999-03-31T00:00:00Z", map[string]any{"imdb": "tt0133093"})),
				ratedEntry(2, 6, ratedItem("movie", "604", "The Matrix Reloaded", "2003-05-15T00:00:00Z", nil)),
			))
		case "/api/v1/media/movie/@1/3":
			// The overlap: the previous page's last title comes back first.
			writeJSON(t, w, ratedPage(3, 3, 1, "",
				ratedEntry(2, 6, ratedItem("movie", "604", "The Matrix Reloaded", "2003-05-15T00:00:00Z", nil)),
				ratedEntry(3, 9.2, ratedItem("movie", "605", "The Matrix Revolutions", "2003-11-05T00:00:00Z", nil)),
			))
		case "/api/v1/media/tv/@0/2":
			writeJSON(t, w, ratedPage(2, 1, 0, "",
				ratedEntry(10, 10, ratedItem("tv", "1668", "Friends", "1994-09-22T00:00:00Z", map[string]any{"tvdb": "79168", "imdb": "tt0108778"})),
			))
		default:
			t.Errorf("unexpected page %s?%s", r.URL.Path, r.URL.RawQuery)
			http.Error(w, "unexpected page", http.StatusBadRequest)
		}
	}))
	defer upstream.Close()

	server := NewServer(upstream.Client())
	var items []*pluginv1.WatchSyncRemoteState
	pageToken := ""
	for page := 0; page < 10; page++ {
		response, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authenticatedContext(upstream.URL), PageSize: 2, PageToken: pageToken, StateKinds: ratingKinds,
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.GetFault() != nil || !response.GetCompleteSnapshot() || response.GetNextCursor() != "" {
			t.Fatalf("page %d = %#v", page, response)
		}
		items = append(items, response.GetItems()...)
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if pageToken != "" {
		t.Fatal("traversal did not finish")
	}

	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	wantRequests := []string{"/api/v1/media/movie/@0/2", "/api/v1/media/movie/@1/3", "/api/v1/media/tv/@0/2"}
	if !slices.Equal(gotRequests, wantRequests) {
		t.Fatalf("requests = %v, want %v", gotRequests, wantRequests)
	}

	want := []struct {
		key       string
		mediaType pluginv1.WatchSyncMediaType
		rating    int32
		title     string
		year      int32
		ids       map[string]string
	}{
		{"rating:movie:tmdb:603", pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, 8, "The Matrix", 1999, map[string]string{"tmdb": "603", "imdb": "tt0133093"}},
		{"rating:movie:tmdb:604", pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, 6, "The Matrix Reloaded", 2003, map[string]string{"tmdb": "604"}},
		{"rating:movie:tmdb:605", pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, 9, "The Matrix Revolutions", 2003, map[string]string{"tmdb": "605"}},
		{"rating:tv:tmdb:1668", pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES, 10, "Friends", 1994, map[string]string{"tmdb": "1668", "tvdb": "79168", "imdb": "tt0108778"}},
	}
	if len(items) != len(want) {
		t.Fatalf("items = %d, want %d: %#v", len(items), len(want), items)
	}
	for index, expected := range want {
		item := items[index]
		media := item.GetMedia()
		if item.GetProviderItemKey() != expected.key || media.GetMediaType() != expected.mediaType ||
			item.GetRating().GetRating() != expected.rating || item.GetRating().GetRemoved() || item.GetRating().GetRatedAt() != nil ||
			media.GetTitle() != expected.title || media.GetYear() != expected.year ||
			len(media.GetSeriesExternalIds()) != 0 || len(media.GetExternalIds()) != len(expected.ids) {
			t.Fatalf("item %d = %#v", index, item)
		}
		for namespace, id := range expected.ids {
			if media.GetExternalIds()[namespace] != id {
				t.Fatalf("item %d external ids = %v, want %v", index, media.GetExternalIds(), expected.ids)
			}
		}
	}
}

func TestListRatingsSkipsUnwritableAndDuplicateTitles(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/media/tv/" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		anime := ratedItem("tv", "1429", "Attack on Titan", "2013-04-07T00:00:00Z", nil)
		anime["library_media_type"] = "anime"
		manual := ratedItem("tv", "42", "Home Videos", "", map[string]any{"tmdb": "99999"})
		manual["source"] = "manual"
		writeJSON(t, w, ratedPage(20, 5, 0, "",
			ratedEntry(1, 8, ratedItem("tv", "1668", "Friends", "1994-09-22T00:00:00Z", nil)),
			ratedEntry(2, 3, ratedItem("tv", "1668", "Friends", "1994-09-22T00:00:00Z", nil)),
			// Unwritable titles are skipped before any history read, even unscored.
			ratedEntry(3, nil, anime),
			ratedEntry(4, 7, manual),
			map[string]any{"id": 5, "score": 5, "item": nil},
		))
	}))
	defer upstream.Close()

	response, err := NewServer(upstream.Client()).ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authenticatedContext(upstream.URL), StateKinds: ratingKinds,
		PageToken: encodePageToken(ratingTraversal{Phase: floppyTV}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil || response.GetNextPageToken() != "" || len(response.GetItems()) != 1 {
		t.Fatalf("response = %#v", response)
	}
	if item := response.GetItems()[0]; item.GetProviderItemKey() != "rating:tv:tmdb:1668" || item.GetRating().GetRating() != 8 {
		t.Fatalf("item = %#v", item)
	}
}

func TestListRatingsResolvesUnscoredTitleFromHistory(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var historyReads []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/media/movie/":
			writeJSON(t, w, ratedPage(20, 2, 0, "",
				ratedEntry(1, 7, ratedItem("movie", "603", "The Matrix", "", nil)),
				// A rewatch left the newest consumption unscored.
				ratedEntry(2, nil, ratedItem("movie", "604", "The Matrix Reloaded", "", nil)),
			))
		case "/api/v1/media/movie/tmdb/604/history/":
			mu.Lock()
			historyReads = append(historyReads, r.URL.RawQuery)
			mu.Unlock()
			writeJSON(t, w, historyPage(
				historyRow(11, 6, 3, "2024-01-01T00:00:00Z", "2025-06-01T20:00:00Z"),
				// Created later but watched earlier: not the score Floppy shows.
				historyRow(12, 9, 3, "2025-02-01T00:00:00Z", "2019-03-01T20:00:00Z"),
				historyRow(13, nil, 1, "2026-01-01T00:00:00Z", ""),
			))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer upstream.Close()

	response, err := NewServer(upstream.Client()).ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authenticatedContext(upstream.URL), StateKinds: ratingKinds,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil || !response.GetCompleteSnapshot() || len(response.GetItems()) != 2 {
		t.Fatalf("response = %#v", response)
	}
	for index, want := range []struct {
		key    string
		rating int32
	}{{"rating:movie:tmdb:603", 7}, {"rating:movie:tmdb:604", 6}} {
		if item := response.GetItems()[index]; item.GetProviderItemKey() != want.key || item.GetRating().GetRating() != want.rating {
			t.Fatalf("item %d = %#v, want %s rated %d", index, item, want.key, want.rating)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(historyReads, []string{"limit=100&offset=0"}) {
		t.Fatalf("history reads = %v, want one for the unscored title", historyReads)
	}
}

func TestListRatingsFailsTraversalWhenUnscoredTitleCannotBeResolved(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		history func(w http.ResponseWriter)
	}{
		{"no scored consumption", func(w http.ResponseWriter) {
			writeJSON(t, w, historyPage(historyRow(11, nil, 3, "2024-01-01T00:00:00Z", "2024-01-01T20:00:00Z")))
		}},
		{"per-play history", func(w http.ResponseWriter) {
			play := historyRow(51, nil, 3, "2024-01-01T00:00:00Z", "2024-01-01T20:00:00Z")
			play["external_id"] = nil
			writeJSON(t, w, historyPage(play))
		}},
		{"untracked since the listing", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusNotFound)
		}},
		{"unreadable history", func(w http.ResponseWriter) {
			writeJSON(t, w, historyPage(historyRow(11, 6, 3, "yesterday", "")))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/media/movie/tmdb/604/history/" {
					test.history(w)
					return
				}
				writeJSON(t, w, ratedPage(20, 2, 0, "",
					ratedEntry(1, 7, ratedItem("movie", "603", "The Matrix", "", nil)),
					ratedEntry(2, nil, ratedItem("movie", "604", "The Matrix Reloaded", "", nil)),
				))
			}))
			defer upstream.Close()

			response, err := NewServer(upstream.Client()).ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
				Context: authenticatedContext(upstream.URL), StateKinds: ratingKinds,
			})
			if err != nil {
				t.Fatal(err)
			}
			if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY ||
				len(response.GetItems()) != 0 || response.GetNextPageToken() != "" {
				t.Fatalf("response = %#v", response)
			}
		})
	}
}

func TestListRatingsChecksThePageOverlap(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		results   []any
		wantFault bool
	}{
		{"overlap matches", []any{
			ratedEntry(2, 6, ratedItem("movie", "604", "The Matrix Reloaded", "", nil)),
			ratedEntry(3, 9, ratedItem("movie", "605", "The Matrix Revolutions", "", nil)),
		}, false},
		// A rating cleared before the offset shifted every later title back.
		{"titles shifted", []any{
			ratedEntry(3, 9, ratedItem("movie", "605", "The Matrix Revolutions", "", nil)),
		}, true},
		{"list shrank below the offset", nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if query := r.URL.Query(); query.Get("offset") != "1" || query.Get("limit") != "21" {
					t.Errorf("query = %s, want the page to start one title early", r.URL.RawQuery)
				}
				writeJSON(t, w, ratedPage(21, 1+len(test.results), 1, "", test.results...))
			}))
			defer upstream.Close()

			response, err := NewServer(upstream.Client()).ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
				Context: authenticatedContext(upstream.URL), StateKinds: ratingKinds,
				PageToken: encodePageToken(ratingTraversal{Phase: floppyMovie, Offset: 2, LastKey: "item:2"}),
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.wantFault {
				if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || len(response.GetItems()) != 0 {
					t.Fatalf("response = %#v", response)
				}
				return
			}
			// The overlap title was reported by the previous page.
			if response.GetFault() != nil || len(response.GetItems()) != 1 ||
				response.GetItems()[0].GetProviderItemKey() != "rating:movie:tmdb:605" {
				t.Fatalf("response = %#v", response)
			}
		})
	}
}

func TestListRatingsCarriesTheLastKeyAcrossPages(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, ratedPage(2, 5, 0, "",
			ratedEntry(1, 7, ratedItem("movie", "603", "The Matrix", "", nil)),
			// The page ends on a title the plugin skips; the overlap still checks it.
			map[string]any{"id": 9, "score": 5, "item": nil},
		))
	}))
	defer upstream.Close()

	response, err := NewServer(upstream.Client()).ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authenticatedContext(upstream.URL), PageSize: 2, StateKinds: ratingKinds,
	})
	if err != nil {
		t.Fatal(err)
	}
	next, fault := ratingTraversalFromRequest(&pluginv1.WatchSyncListRemoteStateRequest{PageToken: response.GetNextPageToken()})
	if response.GetFault() != nil || fault != nil || next != (ratingTraversal{Phase: floppyMovie, Offset: 2, LastKey: "item:9"}) {
		t.Fatalf("response = %#v, next = %#v", response, next)
	}
}

func TestListRatingsRejectsForeignPageToken(t *testing.T) {
	t.Parallel()
	for _, token := range []ratingTraversal{
		{Phase: "episode"},
		{Phase: floppyMovie, Offset: 20},
		{Phase: floppyTV, LastKey: "item:4"},
	} {
		response, err := NewServer(nil).ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authenticatedContext("https://floppy.example.com"), StateKinds: ratingKinds,
			PageToken: encodePageToken(token),
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("token %#v response = %#v", token, response)
		}
	}
}

func TestRatingFromScoreRoundsHalfUpAndClamps(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		score float64
		want  int32
	}{
		{score: 0, want: 1},
		{score: 0.4, want: 1},
		{score: 1.4, want: 1},
		{score: 1.5, want: 2},
		{score: 6.5, want: 7},
		{score: 7.4, want: 7},
		{score: 7.5, want: 8},
		{score: 9.5, want: 10},
		{score: 10, want: 10},
		{score: -1, want: 1},
		{score: 12, want: 10},
	} {
		if got := ratingFromScore(test.score); got != test.want {
			t.Errorf("ratingFromScore(%v) = %d, want %d", test.score, got, test.want)
		}
	}
}

func TestApplyRatingSetsAndClearsScoresOnce(t *testing.T) {
	t.Parallel()
	floppy := newFakeRatingFloppy(t, map[string][]*fakeRatingRow{
		"movie/603": {{id: 21, status: 3, created: "2026-01-01T00:00:00Z", endDate: "2026-01-01T20:00:00Z"}},
		"tv/1668":   {{id: 31, score: floatPointer(7), status: 1, created: "2025-01-01T00:00:00Z"}},
	})
	events := []*pluginv1.WatchSyncEvent{
		{
			EventId: "set-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 8,
			Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: map[string]string{"tmdb": "603", "imdb": "tt0133093"}},
		},
		{
			// A removal addressed only by the key a previous snapshot returned.
			EventId: "remove-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING,
			ProviderItemKey: "rating:tv:tmdb:1668",
			Media:           &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES},
		},
	}
	for attempt, wantStatus := range []pluginv1.WatchSyncApplyStatus{
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
	} {
		response := floppy.apply(t, events...)
		if response.GetFault() != nil || len(response.GetResults()) != 2 {
			t.Fatalf("attempt %d response = %#v", attempt, response)
		}
		for _, result := range response.GetResults() {
			if result.GetStatus() != wantStatus || result.GetFault() != nil {
				t.Fatalf("attempt %d result = %#v, want %s", attempt, result, wantStatus)
			}
		}
	}
	floppy.wantRequests(t,
		"GET /api/v1/media/movie/tmdb/603/history/",
		`PATCH /api/v1/media/movie/tmdb/603/history/21/ {"score":8}`,
		"GET /api/v1/media/tv/tmdb/1668/history/",
		`PATCH /api/v1/media/tv/tmdb/1668/history/31/ {"score":null}`,
		// The redelivery finds both titles already in the desired state.
		"GET /api/v1/media/movie/tmdb/603/history/",
		"GET /api/v1/media/tv/tmdb/1668/history/",
	)
}

func TestApplyRatingWritesTheConsumptionsFloppyReads(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		rows        []*fakeRatingRow
		wantPatches []string
	}{
		{
			name: "rewatch",
			rows: []*fakeRatingRow{
				{id: 1, score: floatPointer(6), status: 3, created: "2024-01-01T00:00:00Z", endDate: "2024-01-01T20:00:00Z"},
				{id: 2, status: 3, created: "2026-01-01T00:00:00Z", endDate: "2026-01-01T20:00:00Z"},
			},
			wantPatches: []string{`PATCH /api/v1/media/movie/tmdb/603/history/2/ {"score":9}`},
		},
		{
			// The newest consumption records an earlier watch, so the older one
			// keeps showing its score until it takes the rating too.
			name: "backdated newest consumption",
			rows: []*fakeRatingRow{
				{id: 1, score: floatPointer(6), status: 3, created: "2024-01-01T00:00:00Z", endDate: "2025-06-01T20:00:00Z"},
				{id: 2, status: 3, created: "2026-01-01T00:00:00Z", endDate: "2019-03-01T20:00:00Z"},
			},
			wantPatches: []string{
				`PATCH /api/v1/media/movie/tmdb/603/history/2/ {"score":9}`,
				`PATCH /api/v1/media/movie/tmdb/603/history/1/ {"score":9}`,
			},
		},
		{
			name: "newest consumption shows another score",
			rows: []*fakeRatingRow{
				{id: 1, score: floatPointer(9), status: 3, created: "2024-01-01T00:00:00Z", endDate: "2025-06-01T20:00:00Z"},
				{id: 2, score: floatPointer(4), status: 3, created: "2026-01-01T00:00:00Z", endDate: "2019-03-01T20:00:00Z"},
			},
			wantPatches: []string{`PATCH /api/v1/media/movie/tmdb/603/history/2/ {"score":9}`},
		},
		{
			// 8.6 already reads as 9, so Floppy keeps its finer score.
			name: "already shown",
			rows: []*fakeRatingRow{
				{id: 1, score: floatPointer(8.6), status: 3, created: "2024-01-01T00:00:00Z", endDate: "2025-06-01T20:00:00Z"},
				{id: 2, status: 3, created: "2026-01-01T00:00:00Z", endDate: "2026-01-01T20:00:00Z"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			floppy := newFakeRatingFloppy(t, map[string][]*fakeRatingRow{"movie/603": test.rows})
			response := floppy.apply(t, &pluginv1.WatchSyncEvent{
				EventId: "set-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 9,
				Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: map[string]string{"tmdb": "603"}},
			})
			wantStatus := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
			if len(test.wantPatches) == 0 {
				wantStatus = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
			}
			if response.GetFault() != nil || len(response.GetResults()) != 1 || response.GetResults()[0].GetStatus() != wantStatus {
				t.Fatalf("response = %#v", response)
			}
			if got := floppy.patches(); !slices.Equal(got, test.wantPatches) {
				t.Fatalf("patches = %v, want %v", got, test.wantPatches)
			}
			// Floppy's listing and its rated filter now both read 9.
			rows := floppy.rows("movie/603")
			if listed := newestByCreation(rows); listed.score != nil && ratingFromScore(*listed.score) != 9 {
				t.Fatalf("listed score = %v", *listed.score)
			}
			if shown := latestScored(rows); shown == nil || ratingFromScore(*shown.score) != 9 {
				t.Fatalf("shown consumption = %#v", shown)
			}
		})
	}
}

func TestApplyRatingRemovalClearsEveryScoredConsumption(t *testing.T) {
	t.Parallel()
	floppy := newFakeRatingFloppy(t, map[string][]*fakeRatingRow{
		"movie/603": {
			{id: 1, score: floatPointer(7), status: 3, created: "2024-01-01T00:00:00Z", endDate: "2024-01-01T20:00:00Z"},
			{id: 2, score: floatPointer(5), status: 0, created: "2025-01-01T00:00:00Z"},
			{id: 3, status: 3, created: "2025-06-01T00:00:00Z", endDate: "2025-06-01T20:00:00Z"},
			{id: 4, score: floatPointer(8.5), status: 1, created: "2026-01-01T00:00:00Z"},
		},
	})
	event := &pluginv1.WatchSyncEvent{
		EventId: "remove-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING,
		Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: map[string]string{"tmdb": "603"}},
	}
	for attempt, wantStatus := range []pluginv1.WatchSyncApplyStatus{
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
	} {
		response := floppy.apply(t, event)
		if response.GetFault() != nil || len(response.GetResults()) != 1 || response.GetResults()[0].GetStatus() != wantStatus {
			t.Fatalf("attempt %d response = %#v", attempt, response)
		}
	}
	// The Planning consumption goes first; the unscored one is left alone.
	want := []string{
		`PATCH /api/v1/media/movie/tmdb/603/history/2/ {"score":null}`,
		`PATCH /api/v1/media/movie/tmdb/603/history/1/ {"score":null}`,
		`PATCH /api/v1/media/movie/tmdb/603/history/4/ {"score":null}`,
	}
	if got := floppy.patches(); !slices.Equal(got, want) {
		t.Fatalf("patches = %v, want %v", got, want)
	}
	if shown := latestScored(floppy.rows("movie/603")); shown != nil {
		t.Fatalf("Floppy still shows a score: %#v", shown)
	}
}

func TestApplyRatingForUntrackedTitle(t *testing.T) {
	t.Parallel()
	floppy := newFakeRatingFloppy(t, nil)
	media := &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES, ExternalIds: map[string]string{"tmdb": "1668"}}
	response := floppy.apply(t,
		&pluginv1.WatchSyncEvent{EventId: "set-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 7, Media: media},
		&pluginv1.WatchSyncEvent{EventId: "remove-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING, Media: media},
	)
	if response.GetFault() != nil || len(response.GetResults()) != 2 {
		t.Fatalf("response = %#v", response)
	}
	set, remove := response.GetResults()[0], response.GetResults()[1]
	if set.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED ||
		set.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST ||
		set.GetFault().GetSafeMessage() != "Floppy is not tracking this title" {
		t.Fatalf("set result = %#v", set)
	}
	// Removing a rating that is already absent must not fault.
	if remove.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE || remove.GetFault() != nil {
		t.Fatalf("remove result = %#v", remove)
	}
	if patches := floppy.patches(); len(patches) != 0 {
		t.Fatalf("patches = %v", patches)
	}
}

func TestApplyRatingForPerPlayMovieWritesThroughTheTitleRoute(t *testing.T) {
	t.Parallel()
	floppy := newFakeRatingFloppy(t, map[string][]*fakeRatingRow{
		"movie/603": {{id: 21, status: 3, created: "2026-01-01T00:00:00Z", endDate: "2026-01-01T20:00:00Z"}},
	})
	floppy.perPlay["movie/603"] = true
	media := &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: map[string]string{"tmdb": "603"}}
	response := floppy.apply(t,
		&pluginv1.WatchSyncEvent{EventId: "set-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 8, Media: media},
		&pluginv1.WatchSyncEvent{EventId: "remove-1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING, Media: media},
	)
	if response.GetFault() != nil || len(response.GetResults()) != 2 {
		t.Fatalf("response = %#v", response)
	}
	for _, result := range response.GetResults() {
		if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
			t.Fatalf("result = %#v", result)
		}
	}
	want := []string{
		`PATCH /api/v1/media/movie/tmdb/603/ {"score":8}`,
		`PATCH /api/v1/media/movie/tmdb/603/ {"score":null}`,
	}
	if got := floppy.patches(); !slices.Equal(got, want) {
		t.Fatalf("patches = %v, want %v", got, want)
	}
}

func TestApplyRatingForbiddenIsPerEventOnlyForAValidToken(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		patchStatus    int
		validateStatus int
		wantPerEvent   bool
		wantValidates  int
	}{
		{name: "token lacks watchlist:write", patchStatus: http.StatusForbidden, wantPerEvent: true, wantValidates: 1},
		// Floppy's Bearer routes also answer 403 for a token that no longer
		// authenticates.
		{name: "token revoked", patchStatus: http.StatusForbidden, validateStatus: http.StatusUnauthorized, wantValidates: 1},
		{name: "unauthorized", patchStatus: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := func() []*fakeRatingRow {
				return []*fakeRatingRow{{id: 1, status: 3, created: "2026-01-01T00:00:00Z"}}
			}
			floppy := newFakeRatingFloppy(t, map[string][]*fakeRatingRow{"movie/603": rows(), "movie/604": rows()})
			floppy.patchStatus = test.patchStatus
			floppy.validateStatus = test.validateStatus
			var events []*pluginv1.WatchSyncEvent
			for _, id := range []string{"603", "604"} {
				events = append(events, &pluginv1.WatchSyncEvent{
					EventId: "set-" + id, Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 7,
					Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: map[string]string{"tmdb": id}},
				})
			}
			response := floppy.apply(t, events...)
			if got := floppy.count("GET /apis/listenbrainz/1/validate-token"); got != test.wantValidates {
				t.Fatalf("validate-token calls = %d, want %d", got, test.wantValidates)
			}
			if !test.wantPerEvent {
				if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || len(response.GetResults()) != 0 {
					t.Fatalf("response = %#v, want a connection-wide credential fault", response)
				}
				return
			}
			if response.GetFault() != nil || len(response.GetResults()) != len(events) {
				t.Fatalf("response = %#v", response)
			}
			for index, result := range response.GetResults() {
				if result.GetEventId() != events[index].GetEventId() ||
					result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED ||
					result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED ||
					result.GetFault().GetSafeMessage() != "The Floppy API token needs the watchlist:write scope to send ratings" {
					t.Fatalf("result %d = %#v", index, result)
				}
			}
		})
	}
}

func TestApplyRatingRejectsEventsFloppyCannotAddress(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}))
	defer upstream.Close()

	movie := pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE
	events := []*pluginv1.WatchSyncEvent{
		{
			EventId: "no-tmdb", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 6,
			Media: &pluginv1.WatchSyncMedia{MediaType: movie, ExternalIds: map[string]string{"imdb": "tt0133093"}},
		},
		{
			// A key for a series never addresses a movie.
			EventId: "wrong-key", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING,
			ProviderItemKey: "rating:tv:tmdb:1668", Media: &pluginv1.WatchSyncMedia{MediaType: movie},
		},
		{
			EventId: "bad-id", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 6,
			Media: &pluginv1.WatchSyncMedia{MediaType: movie, ExternalIds: map[string]string{"tmdb": "../603"}},
		},
		{
			EventId: "zero-rating", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
			Media: &pluginv1.WatchSyncMedia{MediaType: movie, ExternalIds: map[string]string{"tmdb": "603"}},
		},
		{
			EventId: "episode", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 6,
			Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE, ExternalIds: map[string]string{"tmdb": "62085"}},
		},
	}
	response, err := NewServer(upstream.Client()).ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authenticatedContext(upstream.URL), Events: events,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil || len(response.GetResults()) != len(events) {
		t.Fatalf("response = %#v", response)
	}
	for index, result := range response.GetResults() {
		if result.GetEventId() != events[index].GetEventId() ||
			result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED ||
			result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("result %d = %#v", index, result)
		}
	}
}

func TestApplyEventsDefersEventsPastTheTimeBox(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		deadline    time.Duration
		writeTakes  time.Duration
		wantApplied int
	}{
		// Events start at 0s and 60s; at 120s the 90-second budget is spent.
		{name: "budget", writeTakes: time.Minute, wantApplied: 2},
		// A 30-second deadline leaves 25 seconds: events start at 0s and 20s.
		{name: "call deadline", deadline: 30 * time.Second, writeTakes: 20 * time.Second, wantApplied: 2},
		{name: "call deadline already too close", deadline: 3 * time.Second, writeTakes: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			now := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
			titles := map[string][]*fakeRatingRow{}
			var events []*pluginv1.WatchSyncEvent
			for _, id := range []string{"603", "604", "605", "606"} {
				titles["movie/"+id] = []*fakeRatingRow{{id: 1, status: 3, created: "2026-01-01T00:00:00Z"}}
				events = append(events, &pluginv1.WatchSyncEvent{
					EventId: "set-" + id, Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 7,
					Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: map[string]string{"tmdb": id}},
				})
			}
			floppy := newFakeRatingFloppy(t, titles)
			// Each write stands in for a slow Floppy request.
			floppy.onPatch = func() {
				mu.Lock()
				now = now.Add(test.writeTakes)
				mu.Unlock()
			}
			floppy.server.now = func() time.Time {
				mu.Lock()
				defer mu.Unlock()
				return now
			}
			ctx := context.Background()
			if test.deadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, test.deadline)
				defer cancel()
			}
			response, err := floppy.server.ApplyEvents(ctx, &pluginv1.WatchSyncApplyEventsRequest{
				Context: authenticatedContext(floppy.url), Events: events,
			})
			if err != nil {
				t.Fatal(err)
			}
			if response.GetFault() != nil || len(response.GetResults()) != len(events) {
				t.Fatalf("response = %#v", response)
			}
			for index, result := range response.GetResults() {
				wantStatus := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
				if index >= test.wantApplied {
					wantStatus = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
					if result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
						t.Fatalf("result %d fault = %#v", index, result.GetFault())
					}
				}
				if result.GetEventId() != events[index].GetEventId() || result.GetStatus() != wantStatus {
					t.Fatalf("result %d = %#v", index, result)
				}
			}
			if got := len(floppy.patches()); got != test.wantApplied {
				t.Fatalf("patches = %d, want %d", got, test.wantApplied)
			}
		})
	}
}

func TestApplyEventsTimeBox(t *testing.T) {
	t.Parallel()
	if got := applyEventsTimeBox(context.Background()); got != applyEventsBudget {
		t.Fatalf("time box without a deadline = %s, want %s", got, applyEventsBudget)
	}
	far, cancelFar := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancelFar()
	if got := applyEventsTimeBox(far); got != applyEventsBudget {
		t.Fatalf("time box with a distant deadline = %s, want %s", got, applyEventsBudget)
	}
	near, cancelNear := context.WithTimeout(context.Background(), time.Minute)
	defer cancelNear()
	if got := applyEventsTimeBox(near); got > time.Minute-applyEventsDeadlineMargin || got < time.Minute-applyEventsDeadlineMargin-5*time.Second {
		t.Fatalf("time box with a one-minute deadline = %s", got)
	}
}

// fakeRatingRow is one consumption in fakeRatingFloppy.
type fakeRatingRow struct {
	id      int64
	score   *float64
	status  int
	created string
	endDate string
}

// fakeRatingFloppy serves Floppy's rating routes over a mutable set of titles,
// keyed "movie/603", and records every request.
type fakeRatingFloppy struct {
	t      *testing.T
	server *Server
	url    string

	mu             sync.Mutex
	titles         map[string][]*fakeRatingRow
	perPlay        map[string]bool
	patchStatus    int
	validateStatus int
	onPatch        func()
	requests       []string
}

func newFakeRatingFloppy(t *testing.T, titles map[string][]*fakeRatingRow) *fakeRatingFloppy {
	t.Helper()
	if titles == nil {
		titles = map[string][]*fakeRatingRow{}
	}
	floppy := &fakeRatingFloppy{t: t, titles: titles, perPlay: map[string]bool{}}
	upstream := httptest.NewServer(floppy)
	t.Cleanup(upstream.Close)
	floppy.server = NewServer(upstream.Client())
	floppy.url = upstream.URL
	return floppy
}

func (f *fakeRatingFloppy) apply(t *testing.T, events ...*pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyEventsResponse {
	t.Helper()
	response, err := f.server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authenticatedContext(f.url), Events: events,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func (f *fakeRatingFloppy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, strings.TrimSpace(r.Method+" "+r.URL.Path+" "+string(body)))
	patchStatus, validateStatus, onPatch := f.patchStatus, f.validateStatus, f.onPatch
	f.mu.Unlock()

	if r.URL.Path == "/apis/listenbrainz/1/validate-token" {
		if validateStatus != 0 {
			w.WriteHeader(validateStatus)
			return
		}
		writeJSON(f.t, w, map[string]any{"valid": true, "user_name": "alice"})
		return
	}
	if r.Header.Get("Authorization") != "Bearer token" {
		f.t.Errorf("authorization = %q", r.Header.Get("Authorization"))
	}
	if r.Method == http.MethodPatch {
		if onPatch != nil {
			onPatch()
		}
		if patchStatus != 0 {
			w.WriteHeader(patchStatus)
			return
		}
	}
	// api/v1/media/{type}/tmdb/{id}/[history/[{consumption_id}/]]
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 6 || parts[2] != "media" || parts[4] != "tmdb" {
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected request", http.StatusBadRequest)
		return
	}
	title := parts[3] + "/" + parts[5]
	f.mu.Lock()
	defer f.mu.Unlock()
	rows, tracked := f.titles[title]
	if !tracked {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(f.t, w, map[string]any{"detail": "Media not found or not tracked."})
		return
	}
	switch {
	case r.Method == http.MethodGet && len(parts) == 7 && parts[6] == "history":
		if query := r.URL.Query(); query.Get("limit") != "100" || query.Get("offset") != "0" {
			f.t.Errorf("history query = %s", r.URL.RawQuery)
		}
		var results []any
		for _, row := range rows {
			entry := row.json()
			if f.perPlay[title] {
				entry["score"], entry["status"], entry["external_id"] = nil, nil, nil
			}
			results = append(results, entry)
		}
		writeJSON(f.t, w, historyPage(results...))
	case r.Method == http.MethodPatch && len(parts) == 8 && parts[6] == "history":
		id, _ := strconv.ParseInt(parts[7], 10, 64)
		index := slices.IndexFunc(rows, func(row *fakeRatingRow) bool { return row.id == id })
		if index < 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var payload struct {
			Score *float64 `json:"score"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			f.t.Errorf("PATCH body %q: %v", body, err)
		}
		rows[index].score = payload.Score
		writeJSON(f.t, w, rows[index].json())
	case r.Method == http.MethodPatch && len(parts) == 6:
		// The title route updates the newest consumption.
		newest := rows[0]
		for _, row := range rows {
			if row.created > newest.created {
				newest = row
			}
		}
		var payload struct {
			Score *float64 `json:"score"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			f.t.Errorf("PATCH body %q: %v", body, err)
		}
		newest.score = payload.Score
		writeJSON(f.t, w, map[string]any{"tracked": true})
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

func (row *fakeRatingRow) json() map[string]any {
	var score, end any
	if row.score != nil {
		score = *row.score
	}
	if row.endDate != "" {
		end = row.endDate
	}
	return map[string]any{
		"consumption_id": row.id, "created": row.created, "score": score, "progress": 1,
		"progressed_at": row.created, "status": row.status, "start_date": nil, "end_date": end,
		"notes": "", "source": "",
	}
}

// rows parses the title's consumptions the way the plugin reads them.
func (f *fakeRatingFloppy) rows(title string) []titleConsumption {
	f.mu.Lock()
	defer f.mu.Unlock()
	var parsed []titleConsumption
	for _, row := range f.titles[title] {
		encoded, _ := json.Marshal(row.json())
		var entry consumptionHistory
		if err := json.Unmarshal(encoded, &entry); err != nil {
			f.t.Fatal(err)
		}
		consumption, ok := titleConsumptionFrom(entry)
		if !ok {
			f.t.Fatalf("unreadable consumption %#v", entry)
		}
		parsed = append(parsed, consumption)
	}
	return parsed
}

func (f *fakeRatingFloppy) patches() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var patches []string
	for _, request := range f.requests {
		if strings.HasPrefix(request, http.MethodPatch+" ") {
			patches = append(patches, request)
		}
	}
	return patches
}

func (f *fakeRatingFloppy) count(request string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, got := range f.requests {
		if got == request {
			count++
		}
	}
	return count
}

func (f *fakeRatingFloppy) wantRequests(t *testing.T, want ...string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if !slices.Equal(f.requests, want) {
		t.Fatalf("requests = %q, want %q", f.requests, want)
	}
}

func floatPointer(value float64) *float64 { return &value }

func ratedPage(limit, total, offset int, next string, results ...any) map[string]any {
	var nextValue any
	if next != "" {
		nextValue = next
	}
	if results == nil {
		results = []any{}
	}
	return map[string]any{
		"pagination": map[string]any{"total": total, "limit": limit, "offset": offset, "next": nextValue, "previous": nil},
		"results":    results,
	}
}

func ratedEntry(id int, score any, item map[string]any) map[string]any {
	return map[string]any{"id": id, "score": score, "status": 3, "tracked": true, "item": item}
}

func ratedItem(mediaType, mediaID, title, released string, ids map[string]any) map[string]any {
	if ids == nil {
		ids = map[string]any{}
	}
	var release any
	if released != "" {
		release = released
	}
	return map[string]any{
		"media_id": mediaID, "source": "tmdb", "media_type": mediaType, "library_media_type": "",
		"title": title, "release_datetime": release, "ids": ids,
		"provider_external_ids": map[string]any{"tmdb_id": mediaID},
		"watch_providers":       map[string]any{"US": map[string]any{"flatrate": []any{}}},
	}
}

func historyPage(results ...any) map[string]any {
	return ratedPage(100, len(results), 0, "", results...)
}

// historyRow is one consumption as Floppy's history route serializes it.
func historyRow(id int64, score any, status int, created, endDate string) map[string]any {
	var end any
	if endDate != "" {
		end = endDate
	}
	return map[string]any{
		"consumption_id": id, "created": created, "score": score, "progress": 1,
		"progressed_at": created, "status": status, "start_date": nil, "end_date": end,
		"notes": "", "source": "",
	}
}

func TestListRatingsForbiddenIsAScopeFaultOnlyForAValidToken(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		validateStatus int
		wantCode       pluginv1.WatchSyncFaultCode
	}{
		{name: "token lacks watchlist:read", wantCode: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED},
		{name: "token revoked", validateStatus: http.StatusUnauthorized, wantCode: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			validates := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/apis/listenbrainz/1/validate-token" {
					validates++
					if test.validateStatus != 0 {
						w.WriteHeader(test.validateStatus)
						return
					}
					writeJSON(t, w, map[string]any{"valid": true, "user_name": "viewer"})
					return
				}
				w.WriteHeader(http.StatusForbidden)
			}))
			defer upstream.Close()

			response, err := NewServer(upstream.Client()).ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
				Context: authenticatedContext(upstream.URL), StateKinds: ratingKinds,
			})
			if err != nil {
				t.Fatal(err)
			}
			if response.GetFault().GetCode() != test.wantCode || validates != 1 || len(response.GetItems()) != 0 {
				t.Fatalf("fault = %#v, validate calls = %d", response.GetFault(), validates)
			}
			if test.wantCode == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED &&
				!strings.Contains(response.GetFault().GetSafeMessage(), "watchlist:read") {
				t.Fatalf("message = %q, want the missing scope named", response.GetFault().GetSafeMessage())
			}
		})
	}
}
