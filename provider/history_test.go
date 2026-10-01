package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeFloppyHistory serves GET /api/v1/history/ the way Floppy v26.8.27 does:
// day groups that list at most 30 entries each or, with flat=true, every entry.
// Floppy clamps limit to 200 and reports the limit it used. A Floppy release
// before v26.8.20 ignores flat, which ignoreFlat reproduces.
type fakeFloppyHistory struct {
	t          *testing.T
	ignoreFlat bool

	mu sync.Mutex
	// entries are newest first, as Floppy lists them.
	entries   []map[string]any
	scrobbles int
}

func (f *fakeFloppyHistory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && r.URL.Path == "/api/v1/scrobble/" {
		f.mu.Lock()
		f.scrobbles++
		f.mu.Unlock()
		writeJSON(f.t, w, map[string]any{"detail": "accepted"})
		return
	}
	if r.Method != http.MethodGet || r.URL.Path != "/api/v1/history/" {
		http.NotFound(w, r)
		return
	}
	query := r.URL.Query()
	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil || limit <= 0 {
		limit = 20
	}
	limit = min(limit, 200)
	offset, _ := strconv.Atoi(query.Get("offset"))

	f.mu.Lock()
	entries := append([]map[string]any(nil), f.entries...)
	f.mu.Unlock()
	var results []any
	if query.Get("flat") == "true" && !f.ignoreFlat {
		for _, entry := range entries {
			results = append(results, entry)
		}
	} else {
		results = floppyHistoryDays(entries)
	}
	total := len(results)
	page := results[min(offset, total):min(offset+limit, total)]
	var next any
	if offset+limit < total {
		query.Set("limit", strconv.Itoa(limit))
		query.Set("offset", strconv.Itoa(offset+limit))
		next = "http://" + r.Host + r.URL.Path + "?" + query.Encode()
	}
	writeJSON(f.t, w, map[string]any{
		"pagination": map[string]any{"total": total, "limit": limit, "offset": offset, "next": next},
		"results":    append([]any{}, page...),
	})
}

func (f *fakeFloppyHistory) remove(index int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries[:index:index], f.entries[index+1:]...)
}

func (f *fakeFloppyHistory) scrobbleCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scrobbles
}

// floppyHistoryDays groups entries by day the way Floppy's grouped history
// does, listing at most 30 entries per day.
func floppyHistoryDays(entries []map[string]any) []any {
	date := func(entry map[string]any) string {
		return entry["played_at_local"].(string)[:len(time.DateOnly)]
	}
	var days []any
	for start := 0; start < len(entries); {
		end := start + 1
		for end < len(entries) && date(entries[end]) == date(entries[start]) {
			end++
		}
		dayEntries := entries[start:end]
		listed := dayEntries[:min(len(dayEntries), 30)]
		days = append(days, map[string]any{
			"date": date(entries[start]), "entries": listed,
			"entry_count": len(dayEntries), "entries_truncated": len(listed) < len(dayEntries),
		})
		start = end
	}
	return days
}

// floppyEpisodeEntry is an episode entry as Floppy v26.8.27 writes it: the
// item's media_id is the show's TMDB ID, and there is no status and no
// provider_external_ids.
func floppyEpisodeEntry(instanceID int, showTMDBID string, season, episode int, playedAt time.Time) map[string]any {
	return map[string]any{
		"media_type": "episode",
		"item": map[string]any{
			"id": 9000 + instanceID, "media_type": "episode", "media_id": showTMDBID, "source": "tmdb",
			"title": "Episode " + strconv.Itoa(episode), "season_number": season, "episode_number": episode,
		},
		"show":  map[string]any{"id": 44243, "title": "Show " + showTMDBID},
		"title": "Episode " + strconv.Itoa(episode), "display_title": "Episode " + strconv.Itoa(episode),
		"season_number": season, "episode_number": episode,
		"played_at_local": playedAt.Format(time.RFC3339Nano),
		"instance_id":     instanceID, "entry_key": strconv.Itoa(instanceID),
	}
}

func floppyMovieEntry(instanceID int, tmdbID string, playedAt time.Time) map[string]any {
	return map[string]any{
		"media_type": "movie",
		"item": map[string]any{
			"id": 9000 + instanceID, "media_type": "movie", "media_id": tmdbID, "source": "tmdb", "title": "Movie " + tmdbID,
		},
		"title": "Movie " + tmdbID, "display_title": "Movie " + tmdbID,
		"status": "Completed", "play_count": 1,
		"played_at_local": playedAt.Format(time.RFC3339Nano),
		"instance_id":     instanceID, "entry_key": strconv.Itoa(instanceID),
	}
}

// busyDayHistory is one day holding 40 episodes of one show and 5 movies,
// newest first, like a day that a Trakt import or a binge lands on.
func busyDayHistory(day time.Time) []map[string]any {
	var entries []map[string]any
	for index := range 45 {
		playedAt := day.Add(-time.Duration(index) * time.Minute)
		if index%9 == 0 {
			entries = append(entries, floppyMovieEntry(index+1, strconv.Itoa(600+index), playedAt))
			continue
		}
		entries = append(entries, floppyEpisodeEntry(index+1, "1429", 1, 45-index, playedAt))
	}
	return entries
}

// listAllWatched walks one watched traversal from cursor and returns its
// items, whether it was a complete snapshot, and the cursor it ended on.
func listAllWatched(t *testing.T, server *Server, baseURL, cursor string) ([]*pluginv1.WatchSyncRemoteState, bool, string) {
	t.Helper()
	var items []*pluginv1.WatchSyncRemoteState
	pageToken := ""
	for range 100 {
		response, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authenticatedContext(baseURL), Cursor: cursor, PageSize: 20, PageToken: pageToken,
			StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED},
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.GetFault() != nil {
			t.Fatalf("fault = %v", response.GetFault())
		}
		items = append(items, response.GetItems()...)
		if pageToken = response.GetNextPageToken(); pageToken == "" {
			return items, response.GetCompleteSnapshot(), response.GetNextCursor()
		}
	}
	t.Fatal("history traversal did not finish")
	return nil, false, ""
}

func TestListWatchedImportsEveryEntryOfABusyDay(t *testing.T) {
	t.Parallel()
	day := time.Date(2020, time.October, 10, 23, 0, 0, 0, time.UTC)
	floppy := &fakeFloppyHistory{t: t, entries: busyDayHistory(day)}
	upstream := httptest.NewServer(floppy)
	defer upstream.Close()

	items, _, _ := listAllWatched(t, NewServer(upstream.Client()), upstream.URL, "")
	movies, episodes := 0, map[string]bool{}
	for _, item := range items {
		media := item.GetMedia()
		switch media.GetMediaType() {
		case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
			movies++
		case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
			if media.GetSeriesExternalIds()["tmdb"] != "1429" || len(media.GetExternalIds()) != 0 || media.GetSeasonNumber() != 1 {
				t.Fatalf("episode media = %#v", media)
			}
			episodes[media.GetMediaItemId()] = true
		}
	}
	if movies != 5 || len(episodes) != 40 || !episodes["tmdb:1429:1:44"] {
		t.Fatalf("imported %d movies and %d distinct episodes from %d items, want 5 and 40", movies, len(episodes), len(items))
	}
}

func TestListWatchedReadsDayGroupsFromFloppyWithoutFlatHistory(t *testing.T) {
	t.Parallel()
	day := time.Date(2026, time.August, 5, 20, 0, 0, 0, time.UTC)
	floppy := &fakeFloppyHistory{t: t, ignoreFlat: true, entries: []map[string]any{
		floppyEpisodeEntry(3, "1429", 1, 2, day),
		floppyMovieEntry(2, "603", day.Add(-time.Hour)),
		floppyEpisodeEntry(1, "1429", 1, 1, day.Add(-24*time.Hour)),
	}}
	upstream := httptest.NewServer(floppy)
	defer upstream.Close()

	items, _, _ := listAllWatched(t, NewServer(upstream.Client()), upstream.URL, "")
	var keys []string
	for _, item := range items {
		keys = append(keys, item.GetMedia().GetMediaItemId())
	}
	if len(keys) != 3 || keys[0] != "tmdb:1429:1:2" || keys[1] != "tmdb:603" || keys[2] != "tmdb:1429:1:1" {
		t.Fatalf("media item IDs = %v", keys)
	}
}

func TestApplyWatchedEpisodeFindsEarlierPlayOnABusyDay(t *testing.T) {
	t.Parallel()
	day := time.Date(2020, time.October, 10, 23, 0, 0, 0, time.UTC)
	entries := busyDayHistory(day)
	// The 45th entry of the day is beyond what a day group lists.
	earlier := entries[len(entries)-1]
	occurredAt, err := time.Parse(time.RFC3339Nano, earlier["played_at_local"].(string))
	if err != nil {
		t.Fatal(err)
	}
	floppy := &fakeFloppyHistory{t: t, entries: entries}
	upstream := httptest.NewServer(floppy)
	defer upstream.Close()

	response, err := NewServer(upstream.Client()).ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authenticatedContext(upstream.URL),
		Events: []*pluginv1.WatchSyncEvent{{
			EventId:    "history-1",
			Operation:  pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
			OccurredAt: timestamppb.New(occurredAt),
			Media: &pluginv1.WatchSyncMedia{
				MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
				SeriesExternalIds: map[string]string{"tmdb": "1429", "tvdb": "267440"},
				SeasonNumber:      1,
				EpisodeNumber:     1,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil || len(response.GetResults()) != 1 ||
		response.GetResults()[0].GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("response = %#v", response)
	}
	if floppy.scrobbleCount() != 0 {
		t.Fatalf("scrobbles = %d, want 0", floppy.scrobbleCount())
	}
}

func TestListWatchedImportsEverythingAgainAfterACursorFromAnEarlierRelease(t *testing.T) {
	t.Parallel()
	day := time.Date(2020, time.October, 10, 23, 0, 0, 0, time.UTC)
	floppy := &fakeFloppyHistory{t: t, entries: busyDayHistory(day)}
	upstream := httptest.NewServer(floppy)
	defer upstream.Close()
	server := NewServer(upstream.Client())
	wantCursor := watchedCursorPrefix + day.Add(-providerCursorOverlap).Format(time.RFC3339Nano)

	// Releases up to 0.3.0 saved a bare timestamp past the episodes they skipped.
	items, complete, cursor := listAllWatched(t, server, upstream.URL, day.Format(time.RFC3339Nano))
	if len(items) != 45 || !complete || cursor != wantCursor {
		t.Fatalf("after a legacy cursor: %d items, complete snapshot %t, cursor %q", len(items), complete, cursor)
	}
	// Only the newest play, inside the cursor's overlap, comes back.
	items, complete, _ = listAllWatched(t, server, upstream.URL, cursor)
	if len(items) != 1 || complete {
		t.Fatalf("after a current cursor: %d items, complete snapshot %t", len(items), complete)
	}
}

func TestListWatchedStartsOverWhenAReadEntryIsDeleted(t *testing.T) {
	t.Parallel()
	day := time.Date(2020, time.October, 10, 23, 0, 0, 0, time.UTC)
	floppy := &fakeFloppyHistory{t: t, entries: busyDayHistory(day)}
	upstream := httptest.NewServer(floppy)
	defer upstream.Close()
	server := NewServer(upstream.Client())
	page := func(pageToken string) *pluginv1.WatchSyncListRemoteStateResponse {
		t.Helper()
		response, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authenticatedContext(upstream.URL), PageSize: 20, PageToken: pageToken,
			StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED},
		})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}

	first := page("")
	if first.GetFault() != nil || len(first.GetItems()) != 20 || first.GetNextPageToken() == "" {
		t.Fatalf("first page = %#v", first)
	}
	// Deleting a play the first page read moves every later entry up a place.
	floppy.remove(0)
	second := page(first.GetNextPageToken())
	if second.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || len(second.GetItems()) != 0 {
		t.Fatalf("second page = %#v", second)
	}
	// The next sync starts over and reads every remaining play.
	items, _, _ := listAllWatched(t, server, upstream.URL, "")
	if len(items) != 44 {
		t.Fatalf("items after starting over = %d, want 44", len(items))
	}
}
