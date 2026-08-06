package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const providerCursorOverlap = 2 * time.Second

func (s *Server) listWatched(ctx context.Context, client *apiClient, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	cursor, fault := parseCursor(req.GetCursor())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	token, fault := traversal(req)
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	limit := pageSize(req.GetPageSize())
	query := url.Values{
		"limit":         {strconv.Itoa(limit)},
		"offset":        {strconv.Itoa(token.Offset)},
		"logging_style": {"sessions"},
	}
	if !cursor.IsZero() {
		// Floppy filters dates in its server timezone while Silo cursors are UTC.
		// Widen by a day and apply the exact timestamp bounds locally.
		query.Set("start_date", cursor.Add(-24*time.Hour).Format(time.DateOnly))
	}
	if !token.HighWater.IsZero() {
		query.Set("end_date", token.HighWater.Add(24*time.Hour).Format(time.DateOnly))
	}

	var upstream historyResponse
	if requestFault := client.do(ctx, http.MethodGet, "/api/v1/history/", query, nil, &upstream, "Bearer"); requestFault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: requestFault}, nil
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{
		CompleteSnapshot: cursor.IsZero(),
	}
	if token.HighWater.IsZero() {
		token.HighWater = historyWatermark(upstream.Results)
	}
	for _, day := range upstream.Results {
		for _, entry := range day.Entries {
			if !completedHistoryEntry(entry) {
				continue
			}
			watchedAt, err := time.Parse(time.RFC3339Nano, entry.PlayedAt)
			if err != nil || !watchedAt.After(cursor) || (!token.HighWater.IsZero() && watchedAt.After(token.HighWater)) {
				continue
			}
			state := watchedState(entry, watchedAt)
			if state != nil {
				response.Items = append(response.Items, state)
			}
		}
	}
	nextOffset, more, paginationFault := nextOffsetFromPagination(upstream.Pagination, token.Offset)
	if paginationFault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: paginationFault}, nil
	}
	if more {
		if token.HighWater.IsZero() {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault("Floppy returned history pages without a usable timestamp", 0)}, nil
		}
		token.Offset = nextOffset
		response.NextPageToken = nextPageToken(token)
	} else if nextCursor := providerNextCursor(cursor, token.HighWater); nextCursor != "" {
		response.NextCursor = nextCursor
	}
	return response, nil
}

func (s *Server) listProgress(ctx context.Context, client *apiClient, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	cursor, fault := parseCursor(req.GetCursor())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	token, fault := traversal(req)
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	entries, requestFault := fetchProgressSnapshot(ctx, client, cursor)
	if requestFault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: requestFault}, nil
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: cursor.IsZero()}
	candidates := make([]progressCandidate, 0, len(entries))
	for _, entry := range entries {
		updatedAt, err := time.Parse(time.RFC3339Nano, entry.UpdatedAt)
		if err != nil || !updatedAt.After(cursor) {
			continue
		}
		state := progressState(entry, updatedAt)
		if state != nil {
			candidates = append(candidates, progressCandidate{state: state, updatedAt: updatedAt, key: state.GetProviderItemKey()})
		}
	}
	if token.HighWater.IsZero() {
		for _, candidate := range candidates {
			if candidate.updatedAt.After(token.HighWater) {
				token.HighWater = candidate.updatedAt
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].updatedAt.Equal(candidates[j].updatedAt) {
			return candidates[i].key < candidates[j].key
		}
		return candidates[i].updatedAt.Before(candidates[j].updatedAt)
	})
	filtered := candidates[:0]
	for _, candidate := range candidates {
		if (!token.HighWater.IsZero() && candidate.updatedAt.After(token.HighWater)) || !afterProgressBoundary(candidate, token) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	limit := pageSize(req.GetPageSize())
	page := filtered
	if len(page) > limit {
		page = page[:limit]
	}
	for _, candidate := range page {
		response.Items = append(response.Items, candidate.state)
	}
	if len(filtered) > len(page) {
		last := page[len(page)-1]
		token.BoundaryAt = last.updatedAt
		token.BoundaryKey = last.key
		response.NextPageToken = nextPageToken(token)
	} else if nextCursor := providerNextCursor(cursor, token.HighWater); nextCursor != "" {
		response.NextCursor = nextCursor
	}
	return response, nil
}

type progressCandidate struct {
	state     *pluginv1.WatchSyncRemoteState
	updatedAt time.Time
	key       string
}

func afterProgressBoundary(candidate progressCandidate, token traversalToken) bool {
	if token.BoundaryAt.IsZero() {
		return true
	}
	return candidate.updatedAt.After(token.BoundaryAt) ||
		(candidate.updatedAt.Equal(token.BoundaryAt) && candidate.key > token.BoundaryKey)
}

func fetchProgressSnapshot(ctx context.Context, client *apiClient, cursor time.Time) ([]progressEntry, *pluginv1.WatchSyncFault) {
	query := url.Values{
		"limit":      {"200"},
		"offset":     {"0"},
		"media_type": {"movie,episode"},
		"completed":  {"false"},
	}
	if !cursor.IsZero() {
		query.Set("updated_since", cursor.Format(time.RFC3339Nano))
	}
	var entries []progressEntry
	for offset := 0; ; {
		query.Set("offset", strconv.Itoa(offset))
		var upstream progressResponse
		if fault := client.do(ctx, http.MethodGet, "/api/v1/playback/progress/", query, nil, &upstream, "Bearer"); fault != nil {
			return nil, fault
		}
		entries = append(entries, upstream.Results...)
		nextOffset, more, fault := nextOffsetFromPagination(upstream.Pagination, offset)
		if fault != nil {
			return nil, fault
		}
		if !more {
			return entries, nil
		}
		offset = nextOffset
	}
}

func historyWatermark(days []historyDay) time.Time {
	var watermark time.Time
	for _, day := range days {
		for _, entry := range day.Entries {
			if playedAt, err := time.Parse(time.RFC3339Nano, entry.PlayedAt); err == nil && playedAt.After(watermark) {
				watermark = playedAt
			}
		}
	}
	return watermark
}

func providerNextCursor(previous, highWater time.Time) string {
	if highWater.IsZero() {
		return ""
	}
	next := highWater.Add(-providerCursorOverlap)
	if !previous.IsZero() && next.Before(previous) {
		next = previous
	}
	return next.UTC().Format(time.RFC3339Nano)
}

func watchedState(entry historyEntry, watchedAt time.Time) *pluginv1.WatchSyncRemoteState {
	media := mediaFromHistory(entry)
	if media == nil {
		return nil
	}
	instanceID := rawString(entry.InstanceID)
	providerKey := "history:" + entry.MediaType + ":" + instanceID
	if instanceID == "" {
		providerKey = fmt.Sprintf("history:%s:%s:%s:%d:%d:%d", entry.MediaType, entry.Item.Source, rawString(entry.Item.MediaID), valueOrZero(entry.Item.SeasonNumber), valueOrZero(entry.Item.EpisodeNumber), watchedAt.UnixNano())
	}
	playCount := int32(max(1, entry.PlayCount))
	return &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: providerKey,
		Media:           media,
		Watched: &pluginv1.WatchSyncRemoteWatchedState{
			PlayCount:     playCount,
			LastWatchedAt: timestamp(entry.PlayedAt),
		},
	}
}

func progressState(entry progressEntry, updatedAt time.Time) *pluginv1.WatchSyncRemoteState {
	if entry.Completed || entry.Duration <= 0 || entry.Position < 0 {
		return nil
	}
	media := mediaFromProgress(entry)
	if media == nil {
		return nil
	}
	percent := min(99.999, entry.Position/entry.Duration*100)
	providerKey := fmt.Sprintf("progress:%s:%s:%s:%d:%d", entry.MediaType, entry.Source, rawString(entry.MediaID), valueOrZero(entry.SeasonNumber), valueOrZero(entry.EpisodeNumber))
	return &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: providerKey,
		Media:           media,
		Progress: &pluginv1.WatchSyncRemoteProgressState{
			ProgressPercent: percent,
			PausedAt:        timestamp(entry.UpdatedAt),
		},
	}
}

func mediaFromHistory(entry historyEntry) *pluginv1.WatchSyncMedia {
	ids := normalizedIDs(entry.Item.ProviderExternalID)
	addSourceID(ids, entry.Item.Source, rawString(entry.Item.MediaID))
	mediaType := protoMediaType(entry.MediaType)
	if mediaType == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_UNSPECIFIED || len(ids) == 0 {
		return nil
	}
	title := entry.DisplayTitle
	if title == "" {
		title = entry.Title
	}
	media := &pluginv1.WatchSyncMedia{
		MediaItemId: entry.Item.Source + ":" + rawString(entry.Item.MediaID),
		MediaType:   mediaType,
		Title:       title,
		ExternalIds: cloneMap(ids),
	}
	if mediaType == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE {
		media.MediaItemId = episodeMediaItemID(entry.Item.Source, rawString(entry.Item.MediaID), valueOrZero(entry.Item.SeasonNumber), valueOrZero(entry.Item.EpisodeNumber))
		media.ExternalIds = nil
		media.SeriesTitle = entry.Item.Title
		media.SeriesExternalIds = cloneMap(ids)
		media.SeasonNumber = valueOrZero(entry.Item.SeasonNumber)
		media.EpisodeNumber = valueOrZero(entry.Item.EpisodeNumber)
	}
	return media
}

func mediaFromProgress(entry progressEntry) *pluginv1.WatchSyncMedia {
	ids := normalizedIDs(entry.IDs)
	addSourceID(ids, entry.Source, rawString(entry.MediaID))
	mediaType := protoMediaType(entry.MediaType)
	if mediaType == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_UNSPECIFIED || len(ids) == 0 {
		return nil
	}
	media := &pluginv1.WatchSyncMedia{
		MediaItemId: entry.Source + ":" + rawString(entry.MediaID),
		MediaType:   mediaType,
		Title:       entry.Title,
		ExternalIds: cloneMap(ids),
	}
	if mediaType == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE {
		media.MediaItemId = episodeMediaItemID(entry.Source, rawString(entry.MediaID), valueOrZero(entry.SeasonNumber), valueOrZero(entry.EpisodeNumber))
		media.ExternalIds = nil
		media.SeriesTitle = entry.SeriesTitle
		media.SeriesExternalIds = cloneMap(ids)
		media.SeasonNumber = valueOrZero(entry.SeasonNumber)
		media.EpisodeNumber = valueOrZero(entry.EpisodeNumber)
	}
	return media
}

func episodeMediaItemID(source, seriesID string, season, episode int32) string {
	return fmt.Sprintf("%s:%s:%d:%d", source, seriesID, season, episode)
}

func addSourceID(ids map[string]string, source, mediaID string) {
	source = strings.ToLower(strings.TrimSpace(source))
	if (source == "tmdb" || source == "imdb" || source == "tvdb") && strings.TrimSpace(mediaID) != "" && ids[source] == "" {
		ids[source] = mediaID
	}
}

func protoMediaType(value string) pluginv1.WatchSyncMediaType {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "movie":
		return pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE
	case "episode":
		return pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE
	default:
		return pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_UNSPECIFIED
	}
}

func valueOrZero(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

func historyContainsEvent(ctx context.Context, client *apiClient, event *pluginv1.WatchSyncEvent) (bool, *pluginv1.WatchSyncFault) {
	if event.GetOccurredAt() == nil || event.GetOccurredAt().CheckValid() != nil {
		return false, invalidRequestFault("Completed watch events require a valid occurrence time")
	}
	occurredAt := event.GetOccurredAt().AsTime()
	query := url.Values{
		"start_date":    {occurredAt.Add(-24 * time.Hour).Format(time.DateOnly)},
		"end_date":      {occurredAt.Add(24 * time.Hour).Format(time.DateOnly)},
		"logging_style": {"sessions"},
		"limit":         {strconv.Itoa(defaultPageSize)},
	}
	for offset := 0; ; {
		query.Set("offset", strconv.Itoa(offset))
		var upstream historyResponse
		if fault := client.do(ctx, http.MethodGet, "/api/v1/history/", query, nil, &upstream, "Bearer"); fault != nil {
			return false, fault
		}
		for _, day := range upstream.Results {
			for _, entry := range day.Entries {
				if !completedHistoryEntry(entry) {
					continue
				}
				playedAt, err := time.Parse(time.RFC3339Nano, entry.PlayedAt)
				if err != nil || absoluteDuration(playedAt.Sub(occurredAt)) > 2*time.Second {
					continue
				}
				if mediaMatchesHistory(event.GetMedia(), entry) {
					return true, nil
				}
			}
		}
		nextOffset, more, paginationFault := nextOffsetFromPagination(upstream.Pagination, offset)
		if paginationFault != nil {
			return false, paginationFault
		}
		if !more {
			return false, nil
		}
		offset = nextOffset
	}
}

func nextOffsetFromPagination(page pagination, current int) (int, bool, *pluginv1.WatchSyncFault) {
	step := max(1, page.Limit)
	if page.Offset < 0 || page.Offset != current {
		return 0, false, temporaryFault("Floppy returned invalid pagination", 0)
	}
	if page.Next == "" && (page.Total <= 0 || page.Offset+step >= page.Total) {
		return 0, false, nil
	}
	next := page.Offset + step
	if page.Next != "" {
		parsed, err := url.Parse(page.Next)
		if err != nil {
			return 0, false, temporaryFault("Floppy returned invalid pagination", 0)
		}
		candidate, candidateErr := strconv.Atoi(parsed.Query().Get("offset"))
		if candidateErr != nil || candidate <= current {
			return 0, false, temporaryFault("Floppy returned invalid pagination", 0)
		}
		next = candidate
	}
	if next <= current {
		return 0, false, temporaryFault("Floppy returned invalid pagination", 0)
	}
	return next, true, nil
}

func completedHistoryEntry(entry historyEntry) bool {
	return strings.EqualFold(strings.TrimSpace(entry.Status), "completed")
}

func mediaMatchesHistory(media *pluginv1.WatchSyncMedia, entry historyEntry) bool {
	if media == nil || protoMediaType(entry.MediaType) != media.GetMediaType() {
		return false
	}
	if media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE &&
		(valueOrZero(entry.Item.SeasonNumber) != media.GetSeasonNumber() || valueOrZero(entry.Item.EpisodeNumber) != media.GetEpisodeNumber()) {
		return false
	}
	entryIDs := normalizedIDs(entry.Item.ProviderExternalID)
	addSourceID(entryIDs, entry.Item.Source, rawString(entry.Item.MediaID))
	for namespace, eventID := range mergedExternalIDs(media) {
		if entryIDs[namespace] != "" && entryIDs[namespace] == eventID {
			return true
		}
	}
	return false
}

func absoluteDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
