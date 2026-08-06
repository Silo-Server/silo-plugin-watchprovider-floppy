package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func (s *Server) listWatched(ctx context.Context, client *apiClient, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	cursor, fault := parseCursor(req.GetCursor())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	token, fault := traversal(req, s.now())
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
	query.Set("end_date", token.HighWater.Add(24*time.Hour).Format(time.DateOnly))

	var upstream historyResponse
	if requestFault := client.do(ctx, http.MethodGet, "/api/v1/history/", query, nil, &upstream, "Bearer"); requestFault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: requestFault}, nil
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{
		CompleteSnapshot: cursor.IsZero(),
	}
	for _, day := range upstream.Results {
		for _, entry := range day.Entries {
			if !completedHistoryEntry(entry) {
				continue
			}
			watchedAt, err := time.Parse(time.RFC3339Nano, entry.PlayedAt)
			if err != nil || !watchedAt.After(cursor) || watchedAt.After(token.HighWater) {
				continue
			}
			state := watchedState(entry, watchedAt)
			if state != nil {
				response.Items = append(response.Items, state)
			}
		}
	}
	if upstream.Pagination.Next != "" || token.Offset+limit < upstream.Pagination.Total {
		response.NextPageToken = nextPageToken(token.Offset+limit, token.HighWater)
	} else {
		response.NextCursor = token.HighWater.UTC().Format(time.RFC3339Nano)
	}
	return response, nil
}

func (s *Server) listProgress(ctx context.Context, client *apiClient, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	cursor, fault := parseCursor(req.GetCursor())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	token, fault := traversal(req, s.now())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	limit := pageSize(req.GetPageSize())
	query := url.Values{
		"limit":      {strconv.Itoa(limit)},
		"offset":     {strconv.Itoa(token.Offset)},
		"media_type": {"movie,episode"},
		"completed":  {"false"},
	}
	if !cursor.IsZero() {
		query.Set("updated_since", cursor.Format(time.RFC3339Nano))
	}

	var upstream progressResponse
	if requestFault := client.do(ctx, http.MethodGet, "/api/v1/playback/progress/", query, nil, &upstream, "Bearer"); requestFault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: requestFault}, nil
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: cursor.IsZero()}
	for _, entry := range upstream.Results {
		updatedAt, err := time.Parse(time.RFC3339Nano, entry.UpdatedAt)
		if err != nil || !updatedAt.After(cursor) || updatedAt.After(token.HighWater) {
			continue
		}
		state := progressState(entry, updatedAt)
		if state != nil {
			response.Items = append(response.Items, state)
		}
	}
	if upstream.Pagination.Next != "" || token.Offset+limit < upstream.Pagination.Total {
		response.NextPageToken = nextPageToken(token.Offset+limit, token.HighWater)
	} else {
		response.NextCursor = token.HighWater.UTC().Format(time.RFC3339Nano)
	}
	return response, nil
}

func watchedState(entry historyEntry, watchedAt time.Time) *pluginv1.WatchSyncRemoteState {
	media := mediaFromHistory(entry)
	if media == nil {
		return nil
	}
	instanceID := rawString(entry.InstanceID)
	providerKey := "history:" + entry.MediaType + ":" + instanceID
	if instanceID == "" {
		providerKey = fmt.Sprintf("history:%s:%d", entry.MediaType, watchedAt.UnixNano())
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
		media.SeriesTitle = entry.SeriesTitle
		media.SeriesExternalIds = cloneMap(ids)
		media.SeasonNumber = valueOrZero(entry.SeasonNumber)
		media.EpisodeNumber = valueOrZero(entry.EpisodeNumber)
	}
	return media
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
		nextOffset, more, paginationFault := nextOffsetFromHistory(upstream.Pagination, offset)
		if paginationFault != nil {
			return false, paginationFault
		}
		if !more {
			return false, nil
		}
		offset = nextOffset
	}
}

func nextOffsetFromHistory(page pagination, current int) (int, bool, *pluginv1.WatchSyncFault) {
	if page.Next == "" && (page.Total <= 0 || current+max(1, page.Limit) >= page.Total) {
		return 0, false, nil
	}
	next := current + max(1, page.Limit)
	if page.Next != "" {
		parsed, err := url.Parse(page.Next)
		if err != nil {
			return 0, false, temporaryFault("Floppy returned invalid history pagination", 0)
		}
		candidate, candidateErr := strconv.Atoi(parsed.Query().Get("offset"))
		if candidateErr != nil || candidate <= current {
			return 0, false, temporaryFault("Floppy returned invalid history pagination", 0)
		}
		next = candidate
	}
	if next <= current {
		return 0, false, temporaryFault("Floppy returned invalid history pagination", 0)
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
