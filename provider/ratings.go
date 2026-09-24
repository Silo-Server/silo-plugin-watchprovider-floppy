package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	floppyMovie = "movie"
	floppyTV    = "tv"

	// Every media-list result carries the full item, including its per-region
	// watch-provider data, which Floppy notes can reach ~146 KiB per title.
	// Twenty titles, Floppy's own default page, stays well inside
	// maxResponseBytes.
	ratingPageLimit = 20

	// Floppy's status codes 0-4 (Planning, In progress, Paused, Completed,
	// Dropped) plus no_status. Without a status filter Floppy leaves out
	// statusless rows, which is how it stores a rating imported without any
	// tracking state; "all,no_status" would return only the statusless ones.
	ratingStatusFilter = "0,1,2,3,4,no_status"

	// Floppy's status code for Planning.
	floppyStatusPlanning = 0

	// One page of a title's consumption history covers nearly every title;
	// the page cap bounds a server that never stops paging.
	ratingHistoryPageLimit = 100
	ratingHistoryMaxPages  = 10

	ratingKeyPrefix = "rating"

	ratingsChangedMessage = "Floppy ratings changed during the sync; it will start over"
)

// ratingTraversal is the page token for a RATING snapshot: rated movies first,
// then rated TV. Offset is the first position the next page covers, and
// LastKey identifies the title the previous page ended with. The next request
// starts one position earlier so that title comes back first. When it does
// not, a rating added or cleared mid-traversal has shifted the offsets, and the
// snapshot restarts instead of skipping a title the host would then read as
// unrated.
type ratingTraversal struct {
	Phase   string `json:"phase"`
	Offset  int    `json:"offset,omitempty"`
	LastKey string `json:"last_key,omitempty"`
}

func (s *Server) listRatings(ctx context.Context, client *apiClient, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	token, fault := ratingTraversalFromRequest(req)
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	// Every rated title costs a history read, so a page is time-boxed like an
	// ApplyEvents batch and its requests end before the host's deadline.
	budget := syncTimeBox(ctx)
	ctx, cancel := context.WithTimeout(ctx, syncRequestLimit(ctx))
	defer cancel()
	startedAt := s.clock()
	limit := min(pageSize(req.GetPageSize()), ratingPageLimit)
	offset := token.Offset
	overlap := 0
	if token.Offset > 0 {
		// Re-read the previous page's last title as the overlap check.
		offset--
		limit++
		overlap = 1
	}
	query := url.Values{
		"limit":     {strconv.Itoa(limit)},
		"offset":    {strconv.Itoa(offset)},
		"rating":    {"rated"},
		"status":    {ratingStatusFilter},
		"sort":      {"id"},
		"direction": {"asc"},
	}
	var upstream ratedMediaResponse
	if status, requestFault := client.request(ctx, http.MethodGet, "/api/v1/media/"+token.Phase+"/", query, nil, &upstream, "Bearer"); requestFault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: ratingReadFault(ctx, client, status, requestFault)}, nil
	}
	results := upstream.Results
	if token.Offset > 0 {
		if len(results) == 0 || ratingListingKey(results[0]) != token.LastKey {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault(ratingsChangedMessage, 0)}, nil
		}
		results = results[1:]
	}
	_, more, paginationFault := nextOffsetFromPagination(upstream.Pagination, offset)
	if paginationFault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: paginationFault}, nil
	}

	// Ratings have no incremental feed, so every traversal is a complete
	// snapshot and returns no durable cursor.
	response := &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: true}
	seen := make(map[string]struct{}, len(results))
	for index, entry := range results {
		tmdbID := ratingTitleID(token.Phase, entry)
		if tmdbID == "" {
			continue
		}
		key := ratingKey(token.Phase, tmdbID)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		if index > 0 && ratingListingKey(results[index-1]) != "" && s.clock().Sub(startedAt) >= budget {
			// Out of time: end the page after the previous entry, which the
			// next page re-reads as its overlap check.
			results = results[:index]
			more = true
			break
		}
		seen[key] = struct{}{}
		score, resolveFault := resolveTitleScore(ctx, client, token.Phase, tmdbID, entry.Score)
		if resolveFault != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: resolveFault}, nil
		}
		response.Items = append(response.Items, ratingState(token.Phase, entry, tmdbID, score))
	}

	switch {
	case more:
		// A non-final page must bring a new title, and its last entry needs an
		// ID for the next page's overlap check.
		lastKey := ""
		if len(results) > 0 {
			lastKey = ratingListingKey(results[len(results)-1])
		}
		if lastKey == "" {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault("Floppy returned invalid pagination", 0)}, nil
		}
		// The next page starts after the last entry this page covered.
		response.NextPageToken = encodePageToken(ratingTraversal{
			Phase: token.Phase, Offset: offset + overlap + len(results), LastKey: lastKey,
		})
	case token.Phase == floppyMovie:
		response.NextPageToken = encodePageToken(ratingTraversal{Phase: floppyTV})
	}
	return response, nil
}

func ratingTraversalFromRequest(req *pluginv1.WatchSyncListRemoteStateRequest) (ratingTraversal, *pluginv1.WatchSyncFault) {
	if strings.TrimSpace(req.GetPageToken()) == "" {
		return ratingTraversal{Phase: floppyMovie}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(req.GetPageToken())
	if err != nil {
		return ratingTraversal{}, invalidRequestFault("Floppy page token is invalid")
	}
	var token ratingTraversal
	if err := json.Unmarshal(decoded, &token); err != nil || token.Offset < 0 ||
		(token.Offset > 0) != (token.LastKey != "") ||
		(token.Phase != floppyMovie && token.Phase != floppyTV) {
		return ratingTraversal{}, invalidRequestFault("Floppy page token is invalid")
	}
	return token, nil
}

// ratingListingKey identifies a listed entry for the overlap check. It covers
// every entry, including titles the plugin skips, because the check is
// positional.
func ratingListingKey(entry ratedMediaEntry) string {
	if id := rawString(entry.ID); id != "" {
		return "item:" + id
	}
	if item := entry.Item; item != nil && rawString(item.MediaID) != "" {
		return strings.Join([]string{"media", item.MediaType, item.Source, item.LibraryMediaType, rawString(item.MediaID)}, ":")
	}
	return ""
}

// ratingTitleID returns the TMDB ID of a listed title the plugin syncs, or ""
// when the title cannot round-trip through Floppy's rating routes.
func ratingTitleID(phase string, entry ratedMediaEntry) string {
	item := entry.Item
	if item == nil || !strings.EqualFold(strings.TrimSpace(item.MediaType), phase) {
		return ""
	}
	// Floppy files movies and TV under TMDB or manual entries, and its media
	// routes cannot reach manual titles or TV kept in the anime library bucket.
	// Importing them would produce ratings that Silo can never update.
	if !strings.EqualFold(strings.TrimSpace(item.Source), "tmdb") ||
		(phase == floppyTV && strings.EqualFold(strings.TrimSpace(item.LibraryMediaType), "anime")) {
		return ""
	}
	tmdbID := strings.TrimSpace(rawString(item.MediaID))
	if !validTMDBID(tmdbID) {
		return ""
	}
	return tmdbID
}

// ratingState maps one rated Floppy title to a RATING state.
func ratingState(phase string, entry ratedMediaEntry, tmdbID string, score float64) *pluginv1.WatchSyncRemoteState {
	item := entry.Item
	ids := normalizedIDs(item.ProviderExternalIDs)
	for namespace, id := range normalizedIDs(item.IDs) {
		ids[namespace] = id
	}
	ids["tmdb"] = tmdbID
	mediaType := pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE
	if phase == floppyTV {
		mediaType = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES
	}
	return &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: ratingKey(phase, tmdbID),
		Media: &pluginv1.WatchSyncMedia{
			MediaItemId: "tmdb:" + tmdbID,
			MediaType:   mediaType,
			Title:       item.Title,
			Year:        releaseYear(item.ReleaseDatetime),
			ExternalIds: ids,
		},
		// Floppy exposes no time for when a score was set, so rated_at is omitted.
		Rating: &pluginv1.WatchSyncRemoteRatingState{Rating: ratingFromScore(score)},
	}
}

// resolveTitleScore returns the score Floppy shows for a rated title, read from
// its consumption history. The listing alone cannot be trusted: it reports the
// newest-created consumption's own score, which a rewatch leaves empty and a
// backdated entry leaves stale, while Floppy's rated filter and UI read the
// most recently active consumption that has a score. The listing does not say
// whether a title has more than one consumption, so every title costs a read.
//
// A movie with per-play history lists its plays in place of its consumptions,
// so the listed score is the only one available for it.
func resolveTitleScore(ctx context.Context, client *apiClient, floppyMediaType, tmdbID string, listed *float64) (float64, *pluginv1.WatchSyncFault) {
	title, status, fault := loadTitleConsumptions(ctx, client, floppyMediaType, tmdbID)
	switch {
	case status == http.StatusNotFound:
		return 0, temporaryFault(ratingsChangedMessage, 0)
	case fault != nil:
		return 0, ratingReadFault(ctx, client, status, fault)
	}
	if !title.perPlay {
		if latest := latestScored(title.rows); latest != nil {
			return *latest.score, nil
		}
	} else if listed != nil {
		return *listed, nil
	}
	return 0, temporaryFault("Floppy did not return the entry that holds a rated title's score; the sync will retry", 0)
}

// ratingFromScore converts Floppy's 0-10 decimal score to Silo's 1-10 integer
// rating, rounding half up and clamping to the valid range.
func ratingFromScore(score float64) int32 {
	return int32(min(10, max(1, math.Floor(score+0.5))))
}

func ratingKey(floppyMediaType, tmdbID string) string {
	return ratingKeyPrefix + ":" + floppyMediaType + ":tmdb:" + tmdbID
}

func ratingTitlePath(floppyMediaType, tmdbID string) string {
	return "/api/v1/media/" + floppyMediaType + "/tmdb/" + url.PathEscape(tmdbID) + "/"
}

// titleConsumptions is what one title's consumption history reveals.
type titleConsumptions struct {
	rows []titleConsumption
	// perPlay reports that Floppy listed a movie's per-play rows, which carry
	// no score, in place of the consumptions that do.
	perPlay bool
}

type titleConsumption struct {
	id       int64
	score    *float64
	planning bool
	created  time.Time
	activity time.Time
}

// loadTitleConsumptions reads a title's whole consumption history. The status
// is the failing response's HTTP status, or zero.
func loadTitleConsumptions(ctx context.Context, client *apiClient, floppyMediaType, tmdbID string) (titleConsumptions, int, *pluginv1.WatchSyncFault) {
	var title titleConsumptions
	path := ratingTitlePath(floppyMediaType, tmdbID) + "history/"
	offset := 0
	for range ratingHistoryMaxPages {
		query := url.Values{"limit": {strconv.Itoa(ratingHistoryPageLimit)}, "offset": {strconv.Itoa(offset)}}
		var page consumptionHistoryResponse
		if status, fault := client.request(ctx, http.MethodGet, path, query, nil, &page, "Bearer"); fault != nil {
			return titleConsumptions{}, status, fault
		}
		for _, entry := range page.Results {
			if entry.ExternalID != nil {
				title.perPlay = true
				continue
			}
			row, ok := titleConsumptionFrom(entry)
			if !ok {
				return titleConsumptions{}, 0, temporaryFault("Floppy returned an unreadable rating history", 0)
			}
			title.rows = append(title.rows, row)
		}
		next, more, fault := nextOffsetFromPagination(page.Pagination, offset)
		if fault != nil {
			return titleConsumptions{}, 0, fault
		}
		if !more {
			return title, 0, nil
		}
		offset = next
	}
	return titleConsumptions{}, 0, temporaryFault("Floppy returned too much rating history for one title", 0)
}

// titleConsumptionFrom parses one consumption. Its activity is Floppy's
// ordering key for "most recent": the end date, else the progress time, else
// the creation time.
func titleConsumptionFrom(entry consumptionHistory) (titleConsumption, bool) {
	created, err := time.Parse(time.RFC3339Nano, entry.Created)
	if err != nil || entry.ID <= 0 {
		return titleConsumption{}, false
	}
	activity := created
	for _, value := range []string{entry.EndDate, entry.ProgressedAt} {
		if value == "" {
			continue
		}
		if activity, err = time.Parse(time.RFC3339Nano, value); err != nil {
			return titleConsumption{}, false
		}
		break
	}
	return titleConsumption{
		id:       entry.ID,
		score:    entry.Score,
		planning: entry.Status != nil && *entry.Status == floppyStatusPlanning,
		created:  created,
		activity: activity,
	}, true
}

// latestScored returns the consumption Floppy's rated filter and UI read a
// title's rating from: the most recently active one with a score, ties broken
// by the higher ID.
func latestScored(rows []titleConsumption) *titleConsumption {
	var latest *titleConsumption
	for index := range rows {
		row := &rows[index]
		if row.score == nil {
			continue
		}
		if latest == nil || row.activity.After(latest.activity) ||
			(row.activity.Equal(latest.activity) && row.id > latest.id) {
			latest = row
		}
	}
	return latest
}

// newestByCreation returns the consumption Floppy's media listing reports a
// title's score from.
func newestByCreation(rows []titleConsumption) *titleConsumption {
	var newest *titleConsumption
	for index := range rows {
		row := &rows[index]
		if newest == nil || row.created.After(newest.created) ||
			(row.created.Equal(newest.created) && row.id > newest.id) {
			newest = row
		}
	}
	return newest
}

func applyRatingEvent(ctx context.Context, client *apiClient, event *pluginv1.WatchSyncEvent) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	eventID := event.GetEventId()
	media := event.GetMedia()
	var floppyMediaType string
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		floppyMediaType = floppyMovie
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES:
		floppyMediaType = floppyTV
	default:
		return rejectedResult(eventID, "Floppy syncs ratings for movies and series only"), nil
	}
	tmdbID := ratingTMDBID(media, event.GetProviderItemKey(), floppyMediaType)
	if tmdbID == "" {
		return rejectedResult(eventID, "Rating event needs a TMDB identifier"), nil
	}
	remove := event.GetOperation() == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING
	rating := event.GetRating()
	if !remove && (rating < 1 || rating > 10) {
		return rejectedResult(eventID, "Rating must be an integer from 1 to 10"), nil
	}

	// Both operations are desired-state writes: each reads what Floppy holds
	// and writes only what differs, so a redelivered event changes nothing.
	title, status, fault := loadTitleConsumptions(ctx, client, floppyMediaType, tmdbID)
	if status == http.StatusNotFound || (fault == nil && len(title.rows) == 0 && !title.perPlay) {
		// A title Floppy does not track holds no rating, so a removal has
		// nothing to clear. A rating cannot land until Floppy tracks it.
		if remove {
			return ratingResult(eventID, false), nil
		}
		return rejectedResult(eventID, "Floppy is not tracking this title"), nil
	}
	if fault != nil {
		return ratingFailure(ctx, client, eventID, status, fault, "watchlist:read")
	}
	path := ratingTitlePath(floppyMediaType, tmdbID)
	switch {
	case title.perPlay:
		return patchTitleScore(ctx, client, eventID, path, remove, rating)
	case remove:
		return clearTitleScores(ctx, client, eventID, path, title.rows)
	default:
		return setTitleScore(ctx, client, eventID, path, title.rows, rating)
	}
}

// setTitleScore leaves Floppy showing rating everywhere it reads one. Its
// media listing reports the newest consumption's own score, and its rated
// filter and UI read the most recently active consumption that has a score.
// The newest consumption takes the rating first; a more recently active one
// that still shows another score takes it too.
func setTitleScore(ctx context.Context, client *apiClient, eventID, path string, rows []titleConsumption, rating int32) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	holds := func(row *titleConsumption) bool {
		return row != nil && row.score != nil && ratingFromScore(*row.score) == rating
	}
	newest := newestByCreation(rows)
	if holds(latestScored(rows)) && (newest.score == nil || holds(newest)) {
		return ratingResult(eventID, false), nil
	}
	score := float64(rating)
	write := func(row *titleConsumption) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
		if holds(row) {
			return nil, nil
		}
		status, fault := patchConsumptionScore(ctx, client, path, row.id, &rating)
		if fault == nil {
			row.score = &score
			return nil, nil
		}
		if status == http.StatusNotFound {
			// Floppy deletes a title's Planning consumptions when a completed
			// one is saved. The retry reads the title again.
			return resultFromFault(eventID, temporaryFault("Floppy changed this title during the rating update; it will be retried", 0)), nil
		}
		return ratingFailure(ctx, client, eventID, status, fault, "watchlist:write")
	}
	if result, fault := write(newest); result != nil || fault != nil {
		return result, fault
	}
	// An older consumption with more recent activity can still show another
	// score in front of the newest one.
	if result, fault := write(latestScored(rows)); result != nil || fault != nil {
		return result, fault
	}
	return ratingResult(eventID, true), nil
}

// clearTitleScores clears the score on every consumption that has one;
// Floppy would otherwise go on showing the most recently active remaining
// score. Planning consumptions go first, because saving a completed
// consumption without a score copies a Planning consumption's score into it.
func clearTitleScores(ctx context.Context, client *apiClient, eventID, path string, rows []titleConsumption) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	var scored []titleConsumption
	for _, planningPass := range []bool{true, false} {
		for _, row := range rows {
			if row.score != nil && row.planning == planningPass {
				scored = append(scored, row)
			}
		}
	}
	if len(scored) == 0 {
		return ratingResult(eventID, false), nil
	}
	for _, row := range scored {
		status, fault := patchConsumptionScore(ctx, client, path, row.id, nil)
		// A consumption that is gone no longer holds a score.
		if fault != nil && status != http.StatusNotFound {
			return ratingFailure(ctx, client, eventID, status, fault, "watchlist:write")
		}
	}
	return ratingResult(eventID, true), nil
}

// patchConsumptionScore writes one consumption's score and confirms Floppy
// stored it. The status is the failing response's HTTP status, or zero.
func patchConsumptionScore(ctx context.Context, client *apiClient, titlePath string, id int64, rating *int32) (int, *pluginv1.WatchSyncFault) {
	var stored consumptionHistory
	path := titlePath + "history/" + strconv.FormatInt(id, 10) + "/"
	if status, fault := client.request(ctx, http.MethodPatch, path, nil, ratingPayload{Score: rating}, &stored, "Bearer"); fault != nil {
		return status, fault
	}
	if (rating == nil) != (stored.Score == nil) || (rating != nil && *stored.Score != float64(*rating)) {
		return 0, temporaryFault("Floppy did not store the rating change; it will be retried", 0)
	}
	return 0, nil
}

// patchTitleScore writes through the title route, which updates the title's
// newest consumption. A movie whose history lists per-play rows exposes no
// other way to reach its score.
func patchTitleScore(ctx context.Context, client *apiClient, eventID, path string, remove bool, rating int32) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	var payload ratingPayload
	if !remove {
		payload.Score = &rating
	}
	status, fault := client.request(ctx, http.MethodPatch, path, nil, payload, nil, "Bearer")
	if fault != nil {
		if status == http.StatusNotFound {
			if remove {
				return ratingResult(eventID, false), nil
			}
			return rejectedResult(eventID, "Floppy is not tracking this title"), nil
		}
		return ratingFailure(ctx, client, eventID, status, fault, "watchlist:write")
	}
	return ratingResult(eventID, true), nil
}

// ratingFailure maps a failed rating request to the event's result or to a
// connection-wide fault. Floppy's Bearer routes answer 403 both for a token
// without the route's scope and for a token that no longer authenticates, so
// a 403 counts as a missing scope, rejected for this event only, once
// validate-token has accepted the token.
func ratingFailure(ctx context.Context, client *apiClient, eventID string, status int, fault *pluginv1.WatchSyncFault, scope string) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	if status == http.StatusForbidden {
		if !client.tokenValid {
			if _, validateFault := validateAccount(ctx, client); validateFault != nil {
				fault = validateFault
			} else {
				client.tokenValid = true
			}
		}
		if client.tokenValid {
			return &pluginv1.WatchSyncApplyResult{
				EventId: eventID,
				Status:  pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED,
				Fault: &pluginv1.WatchSyncFault{
					Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
					SafeMessage: "The Floppy API token needs the " + scope + " scope to send ratings",
				},
			}, nil
		}
	}
	if connectionWide(fault) {
		return nil, fault
	}
	return resultFromFault(eventID, fault), nil
}

// ratingReadFault maps a failed rating read. As with writes, a 403 from a
// Bearer route is a missing scope once validate-token accepts the token, and a
// credential fault otherwise.
func ratingReadFault(ctx context.Context, client *apiClient, status int, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncFault {
	if status != http.StatusForbidden {
		return fault
	}
	if !client.tokenValid {
		if _, validateFault := validateAccount(ctx, client); validateFault != nil {
			return validateFault
		}
		client.tokenValid = true
	}
	return &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
		SafeMessage: "The Floppy API token needs the watchlist:read scope to import ratings",
	}
}

func ratingResult(eventID string, changed bool) *pluginv1.WatchSyncApplyResult {
	status := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
	if changed {
		status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	}
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: status}
}

// ratingTMDBID returns the event's TMDB ID, falling back to a rating key this
// plugin returned earlier for the same media type.
func ratingTMDBID(media *pluginv1.WatchSyncMedia, providerItemKey, floppyMediaType string) string {
	if id := filteredExternalIDs(media.GetExternalIds())["tmdb"]; validTMDBID(id) {
		return id
	}
	parts := strings.Split(strings.TrimSpace(providerItemKey), ":")
	if len(parts) == 4 && parts[0] == ratingKeyPrefix && parts[1] == floppyMediaType && parts[2] == "tmdb" && validTMDBID(parts[3]) {
		return parts[3]
	}
	return ""
}

func isRatingOperation(operation pluginv1.WatchSyncOperation) bool {
	return operation == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING ||
		operation == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING
}

// validTMDBID accepts TMDB's numeric IDs only, which also keeps the value safe
// to place in a URL path.
func validTMDBID(value string) bool {
	if value == "" || len(value) > 20 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func releaseYear(value string) int32 {
	value = strings.TrimSpace(value)
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return int32(parsed.Year())
	}
	if len(value) >= 4 {
		if year, err := strconv.Atoi(value[:4]); err == nil && year > 0 {
			return int32(year)
		}
	}
	return 0
}
