package provider

import "encoding/json"

type validateTokenResponse struct {
	Valid    bool   `json:"valid"`
	Username string `json:"user_name"`
}

type pagination struct {
	Total  int    `json:"total"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
	Next   string `json:"next"`
}

type historyResponse struct {
	Pagination pagination   `json:"pagination"`
	Results    []historyDay `json:"results"`
}

type historyDay struct {
	Date    string         `json:"date"`
	Entries []historyEntry `json:"entries"`
}

type historyEntry struct {
	MediaType    string          `json:"media_type"`
	Item         historyItem     `json:"item"`
	Title        string          `json:"title"`
	DisplayTitle string          `json:"display_title"`
	Status       string          `json:"status"`
	PlayedAt     string          `json:"played_at_local"`
	PlayCount    int             `json:"play_count"`
	InstanceID   json.RawMessage `json:"instance_id"`
	EpisodeLabel string          `json:"episode_label"`
	EpisodeCode  string          `json:"episode_code"`
}

type historyItem struct {
	MediaType          string          `json:"media_type"`
	MediaID            json.RawMessage `json:"media_id"`
	Source             string          `json:"source"`
	Title              string          `json:"title"`
	SeasonNumber       *int32          `json:"season_number"`
	EpisodeNumber      *int32          `json:"episode_number"`
	ProviderExternalID map[string]any  `json:"provider_external_ids"`
}

type progressResponse struct {
	Pagination pagination      `json:"pagination"`
	Results    []progressEntry `json:"results"`
}

type progressEntry struct {
	MediaType     string          `json:"media_type"`
	Source        string          `json:"source"`
	MediaID       json.RawMessage `json:"media_id"`
	SeasonNumber  *int32          `json:"season_number"`
	EpisodeNumber *int32          `json:"episode_number"`
	IDs           map[string]any  `json:"ids"`
	Title         string          `json:"title"`
	SeriesTitle   string          `json:"series_title"`
	Position      float64         `json:"position_seconds"`
	Duration      float64         `json:"duration_seconds"`
	Completed     bool            `json:"completed"`
	UpdatedAt     string          `json:"updated_at"`
}

// ratedMediaResponse is one page of GET /api/v1/media/{media_type}/. Each
// result is one title: Floppy folds repeat consumptions into the newest row,
// and score is that row's own score, which a rewatch leaves empty.
type ratedMediaResponse struct {
	Pagination pagination        `json:"pagination"`
	Results    []ratedMediaEntry `json:"results"`
}

type ratedMediaEntry struct {
	// ID is Floppy's item ID for the title.
	ID    json.RawMessage `json:"id"`
	Score *float64        `json:"score"`
	Item  *ratedMediaItem `json:"item"`
}

type ratedMediaItem struct {
	MediaType           string          `json:"media_type"`
	MediaID             json.RawMessage `json:"media_id"`
	Source              string          `json:"source"`
	LibraryMediaType    string          `json:"library_media_type"`
	Title               string          `json:"title"`
	ReleaseDatetime     string          `json:"release_datetime"`
	IDs                 map[string]any  `json:"ids"`
	ProviderExternalIDs map[string]any  `json:"provider_external_ids"`
}

// consumptionHistoryResponse is one page of GET
// /api/v1/media/{media_type}/{source}/{media_id}/history/.
type consumptionHistoryResponse struct {
	Pagination pagination           `json:"pagination"`
	Results    []consumptionHistory `json:"results"`
}

// consumptionHistory is one of a title's consumptions (tracker rows), each
// with its own score. Once a Floppy movie has per-play rows, the route lists
// those plays instead; only they carry the external_id key, and they have no
// score. PATCH .../history/{consumption_id}/ answers with the same shape.
type consumptionHistory struct {
	ID           int64           `json:"consumption_id"`
	Score        *float64        `json:"score"`
	Status       *int            `json:"status"`
	Created      string          `json:"created"`
	ProgressedAt string          `json:"progressed_at"`
	EndDate      string          `json:"end_date"`
	ExternalID   json.RawMessage `json:"external_id"`
}

// ratingPayload is the PATCH body for a score. A nil Score encodes as JSON
// null, which clears the rating.
type ratingPayload struct {
	Score *int32 `json:"score"`
}

type scrobblePayload struct {
	Action          string            `json:"action"`
	MediaType       string            `json:"media_type"`
	IDs             map[string]string `json:"ids"`
	Title           string            `json:"title,omitempty"`
	SeriesTitle     string            `json:"series_title,omitempty"`
	SeasonNumber    *int32            `json:"season_number,omitempty"`
	EpisodeNumber   *int32            `json:"episode_number,omitempty"`
	PositionSeconds *int64            `json:"position_seconds,omitempty"`
	DurationSeconds *int64            `json:"duration_seconds,omitempty"`
	Completed       *bool             `json:"completed,omitempty"`
	PlayedAt        string            `json:"played_at,omitempty"`
}
