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
