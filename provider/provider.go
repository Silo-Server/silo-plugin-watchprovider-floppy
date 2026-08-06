package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	capabilityID    = "floppy"
	configBaseURL   = "floppy.base_url"
	defaultPageSize = 50
)

type Server struct {
	pluginv1.UnimplementedWatchSyncProviderServer
	http *http.Client
}

func NewServer(httpClient *http.Client) *Server {
	return &Server{http: httpClient}
}

func (s *Server) ExchangeAPIKey(ctx context.Context, req *pluginv1.WatchSyncExchangeAPIKeyRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	client, fault := s.client(req.GetCapabilityId(), req.GetProviderConfig(), req.GetApiKey())
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	account, fault := validateAccount(ctx, client)
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	return &pluginv1.WatchSyncCredentialResponse{
		Credentials: credentials(req.GetApiKey(), client.baseURL.String()),
		Account:     account,
	}, nil
}

func (s *Server) RefreshCredentials(ctx context.Context, req *pluginv1.WatchSyncRefreshCredentialsRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	account, fault := validateAccount(ctx, client)
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	return &pluginv1.WatchSyncCredentialResponse{
		Credentials: cloneCredentials(req.GetContext().GetCredentials()),
		Account:     account,
	}, nil
}

func (s *Server) GetAccount(ctx context.Context, req *pluginv1.WatchSyncGetAccountRequest) (*pluginv1.WatchSyncGetAccountResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: fault}, nil
	}
	account, fault := validateAccount(ctx, client)
	return &pluginv1.WatchSyncGetAccountResponse{Account: account, Fault: fault}, nil
}

func (s *Server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
	}
	response := &pluginv1.WatchSyncApplyEventsResponse{
		Results: make([]*pluginv1.WatchSyncApplyResult, 0, len(req.GetEvents())),
	}
	for _, event := range req.GetEvents() {
		result, connectionFault := s.applyEvent(ctx, client, event)
		if connectionFault != nil {
			return &pluginv1.WatchSyncApplyEventsResponse{Fault: connectionFault}, nil
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func (s *Server) ListRemoteState(ctx context.Context, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	kind, fault := requestedStateKind(req.GetStateKinds())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	switch kind {
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED:
		return s.listWatched(ctx, client, req)
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS:
		return s.listProgress(ctx, client, req)
	default:
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
			SafeMessage: "Floppy does not support the requested state family",
		}}, nil
	}
}

func (s *Server) applyEvent(ctx context.Context, client *apiClient, event *pluginv1.WatchSyncEvent) (*pluginv1.WatchSyncApplyResult, *pluginv1.WatchSyncFault) {
	if event == nil || strings.TrimSpace(event.GetEventId()) == "" {
		return rejectedResult("", "Watch event ID is required"), nil
	}
	payload, completed, fault := payloadFromEvent(event)
	if fault != nil {
		return resultFromFault(event.GetEventId(), fault), nil
	}
	if completed {
		alreadyApplied, lookupFault := historyContainsEvent(ctx, client, event)
		if lookupFault != nil {
			if connectionWide(lookupFault) {
				return nil, lookupFault
			}
			return resultFromFault(event.GetEventId(), lookupFault), nil
		}
		if alreadyApplied {
			return &pluginv1.WatchSyncApplyResult{
				EventId: event.GetEventId(),
				Status:  pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
			}, nil
		}
	}
	// Floppy's scrobble endpoint is convergent: start and pause replace the live
	// playback state, while incomplete stops upsert durable progress. Completed
	// stops additionally use the history lookup above for at-least-once delivery.
	requestFault := client.do(ctx, http.MethodPost, "/api/v1/scrobble/", nil, payload, nil, "Bearer")
	if requestFault != nil {
		if connectionWide(requestFault) {
			return nil, requestFault
		}
		return resultFromFault(event.GetEventId(), requestFault), nil
	}
	return &pluginv1.WatchSyncApplyResult{
		EventId: event.GetEventId(),
		Status:  pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
	}, nil
}

func payloadFromEvent(event *pluginv1.WatchSyncEvent) (scrobblePayload, bool, *pluginv1.WatchSyncFault) {
	media := event.GetMedia()
	if media == nil {
		return scrobblePayload{}, false, invalidRequestFault("Watch event media is required")
	}
	payload := scrobblePayload{
		IDs:         mergedExternalIDs(media),
		Title:       media.GetTitle(),
		SeriesTitle: media.GetSeriesTitle(),
	}
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		payload.MediaType = "movie"
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		payload.MediaType = "episode"
		season, episode := media.GetSeasonNumber(), media.GetEpisodeNumber()
		if season < 0 || episode < 1 {
			return scrobblePayload{}, false, invalidRequestFault("Episode events require season and episode numbers")
		}
		payload.SeasonNumber = &season
		payload.EpisodeNumber = &episode
	default:
		return scrobblePayload{}, false, invalidRequestFault("Floppy supports movie and episode events only")
	}
	if len(payload.IDs) == 0 {
		return scrobblePayload{}, false, invalidRequestFault("Watch event needs a TMDB, IMDb, or TVDB identifier")
	}

	completed := false
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		payload.Action = "stop"
		completed = true
		payload.Completed = boolPointer(true)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START:
		payload.Action = "start"
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE:
		payload.Action = "pause"
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		payload.Action = "stop"
		completed = event.GetWatchHistoryId() != "" || event.GetCompletionPercent() >= 100
		if completed {
			payload.Completed = boolPointer(true)
		}
	default:
		return scrobblePayload{}, false, invalidRequestFault("Floppy does not support this watch operation")
	}
	if event.GetPositionSeconds() > 0 {
		value := int64(math.Round(event.GetPositionSeconds()))
		payload.PositionSeconds = &value
	}
	if event.GetDurationSeconds() > 0 {
		value := int64(math.Round(event.GetDurationSeconds()))
		payload.DurationSeconds = &value
	}
	if event.GetOccurredAt() != nil && event.GetOccurredAt().CheckValid() == nil {
		payload.PlayedAt = event.GetOccurredAt().AsTime().UTC().Format(time.RFC3339Nano)
	}
	return payload, completed, nil
}

func validateAccount(ctx context.Context, client *apiClient) (*pluginv1.WatchSyncAccount, *pluginv1.WatchSyncFault) {
	var response validateTokenResponse
	if fault := client.do(ctx, http.MethodGet, "/apis/listenbrainz/1/validate-token", nil, nil, &response, "Token"); fault != nil {
		return nil, fault
	}
	if !response.Valid || strings.TrimSpace(response.Username) == "" {
		return nil, &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			SafeMessage: "Floppy did not validate the API token",
		}
	}
	return &pluginv1.WatchSyncAccount{
		ExternalSubject: response.Username,
		Username:        response.Username,
		DisplayName:     response.Username,
	}, nil
}

func (s *Server) authenticatedClient(auth *pluginv1.WatchSyncAuthenticatedContext) (*apiClient, *pluginv1.WatchSyncFault) {
	if auth == nil || auth.GetCredentials() == nil {
		return nil, invalidRequestFault("Floppy credentials are required")
	}
	baseURL := auth.GetCredentials().GetSecretAttributes()[configBaseURL]
	return s.client(auth.GetCapabilityId(), auth.GetProviderConfig(), auth.GetCredentials().GetAccessToken(), baseURL)
}

func (s *Server) client(requestedCapability string, config *pluginv1.WatchSyncProviderConfig, token string, connectionBaseURL ...string) (*apiClient, *pluginv1.WatchSyncFault) {
	if requestedCapability != capabilityID {
		return nil, invalidRequestFault("Unknown Floppy capability")
	}
	if strings.TrimSpace(token) == "" {
		return nil, invalidRequestFault("Floppy API token is required")
	}
	baseURL := ""
	if len(connectionBaseURL) > 0 {
		baseURL = strings.TrimSpace(connectionBaseURL[0])
	}
	if config != nil {
		if baseURL == "" {
			baseURL = config.GetValues()[configBaseURL]
		}
		if baseURL == "" {
			baseURL = config.GetSecretValues()[configBaseURL]
		}
	}
	client, err := newAPIClient(baseURL, token, s.http)
	if err != nil {
		return nil, invalidRequestFault(err.Error())
	}
	return client, nil
}

func credentials(token, baseURL string) *pluginv1.WatchSyncCredentials {
	return &pluginv1.WatchSyncCredentials{
		AccessToken: strings.TrimSpace(token), TokenType: "Bearer",
		SecretAttributes: map[string]string{configBaseURL: strings.TrimSpace(baseURL)},
	}
}

func cloneCredentials(value *pluginv1.WatchSyncCredentials) *pluginv1.WatchSyncCredentials {
	if value == nil {
		return nil
	}
	return &pluginv1.WatchSyncCredentials{
		AccessToken: value.GetAccessToken(), RefreshToken: value.GetRefreshToken(),
		ExpiresAt: value.GetExpiresAt(), TokenType: value.GetTokenType(),
		Scopes:           append([]string(nil), value.GetScopes()...),
		SecretAttributes: cloneMap(value.GetSecretAttributes()),
	}
}

func requestedStateKind(kinds []pluginv1.WatchSyncRemoteStateKind) (pluginv1.WatchSyncRemoteStateKind, *pluginv1.WatchSyncFault) {
	if len(kinds) == 0 {
		return pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED, nil
	}
	if len(kinds) != 1 {
		return 0, invalidRequestFault("Floppy accepts one state family per traversal")
	}
	return kinds[0], nil
}

type traversalToken struct {
	Offset      int       `json:"offset,omitempty"`
	HighWater   time.Time `json:"high_water"`
	BoundaryAt  time.Time `json:"boundary_at,omitempty"`
	BoundaryKey string    `json:"boundary_key,omitempty"`
}

func traversal(req *pluginv1.WatchSyncListRemoteStateRequest) (traversalToken, *pluginv1.WatchSyncFault) {
	if strings.TrimSpace(req.GetPageToken()) == "" {
		return traversalToken{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(req.GetPageToken())
	if err != nil {
		return traversalToken{}, invalidRequestFault("Floppy page token is invalid")
	}
	var token traversalToken
	if err := json.Unmarshal(decoded, &token); err != nil || token.Offset < 0 || token.HighWater.IsZero() {
		return traversalToken{}, invalidRequestFault("Floppy page token is invalid")
	}
	return token, nil
}

func nextPageToken(token traversalToken) string {
	encoded, _ := json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func parseCursor(value string) (time.Time, *pluginv1.WatchSyncFault) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, invalidRequestFault("Floppy cursor is invalid")
	}
	return parsed.UTC(), nil
}

func pageSize(requested int32) int {
	if requested <= 0 {
		return defaultPageSize
	}
	return min(100, int(requested))
}

func invalidRequestFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
		SafeMessage: message,
	}
}

func connectionWide(fault *pluginv1.WatchSyncFault) bool {
	return fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL ||
		fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED
}

func resultFromFault(eventID string, fault *pluginv1.WatchSyncFault) *pluginv1.WatchSyncApplyResult {
	status := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
	if fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY ||
		fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED {
		status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	}
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: status, Fault: fault}
}

func rejectedResult(eventID, message string) *pluginv1.WatchSyncApplyResult {
	return resultFromFault(eventID, invalidRequestFault(message))
}

func boolPointer(value bool) *bool { return &value }

func cloneMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func rawString(value json.RawMessage) string {
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	var decoded string
	if err := json.Unmarshal(value, &decoded); err == nil {
		return decoded
	}
	if json.Valid(value) && !strings.HasPrefix(trimmed, `"`) {
		return trimmed
	}
	return ""
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case json.Number:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}

func normalizedIDs(input map[string]any) map[string]string {
	output := make(map[string]string)
	for key, value := range input {
		key = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(key)), "_id")
		if key != "tmdb" && key != "imdb" && key != "tvdb" {
			continue
		}
		if id := strings.TrimSpace(stringValue(value)); id != "" {
			output[key] = id
		}
	}
	return output
}

func mergedExternalIDs(media *pluginv1.WatchSyncMedia) map[string]string {
	seriesIDs := filteredExternalIDs(media.GetSeriesExternalIds())
	if media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE && len(seriesIDs) > 0 {
		// Floppy identifies episode history and scrobbles by the series IDs plus
		// season/episode numbers. Episode-level IDs share the same namespaces but
		// are not interchangeable with series IDs.
		return seriesIDs
	}
	output := filteredExternalIDs(media.GetExternalIds())
	if output == nil {
		output = make(map[string]string)
	}
	for key, value := range seriesIDs {
		if output[key] == "" {
			output[key] = value
		}
	}
	return output
}

func filteredExternalIDs(input map[string]string) map[string]string {
	output := make(map[string]string)
	for key, value := range input {
		key = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(key)), "_id")
		value = strings.TrimSpace(value)
		if (key == "tmdb" || key == "imdb" || key == "tvdb") && value != "" {
			output[key] = value
		}
	}
	if len(output) == 0 {
		return nil
	}
	return output
}

func timestamp(value string) *timestamppb.Timestamp {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil
	}
	return timestamppb.New(parsed)
}
