package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	maxResponseBytes = 8 << 20
	// A cold Floppy metadata lookup can legitimately take more than a minute.
	// Stay below the host's two-minute watch-sync RPC deadline so transport
	// failures are still reported as provider faults with room for cleanup.
	defaultRequestTimeout = 110 * time.Second
)

// apiClient serves one RPC; it is not shared across calls.
type apiClient struct {
	baseURL *url.URL
	token   string
	http    *http.Client
	// tokenValid records that validate-token accepted the token during this
	// call, so a 403 from a Bearer route means a missing scope.
	tokenValid bool
}

func newAPIClient(rawBaseURL, token string, httpClient *http.Client) (*apiClient, error) {
	baseURL, err := url.Parse(strings.TrimSpace(rawBaseURL))
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return nil, errors.New("Floppy base URL must be an absolute http or https URL")
	}
	if baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, errors.New("Floppy base URL must not include credentials, a query, or a fragment")
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/")
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	return &apiClient{baseURL: baseURL, token: strings.TrimSpace(token), http: httpClient}, nil
}

func (c *apiClient) endpoint(path string, query url.Values) string {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(c.baseURL.Path, "/") + "/" + strings.TrimLeft(path, "/")
	endpoint.RawQuery = query.Encode()
	return endpoint.String()
}

func (c *apiClient) do(ctx context.Context, method, path string, query url.Values, payload, output any, tokenScheme string) *pluginv1.WatchSyncFault {
	_, fault := c.request(ctx, method, path, query, payload, output, tokenScheme)
	return fault
}

// request is do plus the HTTP status, which is zero when no response arrived.
// Callers use it when one status needs handling beyond the shared fault.
func (c *apiClient) request(ctx context.Context, method, path string, query url.Values, payload, output any, tokenScheme string) (int, *pluginv1.WatchSyncFault) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, permanentFault("Floppy request could not be encoded")
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path, query), body)
	if err != nil {
		return 0, permanentFault("Floppy request could not be created")
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", tokenScheme+" "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, temporaryFault("Floppy is temporarily unreachable", 0)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return resp.StatusCode, faultForHTTPResponse(resp)
	}
	if output == nil || resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
		return resp.StatusCode, nil
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes))
	if err := decoder.Decode(output); err != nil {
		return resp.StatusCode, temporaryFault("Floppy returned an unreadable response", 0)
	}
	return resp.StatusCode, nil
}

func faultForHTTPResponse(resp *http.Response) *pluginv1.WatchSyncFault {
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			SafeMessage: "Floppy rejected the API token",
		}
	case http.StatusTooManyRequests:
		return &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
			SafeMessage: "Floppy rate limit reached",
			RetryAfter:  durationpb.New(retryAfter(resp.Header.Get("Retry-After"))),
		}
	case http.StatusRequestTimeout:
		return temporaryFault("Floppy request timed out", 0)
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity:
		return &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
			SafeMessage: fmt.Sprintf("Floppy rejected the request (HTTP %d)", resp.StatusCode),
		}
	default:
		if resp.StatusCode >= http.StatusInternalServerError {
			return temporaryFault("Floppy is temporarily unavailable", 0)
		}
		return permanentFault(fmt.Sprintf("Floppy request failed (HTTP %d)", resp.StatusCode))
	}
}

func retryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, time.Until(at))
	}
	return 0
}

func temporaryFault(message string, retryAfter time.Duration) *pluginv1.WatchSyncFault {
	fault := &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
		SafeMessage: message,
	}
	if retryAfter > 0 {
		fault.RetryAfter = durationpb.New(retryAfter)
	}
	return fault
}

func permanentFault(message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
		SafeMessage: message,
	}
}
