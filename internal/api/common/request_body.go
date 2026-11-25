package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// DecodeRequestBody accepts exactly one JSON value within a byte budget.
// Checking Content-Length also rejects oversized bodies with a short valid
// JSON prefix. Reading through EOF enforces the same rule on chunked input.
func DecodeRequestBody(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	if r.ContentLength > limit {
		return &http.MaxBytesError{Limit: limit}
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("request body must contain exactly one JSON value")
	}
	return nil
}

// RequestBodyError keeps size rejection distinct from malformed JSON. It does
// not include request content or credentials in the error response.
func RequestBodyError(err error) (status int, kind, message string) {
	var oversized *http.MaxBytesError
	if errors.As(err, &oversized) {
		return http.StatusRequestEntityTooLarge, "request_too_large", fmt.Sprintf("request body exceeds server.max_request_body_bytes (%d bytes); no upstream request was sent", oversized.Limit)
	}
	return http.StatusBadRequest, "invalid_request_error", "bad JSON: " + err.Error()
}
