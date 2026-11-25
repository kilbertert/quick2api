package common

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeRequestBodyEnforcesWholeByteBudget(t *testing.T) {
	const valid = `{"text":"你好"}`
	for _, knownLength := range []bool{false, true} {
		for _, test := range []struct {
			name, body string
			limit      int64
			status     int
		}{
			{"exact-byte-boundary", valid, int64(len(valid)), 200},
			{"one-byte-over", valid, int64(len(valid) - 1), 413},
			{"oversized-padding", valid + strings.Repeat(" ", 1000), 64, 413},
			{"small-padding", valid + " \n\t", 64, 200},
			{"multiple-values", valid + `{}`, 64, 400},
			{"malformed", `{"text":`, 64, 400},
		} {
			t.Run(test.name+map[bool]string{false: "/chunked", true: "/known-length"}[knownLength], func(t *testing.T) {
				r := httptest.NewRequest("POST", "/", strings.NewReader(test.body))
				if !knownLength {
					r.ContentLength = -1
				}
				var value struct{ Text string }
				err := DecodeRequestBody(httptest.NewRecorder(), r, &value, test.limit)
				if test.status == 200 {
					if err != nil || value.Text != "你好" {
						t.Fatalf("valid body changed or rejected: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatal("invalid or oversized body accepted")
				}
				status, kind, message := RequestBodyError(err)
				if status != test.status {
					t.Fatalf("wrong status: %d (%s)", status, message)
				}
				if status == 413 {
					var sizeErr *http.MaxBytesError
					if !errors.As(err, &sizeErr) || sizeErr.Limit != test.limit || kind != "request_too_large" || !strings.Contains(message, "server.max_request_body_bytes") {
						t.Fatalf("size rejection lost its budget: %v", err)
					}
				}
			})
		}
	}
}
