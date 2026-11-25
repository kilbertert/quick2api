package config

import "testing"

func TestRequestBodyLimitDefaultsAndBounds(t *testing.T) {
	if (*Config)(nil).RequestBodyLimit() != 32<<20 {
		t.Fatal("nil configuration did not retain the default")
	}
	for _, limit := range []int64{0, 1, 32 << 20, 128 << 20, -1, (128 << 20) + 1} {
		cfg := Defaults()
		cfg.Server.MaxRequestBodyBytes = limit
		valid := limit >= 0 && limit <= 128<<20
		if err := cfg.Validate(); (err == nil) != valid {
			t.Fatalf("limit=%d: validation=%v", limit, err)
		}
		if valid {
			want := limit
			if want == 0 {
				want = 32 << 20
			}
			if cfg.RequestBodyLimit() != want {
				t.Fatalf("limit=%d: effective limit=%d", limit, cfg.RequestBodyLimit())
			}
		}
	}
}

func TestNativeTransportRequiresExplicitLocalAuthAndRegistration(t *testing.T) {
	for _, kind := range []string{"default", "valid", "no-key", "remote", "hostname", "no-relay", "server", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			cfg := Defaults()
			if kind != "default" {
				cfg.Tools.ClientTransport = "mcp"
				cfg.Tools.NativeRelayName = "mcp__quick2api__quick2api_client_call"
				cfg.API.APIKey = "local-fixture"
			}
			switch kind {
			case "no-key":
				cfg.API.APIKey = ""
			case "remote":
				cfg.Server.Listen = "0.0.0.0:8377"
			case "hostname":
				cfg.Server.Listen = "localhost:8377"
			case "no-relay":
				cfg.Tools.NativeRelayName = ""
			case "server":
				cfg.Tools.Mode = "server"
			case "timeout":
				cfg.Tools.BridgeTimeoutS = 301
			}
			err := cfg.Validate()
			valid := kind == "default" || kind == "valid"
			if (err == nil) != valid {
				t.Fatalf("Validate()=%v, expected valid=%v", err, valid)
			}
		})
	}
}
