package api

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/config"
	"golang.org/x/oauth2"
)

type oidcTimeoutTransport func(*http.Request) (*http.Response, error)

func (f oidcTimeoutTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestGoogleOIDCDiscoveryBoundsHTTPClientWithoutMutatingIt(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second, time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			client := &http.Client{Timeout: timeout, Transport: oidcTimeoutTransport(func(r *http.Request) (*http.Response, error) {
				deadline, bounded := r.Context().Deadline()
				require.True(t, bounded, "OIDC HTTP client needs a timeout independent of request context")
				want := googleOIDCHTTPTimeout
				if timeout > 0 && timeout < want {
					want = timeout
				}
				require.InDelta(t, want.Seconds(), time.Until(deadline).Seconds(), 0.5)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"issuer":"https://oidc.example","jwks_uri":"https://oidc.example/jwks"}`))}, nil
			})}
			ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
			_, err := newDiscoveredGoogleOIDCProvider(ctx, config.GoogleOIDCConfig{Issuer: "https://oidc.example"})
			require.NoError(t, err)
			require.Equal(t, timeout, client.Timeout)
		})
	}
}

func TestGoogleOIDCSigningKeyFetchTimeoutAllowsRetry(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	t.Setenv("MSGVAULT_GOOGLE_CLIENT_ID", "client-id")
	mux := http.NewServeMux()
	server := httptest.NewTLSServer(mux)
	defer server.Close()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"issuer": server.URL, "authorization_endpoint": server.URL + "/authorize",
			"token_endpoint": server.URL + "/token", "jwks_uri": server.URL + "/jwks",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	var requests atomic.Int32
	stalledRequestCanceled := make(chan struct{})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			<-r.Context().Done()
			close(stalledRequestCanceled)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"keys": []any{map[string]string{
			"kty": "RSA", "kid": "timeout-key", "alg": "RS256", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": "AQAB",
		}}})
	})
	client := server.Client()
	client.Timeout = 500 * time.Millisecond
	discoveryCtx, cancelDiscovery := context.WithCancel(context.WithValue(context.Background(), oauth2.HTTPClient, client))
	provider, err := newDiscoveredGoogleOIDCProvider(discoveryCtx, config.GoogleOIDCConfig{
		Issuer: server.URL, ClientIDEnv: "MSGVAULT_GOOGLE_CLIENT_ID",
	})
	require.NoError(t, err)
	cancelDiscovery()
	require.Equal(t, 500*time.Millisecond, client.Timeout)

	payload, err := json.Marshal(map[string]any{
		"iss": server.URL, "aud": "client-id", "sub": "subject", "exp": time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, err)
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"timeout-key"}`)) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)
	token := unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = provider.verifier.Verify(ctx, token)
	require.Error(t, err)
	select {
	case <-stalledRequestCanceled:
	case <-ctx.Done():
		require.FailNow(t, "JWKS fetch outlived its HTTP timeout")
	}
	// The provider remains usable after discovery cancellation and the failed
	// shared JWKS fetch releases its slot for a fresh request.
	require.Eventually(t, func() bool {
		_, err := provider.verifier.Verify(ctx, token)
		return err == nil
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(2), requests.Load())
}
