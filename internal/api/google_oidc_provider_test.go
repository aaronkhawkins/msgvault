package api

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/config"
	"golang.org/x/oauth2"
)

// Exercise discovery, token exchange, JWKS retrieval, and signature/claim
// verification together, rather than replacing the production provider seam.
func TestDiscoveredGoogleOIDCProviderVerifiesSignedTokens(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	t.Setenv("MSGVAULT_GOOGLE_CLIENT_ID", "client-id")
	t.Setenv("MSGVAULT_GOOGLE_CLIENT_SECRET", "client-secret")
	for _, scenario := range []string{"valid", "wrong issuer", "wrong audience", "expired", "wrong signature", "missing token"} {
		t.Run(scenario, func(t *testing.T) {
			mux := http.NewServeMux()
			server := httptest.NewTLSServer(mux)
			defer server.Close()
			claims := map[string]any{
				"iss": server.URL, "aud": "client-id", "exp": time.Now().Add(time.Hour).Unix(),
				"sub": "synthetic-subject", "email": "owner@example.com", "email_verified": true,
				"nonce": "nonce", "azp": "client-id",
			}
			signingKey := key
			switch scenario {
			case "wrong issuer":
				claims["iss"] = "https://other.example.test"
			case "wrong audience":
				claims["aud"] = "other-client"
			case "expired":
				claims["exp"] = time.Now().Add(-time.Hour).Unix()
			case "wrong signature":
				signingKey = otherKey
			}
			header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"test-key"}`))
			payload, err := json.Marshal(claims)
			require.NoError(t, err)
			unsigned := header + "." + base64.RawURLEncoding.EncodeToString(payload)
			digest := sha256.Sum256([]byte(unsigned))
			signature, err := rsa.SignPKCS1v15(rand.Reader, signingKey, crypto.SHA256, digest[:])
			require.NoError(t, err)
			token := unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
			mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, map[string]any{
					"issuer": server.URL, "authorization_endpoint": server.URL + "/authorize",
					"token_endpoint": server.URL + "/token", "jwks_uri": server.URL + "/jwks",
					"id_token_signing_alg_values_supported": []string{"RS256"},
				})
			})
			mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, map[string]any{"keys": []any{map[string]string{
					"kty": "RSA", "kid": "test-key", "alg": "RS256", "use": "sig",
					"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": "AQAB",
				}}})
			})
			mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
				if !assert.NoError(t, r.ParseForm()) {
					http.Error(w, "bad form", http.StatusBadRequest)
					return
				}
				assert.Equal(t, "authorization_code", r.Form.Get("grant_type"))
				assert.Equal(t, "code", r.Form.Get("code"))
				assert.Equal(t, "verifier", r.Form.Get("code_verifier"))
				assert.Equal(t, "https://archive.example.com/auth/google/callback", r.Form.Get("redirect_uri"))
				response := map[string]any{"access_token": "synthetic-access-token", "token_type": "Bearer"}
				if scenario != "missing token" {
					response["id_token"] = token
				}
				writeJSON(w, http.StatusOK, response)
			})
			ctx := context.WithValue(context.Background(), oauth2.HTTPClient, server.Client())
			provider, err := newDiscoveredGoogleOIDCProvider(ctx, config.GoogleOIDCConfig{
				Issuer: server.URL, ClientIDEnv: "MSGVAULT_GOOGLE_CLIENT_ID", ClientSecretEnv: "MSGVAULT_GOOGLE_CLIENT_SECRET",
				RedirectURL: "https://archive.example.com/auth/google/callback",
			})
			require.NoError(t, err)
			identity, err := provider.exchangeAndVerify(ctx, "code", "verifier")
			if scenario != "valid" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "synthetic-subject", identity.Subject)
			assert.Equal(t, "owner@example.com", identity.Email)
			assert.True(t, identity.EmailVerified)
			assert.Equal(t, "nonce", identity.Nonce)
		})
	}
}
