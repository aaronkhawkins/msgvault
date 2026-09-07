package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/config"
	"golang.org/x/oauth2"
)

type fakeGoogleOIDCProvider struct {
	identity googleOIDCIdentity
	err      error
	state    string
	nonce    string
	pkce     string
	domain   string
}

func (f *fakeGoogleOIDCProvider) authorizationURL(state, nonce, pkceChallenge, hostedDomain string) string {
	f.state, f.nonce, f.pkce, f.domain = state, nonce, pkceChallenge, hostedDomain
	return "https://accounts.example.test/authorize?state=" + url.QueryEscape(state)
}

func (f *fakeGoogleOIDCProvider) exchangeAndVerify(_ context.Context, code, pkceVerifier string) (googleOIDCIdentity, error) {
	if code != "good-code" || pkceVerifier == "" {
		return googleOIDCIdentity{}, errors.New("bad exchange")
	}
	return f.identity, f.err
}

func TestGoogleOIDCAuthorizationURLUsesProvidedPKCEChallenge(t *testing.T) {
	assert := assert.New(t)
	provider := &discoveredGoogleOIDCProvider{oauth2Config: oauth2.Config{
		ClientID:    "client-id",
		Endpoint:    oauth2.Endpoint{AuthURL: "https://accounts.example.test/authorize"},
		RedirectURL: "https://archive.example.com/auth/google/callback",
		Scopes:      []string{"openid", "email"},
	}}

	authorizationURL, err := url.Parse(provider.authorizationURL("state", "nonce", "already-derived-challenge", "example.com"))
	require.NoError(t, err)
	query := authorizationURL.Query()
	assert.Equal("state", query.Get("state"))
	assert.Equal("nonce", query.Get("nonce"))
	assert.Equal("already-derived-challenge", query.Get("code_challenge"))
	assert.Equal("S256", query.Get("code_challenge_method"))
	assert.Equal("example.com", query.Get("hd"))
	assert.Equal("openid email", query.Get("scope"))
	assert.Equal("client-id", query.Get("client_id"))
	assert.Equal("https://archive.example.com/auth/google/callback", query.Get("redirect_uri"))
}

func TestGoogleOIDCLoginCreatesExistingBrowserSessionAndPreservesPermalink(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	provider := &fakeGoogleOIDCProvider{identity: googleOIDCIdentity{
		Subject: "google-subject", Email: "owner@example.com", EmailVerified: true,
		HostedDomain: "example.com", Nonce: "set-after-login", AuthorizedParty: "client-id",
	}}
	srv := newGoogleOIDCTestServer(t, provider)

	login := performSessionRequest(t, srv, http.MethodGet,
		googleOIDCLoginPath+"?return_to="+url.QueryEscape("/m/4242?from=personal-os"), nil, nil, true)
	require.Equal(http.StatusFound, login.Code, login.Body.String())
	assert.True(strings.HasPrefix(login.Header().Get("Location"), "https://accounts.example.test/authorize"))
	assert.NotEmpty(provider.state)
	assert.NotEmpty(provider.nonce)
	assert.NotEmpty(provider.pkce)
	assert.Equal("example.com", provider.domain)
	provider.identity.Nonce = provider.nonce

	transactionCookie := requireNamedCookie(t, login, googleOIDCCookieName)
	assert.Equal("/auth/google", transactionCookie.Path)
	assert.True(transactionCookie.HttpOnly)
	assert.True(transactionCookie.Secure)
	assert.Equal(http.SameSiteLaxMode, transactionCookie.SameSite)

	callbackHeaders := http.Header{"Cookie": []string{transactionCookie.String()}}
	callback := performSessionRequest(t, srv, http.MethodGet,
		googleOIDCCallbackPath+"?state="+url.QueryEscape(provider.state)+"&code=good-code",
		nil, callbackHeaders, true)
	require.Equal(http.StatusOK, callback.Code, callback.Body.String())
	assert.Contains(callback.Body.String(), "/m/4242?from=personal-os")
	assert.Equal("no-store", callback.Header().Get("Cache-Control"))
	assert.Equal("no-referrer", callback.Header().Get("Referrer-Policy"))

	sessionCookie := requireNamedCookie(t, callback, sessionCookieName)
	assert.Equal(http.SameSiteStrictMode, sessionCookie.SameSite)
	assert.True(sessionCookie.Secure)
	clearedTransaction := requireNamedCookie(t, callback, googleOIDCCookieName)
	assert.Negative(clearedTransaction.MaxAge)

	bootstrap := performSessionRequest(t, srv, http.MethodGet, sessionPath, nil,
		http.Header{"Cookie": []string{sessionCookie.String()}}, true)
	require.Equal(http.StatusOK, bootstrap.Code, bootstrap.Body.String())
	status := decodeSessionStatus(t, bootstrap)
	assert.Equal(AuthModeSession, status.AuthMode)
	assert.True(status.GoogleOIDCEnabled)
}

func TestGoogleOIDCCallbackRejectsInvalidIdentityAndReplay(t *testing.T) {
	assert := assert.New(t)
	provider := &fakeGoogleOIDCProvider{identity: googleOIDCIdentity{
		Subject: "google-subject", Email: "intruder@example.com", EmailVerified: true,
		HostedDomain: "example.com", AuthorizedParty: "client-id",
	}}
	srv := newGoogleOIDCTestServer(t, provider)
	login := performSessionRequest(t, srv, http.MethodGet, googleOIDCLoginPath, nil, nil, true)
	require.Equal(t, http.StatusFound, login.Code)
	provider.identity.Nonce = provider.nonce
	cookie := requireNamedCookie(t, login, googleOIDCCookieName)
	headers := http.Header{"Cookie": []string{cookie.String()}}
	path := googleOIDCCallbackPath + "?state=" + url.QueryEscape(provider.state) + "&code=good-code"

	denied := performSessionRequest(t, srv, http.MethodGet, path, nil, headers, true)
	assert.Equal(http.StatusForbidden, denied.Code, denied.Body.String())
	assert.Nil(findNamedCookie(denied, sessionCookieName))

	replay := performSessionRequest(t, srv, http.MethodGet, path, nil, headers, true)
	assert.Equal(http.StatusBadRequest, replay.Code, replay.Body.String())
	assert.Nil(findNamedCookie(replay, sessionCookieName))
}

func TestGoogleOIDCCallbackRejectsMismatchedStateBeforeExchange(t *testing.T) {
	provider := &fakeGoogleOIDCProvider{}
	srv := newGoogleOIDCTestServer(t, provider)
	login := performSessionRequest(t, srv, http.MethodGet, googleOIDCLoginPath, nil, nil, true)
	require.Equal(t, http.StatusFound, login.Code)
	cookie := requireNamedCookie(t, login, googleOIDCCookieName)

	callback := performSessionRequest(t, srv, http.MethodGet,
		googleOIDCCallbackPath+"?state=attacker-state&code=good-code", nil,
		http.Header{"Cookie": []string{cookie.String()}}, true)

	assert.Equal(t, http.StatusBadRequest, callback.Code, callback.Body.String())
	assert.Nil(t, findNamedCookie(callback, sessionCookieName))
}

func TestGoogleOIDCCallbackRejectsExchangeFailure(t *testing.T) {
	provider := &fakeGoogleOIDCProvider{err: errors.New("token verification failed")}
	srv := newGoogleOIDCTestServer(t, provider)
	login := performSessionRequest(t, srv, http.MethodGet, googleOIDCLoginPath, nil, nil, true)
	require.Equal(t, http.StatusFound, login.Code)
	provider.identity.Nonce = provider.nonce
	cookie := requireNamedCookie(t, login, googleOIDCCookieName)

	callback := performSessionRequest(t, srv, http.MethodGet,
		googleOIDCCallbackPath+"?state="+url.QueryEscape(provider.state)+"&code=good-code", nil,
		http.Header{"Cookie": []string{cookie.String()}}, true)

	assert.Equal(t, http.StatusUnauthorized, callback.Code, callback.Body.String())
	assert.Nil(t, findNamedCookie(callback, sessionCookieName))
}

func TestGoogleOIDCRoutesRemainUnavailableWhenNotConfigured(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{APIKey: testSessionAPIKey}}
	srv := NewServerWithOptions(ServerOptions{Config: cfg, Logger: testLogger()})
	t.Cleanup(func() { require.NoError(t, srv.Shutdown(context.Background())) })

	login := performSessionRequest(t, srv, http.MethodGet, googleOIDCLoginPath, nil, nil, true)
	assert.Equal(t, http.StatusNotFound, login.Code, login.Body.String())

	bootstrap := performSessionRequest(t, srv, http.MethodGet, sessionPath, nil, nil, true)
	require.Equal(t, http.StatusOK, bootstrap.Code, bootstrap.Body.String())
	assert.False(t, decodeSessionStatus(t, bootstrap).GoogleOIDCEnabled)
}

func TestGoogleOIDCRejectsExternalReturnURL(t *testing.T) {
	srv := newGoogleOIDCTestServer(t, &fakeGoogleOIDCProvider{})
	for _, returnTo := range []string{"https://evil.example/steal", "//evil.example/steal", `/\evil.example/steal`} {
		resp := performSessionRequest(t, srv, http.MethodGet,
			googleOIDCLoginPath+"?return_to="+url.QueryEscape(returnTo), nil, nil, true)
		assert.Equal(t, http.StatusBadRequest, resp.Code, resp.Body.String())
	}
}

func TestValidateGoogleOIDCIdentityFailsClosed(t *testing.T) {
	t.Setenv("MSGVAULT_GOOGLE_CLIENT_ID", "client-id")
	cfg := config.GoogleOIDCConfig{
		ClientIDEnv: "MSGVAULT_GOOGLE_CLIENT_ID", AllowedEmail: "owner@example.com", HostedDomain: "example.com",
	}
	valid := googleOIDCIdentity{
		Subject: "subject", Email: "owner@example.com", EmailVerified: true,
		HostedDomain: "example.com", Nonce: "nonce", AuthorizedParty: "client-id",
	}
	tests := []struct {
		name   string
		mutate func(*googleOIDCIdentity)
	}{
		{"missing subject", func(identity *googleOIDCIdentity) { identity.Subject = "" }},
		{"unverified email", func(identity *googleOIDCIdentity) { identity.EmailVerified = false }},
		{"wrong email", func(identity *googleOIDCIdentity) { identity.Email = "other@example.com" }},
		{"wrong hosted domain", func(identity *googleOIDCIdentity) { identity.HostedDomain = "other.example" }},
		{"wrong nonce", func(identity *googleOIDCIdentity) { identity.Nonce = "other" }},
		{"wrong authorized party", func(identity *googleOIDCIdentity) { identity.AuthorizedParty = "other-client" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := valid
			tt.mutate(&identity)
			assert.Error(t, validateGoogleOIDCIdentity(identity, "nonce", cfg))
		})
	}
	assert.NoError(t, validateGoogleOIDCIdentity(valid, "nonce", cfg))
}

func TestGoogleOIDCTransactionStoreBoundsPendingLogins(t *testing.T) {
	store := newGoogleOIDCTransactionStore(10 * time.Minute)
	for index := range googleOIDCMaxPending {
		store.transactions[strconv.Itoa(index)] = googleOIDCTransaction{ExpiresAt: time.Now().Add(time.Minute)}
	}

	_, _, err := store.create("/")
	assert.ErrorContains(t, err, "too many pending")
}

func TestAPIKeyLoginRemainsAvailableWithGoogleOIDC(t *testing.T) {
	srv := newGoogleOIDCTestServer(t, &fakeGoogleOIDCProvider{})
	resp := performSessionRequest(t, srv, http.MethodPost, sessionLoginPath,
		[]byte(`{"api_key":"`+testSessionAPIKey+`"}`), nil, true)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	assert.Equal(t, AuthModeSession, decodeSessionStatus(t, resp).AuthMode)
}

func newGoogleOIDCTestServer(t *testing.T, provider googleOIDCProvider) *Server {
	t.Helper()
	t.Setenv("MSGVAULT_GOOGLE_CLIENT_ID", "client-id")
	t.Setenv("MSGVAULT_GOOGLE_CLIENT_SECRET", "client-secret")
	cfg := &config.Config{
		Server: config.ServerConfig{APIKey: testSessionAPIKey},
		GoogleOIDC: config.GoogleOIDCConfig{
			Enabled: true, Issuer: "https://accounts.example.test",
			ClientIDEnv: "MSGVAULT_GOOGLE_CLIENT_ID", ClientSecretEnv: "MSGVAULT_GOOGLE_CLIENT_SECRET",
			RedirectURL:  "https://archive.example.com/auth/google/callback",
			AllowedEmail: "owner@example.com", HostedDomain: "example.com",
		},
	}
	srv := NewServerWithOptions(ServerOptions{Config: cfg, Logger: testLogger(), googleOIDCProvider: provider})
	t.Cleanup(func() { require.NoError(t, srv.Shutdown(context.Background())) })
	return srv
}

func findNamedCookie(resp *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, cookie := range resp.Result().Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	return nil
}

func requireNamedCookie(t *testing.T, resp *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	cookie := findNamedCookie(resp, name)
	require.NotNil(t, cookie, "%s cookie not found", name)
	return cookie
}
