package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"go.kenn.io/msgvault/internal/config"
	"golang.org/x/oauth2"
)

const (
	googleOIDCLoginPath    = "/auth/google/login"
	googleOIDCCallbackPath = "/auth/google/callback"
	googleOIDCCookieName   = "msgvault_google_oidc"
	googleOIDCEmailScope   = "email"
	googleOIDCMaxPending   = 1024
)

type googleOIDCIdentity struct {
	Subject         string
	Email           string
	EmailVerified   bool
	HostedDomain    string
	Nonce           string
	AuthorizedParty string
}

type googleOIDCProvider interface {
	authorizationURL(state, nonce, pkceChallenge, hostedDomain string) string
	exchangeAndVerify(ctx context.Context, code, pkceVerifier string) (googleOIDCIdentity, error)
}

type discoveredGoogleOIDCProvider struct {
	oauth2Config oauth2.Config
	verifier     *oidc.IDTokenVerifier
}

func newDiscoveredGoogleOIDCProvider(ctx context.Context, cfg config.GoogleOIDCConfig) (*discoveredGoogleOIDCProvider, error) {
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover Google OIDC provider: %w", err)
	}
	clientID := cfg.ClientID()
	return &discoveredGoogleOIDCProvider{
		oauth2Config: oauth2.Config{
			ClientID: clientID, ClientSecret: cfg.ClientSecret(), Endpoint: provider.Endpoint(),
			RedirectURL: cfg.RedirectURL, Scopes: []string{oidc.ScopeOpenID, googleOIDCEmailScope},
		},
		verifier: provider.Verifier(&oidc.Config{ClientID: clientID}),
	}, nil
}

func (p *discoveredGoogleOIDCProvider) authorizationURL(state, nonce, pkceChallenge, hostedDomain string) string {
	options := []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("nonce", nonce),
		oauth2.SetAuthURLParam("code_challenge", pkceChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	}
	if hostedDomain != "" {
		options = append(options, oauth2.SetAuthURLParam("hd", hostedDomain))
	}
	return p.oauth2Config.AuthCodeURL(state, options...)
}

func (p *discoveredGoogleOIDCProvider) exchangeAndVerify(ctx context.Context, code, pkceVerifier string) (googleOIDCIdentity, error) {
	token, err := p.oauth2Config.Exchange(ctx, code, oauth2.VerifierOption(pkceVerifier))
	if err != nil {
		return googleOIDCIdentity{}, fmt.Errorf("exchange authorization code: %w", err)
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok || rawIDToken == "" {
		return googleOIDCIdentity{}, errors.New("OIDC token response did not include an ID token")
	}
	idToken, err := p.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return googleOIDCIdentity{}, fmt.Errorf("verify ID token: %w", err)
	}
	var claims struct {
		Subject         string `json:"sub"`
		Email           string `json:"email"`
		EmailVerified   bool   `json:"email_verified"`
		HostedDomain    string `json:"hd"`
		Nonce           string `json:"nonce"`
		AuthorizedParty string `json:"azp"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return googleOIDCIdentity{}, fmt.Errorf("decode ID token claims: %w", err)
	}
	return googleOIDCIdentity(claims), nil
}

type googleOIDCTransaction struct {
	Nonce        string
	PKCEVerifier string
	ReturnTo     string
	ExpiresAt    time.Time
}

type googleOIDCTransactionStore struct {
	mu           sync.Mutex
	transactions map[string]googleOIDCTransaction
	ttl          time.Duration
	now          func() time.Time
	random       io.Reader
}

func newGoogleOIDCTransactionStore(ttl time.Duration) *googleOIDCTransactionStore {
	return &googleOIDCTransactionStore{
		transactions: make(map[string]googleOIDCTransaction), ttl: ttl, now: time.Now, random: rand.Reader,
	}
}

func (s *googleOIDCTransactionStore) create(returnTo string) (string, googleOIDCTransaction, error) {
	state, err := randomURLToken(s.random)
	if err != nil {
		return "", googleOIDCTransaction{}, err
	}
	nonce, err := randomURLToken(s.random)
	if err != nil {
		return "", googleOIDCTransaction{}, err
	}
	verifier, err := randomURLToken(s.random)
	if err != nil {
		return "", googleOIDCTransaction{}, err
	}
	now := s.now()
	transaction := googleOIDCTransaction{Nonce: nonce, PKCEVerifier: verifier, ReturnTo: returnTo, ExpiresAt: now.Add(s.ttl)}
	s.mu.Lock()
	for key, candidate := range s.transactions {
		if !now.Before(candidate.ExpiresAt) {
			delete(s.transactions, key)
		}
	}
	if len(s.transactions) >= googleOIDCMaxPending {
		s.mu.Unlock()
		return "", googleOIDCTransaction{}, errors.New("too many pending Google login transactions")
	}
	s.transactions[state] = transaction
	s.mu.Unlock()
	return state, transaction, nil
}

func (s *googleOIDCTransactionStore) take(state string) (googleOIDCTransaction, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	transaction, ok := s.transactions[state]
	delete(s.transactions, state)
	if !ok || !s.now().Before(transaction.ExpiresAt) {
		return googleOIDCTransaction{}, false
	}
	return transaction, true
}

func (s *Server) registerGoogleOIDCRoutes(mux *http.ServeMux) {
	mux.HandleFunc(googleOIDCLoginPath, s.handleGoogleOIDCLogin)
	mux.HandleFunc(googleOIDCCallbackPath, s.handleGoogleOIDCCallback)
}

func (s *Server) handleGoogleOIDCLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed")
		return
	}
	if !s.cfg.GoogleOIDC.Ready() {
		writeError(w, http.StatusNotFound, "not_found", "Google login is not configured")
		return
	}
	returnTo, ok := safeOIDCReturnTo(r.URL.Query().Get("return_to"))
	if !ok {
		writeError(w, http.StatusBadRequest, "bad_request", "Invalid return path")
		return
	}
	provider, err := s.resolveGoogleOIDCProvider(r.Context())
	if err != nil {
		s.logger.Error("initialize Google OIDC", "error", err)
		writeError(w, http.StatusServiceUnavailable, "oidc_unavailable", "Google login is temporarily unavailable")
		return
	}
	state, transaction, err := s.googleOIDCTransactions.create(returnTo)
	if err != nil {
		s.logger.Error("create Google OIDC transaction", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Could not start Google login")
		return
	}
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // Secure follows the verified request scheme, matching the existing session cookie.
		Name: googleOIDCCookieName, Value: state, Path: "/auth/google", Expires: transaction.ExpiresAt,
		MaxAge: max(1, int(s.googleOIDCTransactions.ttl/time.Second)), HttpOnly: true,
		Secure: requestUsesHTTPS(r), SameSite: http.SameSiteLaxMode,
	})
	challengeBytes := sha256.Sum256([]byte(transaction.PKCEVerifier))
	challenge := base64.RawURLEncoding.EncodeToString(challengeBytes[:])
	// The destination is the authorization endpoint returned by OIDC discovery
	// for the validated configured issuer, never a request-provided URL.
	http.Redirect(w, r, provider.authorizationURL(state, transaction.Nonce, challenge, s.cfg.GoogleOIDC.HostedDomain), http.StatusFound) //nolint:gosec
}

func (s *Server) handleGoogleOIDCCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed")
		return
	}
	s.clearGoogleOIDCCookie(w, requestUsesHTTPS(r))
	state := r.URL.Query().Get("state")
	cookie, err := r.Cookie(googleOIDCCookieName)
	if err != nil || state == "" || subtle.ConstantTimeCompare([]byte(state), []byte(cookie.Value)) != 1 {
		writeError(w, http.StatusBadRequest, "invalid_oidc_state", "Invalid or expired Google login")
		return
	}
	transaction, ok := s.googleOIDCTransactions.take(state)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_oidc_state", "Invalid or expired Google login")
		return
	}
	if r.URL.Query().Get("error") != "" {
		writeError(w, http.StatusUnauthorized, "oidc_denied", "Google login was not completed")
		return
	}
	provider, err := s.resolveGoogleOIDCProvider(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "oidc_unavailable", "Google login is temporarily unavailable")
		return
	}
	identity, err := provider.exchangeAndVerify(r.Context(), r.URL.Query().Get("code"), transaction.PKCEVerifier)
	if err != nil {
		s.logger.Warn("Google OIDC callback verification failed", "error", err)
		writeError(w, http.StatusUnauthorized, "oidc_invalid", "Google login could not be verified")
		return
	}
	if err := validateGoogleOIDCIdentity(identity, transaction.Nonce, s.cfg.GoogleOIDC); err != nil {
		s.logger.Warn("Google OIDC identity denied", "reason", err)
		writeError(w, http.StatusForbidden, "oidc_forbidden", "This Google account is not allowed")
		return
	}
	id, session, err := s.sessions.create()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Could not create browser session")
		return
	}
	s.setBrowserSessionCookie(w, id, session, requestUsesHTTPS(r))
	writeOIDCCompletion(w, transaction.ReturnTo)
}

func (s *Server) resolveGoogleOIDCProvider(ctx context.Context) (googleOIDCProvider, error) {
	s.googleOIDCMu.Lock()
	defer s.googleOIDCMu.Unlock()
	if s.googleOIDCProvider != nil {
		return s.googleOIDCProvider, nil
	}
	provider, err := newDiscoveredGoogleOIDCProvider(ctx, s.cfg.GoogleOIDC)
	if err != nil {
		return nil, err
	}
	s.googleOIDCProvider = provider
	return provider, nil
}

func validateGoogleOIDCIdentity(identity googleOIDCIdentity, nonce string, cfg config.GoogleOIDCConfig) error {
	if identity.Subject == "" {
		return errors.New("missing subject")
	}
	if !identity.EmailVerified {
		return errors.New("email is not verified")
	}
	if !strings.EqualFold(strings.TrimSpace(identity.Email), cfg.AllowedEmail) {
		return errors.New("email is not allowlisted")
	}
	if identity.Nonce == "" || subtle.ConstantTimeCompare([]byte(identity.Nonce), []byte(nonce)) != 1 {
		return errors.New("nonce mismatch")
	}
	if cfg.HostedDomain != "" && !strings.EqualFold(identity.HostedDomain, cfg.HostedDomain) {
		return errors.New("hosted domain mismatch")
	}
	if identity.AuthorizedParty != "" && identity.AuthorizedParty != cfg.ClientID() {
		return errors.New("authorized party mismatch")
	}
	return nil
}

func safeOIDCReturnTo(raw string) (string, bool) {
	if raw == "" {
		return "/", true
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.User != nil ||
		!strings.HasPrefix(parsed.Path, "/") || strings.HasPrefix(parsed.Path, "//") ||
		strings.HasPrefix(raw, "//") || strings.Contains(raw, `\`) {
		return "", false
	}
	return raw, true
}

func (s *Server) clearGoogleOIDCCookie(w http.ResponseWriter, secure bool) {
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // The clearing cookie mirrors the verified request scheme and original cookie attributes.
		Name: googleOIDCCookieName, Value: "", Path: "/auth/google", Expires: time.Unix(1, 0),
		MaxAge: -1, HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode,
	})
}

func writeOIDCCompletion(w http.ResponseWriter, returnTo string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "<!doctype html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"0;url=%s\"><title>Signed in</title></head><body><p>Signed in. Continuing to msgvault…</p></body></html>", html.EscapeString(returnTo))
}
