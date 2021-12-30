package server

import (
	"context"
	"net/http"

	"github.com/kopia/kopia/internal/auth"
)

func requireUIUser(s *Server, r *http.Request) bool {
	if s.authenticator == nil {
		return true
	}

	if c, err := r.Cookie(kopiaUISessionCookie); err == nil && c != nil {
		if remaining := s.isAuthCookieValid(s.options.UIUser, c.Value); remaining > 0 {
			// found a short-term JWT cookie that matches given username, trust it.
			return true
		}
	}

	user, _, _ := r.BasicAuth()

	return user == s.options.UIUser
}

func requireServerControlUser(s *Server, r *http.Request) bool {
	if s.authenticator == nil {
		return true
	}

	user, _, _ := r.BasicAuth()

	return user == s.options.ServerControlUser
}

func anyAuthenticatedUser(s *Server, r *http.Request) bool {
	return true
}

func handlerWillCheckAuthorization(s *Server, r *http.Request) bool {
	return true
}

func requireContentAccess(level auth.AccessLevel) isAuthorizedFunc {
	return func(s *Server, r *http.Request) bool {
		return s.httpAuthorizationInfo(r.Context(), r).ContentAccessLevel() >= level
	}
}

func hasManifestAccess(ctx context.Context, s *Server, r *http.Request, labels map[string]string, level auth.AccessLevel) bool {
	return s.httpAuthorizationInfo(ctx, r).ManifestAccessLevel(labels) >= level
}

var (
	_ isAuthorizedFunc = requireUIUser
	_ isAuthorizedFunc = anyAuthenticatedUser
	_ isAuthorizedFunc = handlerWillCheckAuthorization
)
