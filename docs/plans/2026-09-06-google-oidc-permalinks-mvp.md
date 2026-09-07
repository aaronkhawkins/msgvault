---
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
title: Google OIDC and source-message permalinks MVP
created: 2026-09-06
---

# Google OIDC and source-message permalinks MVP

## Goal capsule

Ship a narrow, single-user MsgVault fork for a personal archive. The
browser signs in through a dedicated Google OIDC client, existing API-key clients
continue unchanged, and an HTTPS permalink opens one exact archive message while
the canonical `msgvault://<URL-encoded mailbox>/<archive id>` identity remains
separate. Production stays untouched until the isolated preview is reviewed.

## Scope boundaries

- Base the deployable fork on the supported `v0.19.3` viewer release. Rebase the
  focused upstream contributions onto current `main` separately when ready.
- Support Google OIDC only, one configured verified email, and an optional hosted
  domain. Do not add multi-user accounts, RBAC, or a provider-settings UI.
- Reuse MsgVault's in-memory browser sessions. Preserve API-key CLI/API/MCP access
  and the API-key browser login as recovery access.
- Add a stable presentation path for an archive-local numeric message ID. Do not
  claim that database row IDs survive export/reimport or replace canonical source
  identity with an HTTPS URL.
- Use a body-first pilot and existing attachment viewers. No new downstream mail
  viewer or attachment feature work.
- Preserve the archive and take a straightforward backup before any live storage
  migration. No rollback rehearsal or enterprise cutover machinery.

## Seven-stage MVP plan

### 1. Baseline and archive compatibility

Confirm the target personal archive, leaving any other archive instances untouched. Exercise `v0.19.3` against an isolated copy or read-only-derived pilot
archive and verify that its embedded web UI and schema startup complete.

### 2. Fork and reproducible build

Maintain `aaronkhawkins/msgvault` with a focused feature branch based on `v0.19.3`.
Build a versioned container from the repository's digest-pinned multi-stage
Dockerfile and deploy by immutable image digest.

### 3. Google browser login

Add optional Google OIDC configuration backed by environment-resolved secrets.
Use authorization code flow with PKCE, state, nonce, issuer/audience/expiry checks,
verified-email enforcement, exact allowlisting, callback replay prevention, and a
short-lived Lax transaction cookie. On success create the existing Strict browser
session and use a same-origin completion page. Keep API-key auth behavior intact.

Test scenarios: disabled configuration, safe return paths, state/cookie mismatch,
callback replay, exchange failure, wrong nonce, unverified/wrong email, hosted
domain mismatch, authorized login, session creation, and unchanged API-key access.

### 4. Dedicated Google client configuration

Use the existing family Google project with a new MsgVault-specific OAuth client
and the archive hostname callback. Store the client secret through the established
Vaultwarden/owner-only environment workflow; never reuse another app's client or
commit credentials. This is a preview/production configuration task, not a code
dependency for local fake-provider tests.

### 5. Exact-message source links

Add `/m/<archive-id>` as the narrow stable browser presentation path. Normalize it
into the existing Everything workspace selection and keep the path through login,
callback completion, refresh, Back, and Forward. Validate positive integer IDs and
show the existing not-found behavior for missing messages. Update downstream link
presentation only after the preview proves the route; retain the canonical
`msgvault://` reference as the source identity.

Test scenarios: direct load, percent/query noise rejection, exact row selection,
login round-trip, refresh/history restoration, far-back paging, missing message,
and an existing attachment opened from the selected message.

### 6. Regression, security, and device checks

Run focused Go and web tests, then the repository's normal Go/web checks and
release build. Exercise API-key CLI/API/MCP calls against the preview. Use the
isolated body-only archive to verify an exact message and one attachment on the
Framework browser plus an iPhone-sized viewport. Submit permalink and OIDC as
focused upstream contributions with operator documentation.

### 7. Reviewed production switch

Show the working isolated preview and the concrete one-user production changes for
review. After approval, take the ordinary archive backup if a storage migration is
required, pause writes only as needed, deploy the tested image digest to
the personal archive, configure the dedicated Google client, and verify login,
source links, sync, attachment access, and downstream integration. Do not modify
other archive instances.

## Verification contract

- Focused: Go config/session/OIDC route tests; web session/permalink/component tests.
- Integration: fake OIDC provider callback creates a normal MsgVault browser
  session and returns to `/m/<id>` without exposing tokens or credentials.
- Compatibility: existing API-key login and authenticated `/api/v1` route tests,
  plus a CLI/API/MCP smoke check against the preview.
- Build: generated OpenAPI/web client remain synchronized; web production build,
  Go release build, and container build succeed.
- Preview: isolated archive opens the exact linked body after fresh Google login,
  refresh, and existing session; one attachment renders; desktop and mobile-sized
  layouts remain usable.

## Definition of done

The fork and feature branch exist; tests and builds pass; a digest-addressable
preview is running without production changes; the operator can review Google login
and an exact-message link; focused upstream-ready commits and documentation exist; and
the production change remains gated on that review.
