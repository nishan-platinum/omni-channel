# ADR-0007 — Bootstrap authentication (temporary M02 stand-in)

* Status: Accepted · Date: 2026-10-06

## Decision
* Minimal identity in schema `identity`: users (role `super_admin` | `tenant_admin`), sessions,
  invitations. Argon2id password hashing, lockout after 5 failures for 15 minutes.
* Browser: opaque random session token in an `HttpOnly`, `SameSite=Lax` cookie (`Secure` outside
  development); DB stores only its SHA-256. Idle timeout from the tenant security policy
  (`security.session_idle_timeout_minutes`, default 30, SEC-004), absolute 12 h. Synchronizer CSRF token
  per session; login form uses a double-submit CSRF cookie.
* API: `POST /v1/bootstrap/token` exchanges email/password (+ optional tenant code) for an opaque bearer
  token (1 h) with scopes derived from the role (`tenants:read`, `tenants:write`, and `platform:elevated`
  for SA). This replaces OAuth2/JWT (API-002) until M02 exists.
* Tenant resolution happens server-side from the user record. Tenant users of a non-active tenant are
  denied (draft → FORBIDDEN, suspended → TENANT_SUSPENDED, terminated/purged → FORBIDDEN); a grace
  tenant may log in read-only.
* The initial Super Admin is seeded from `BOOTSTRAP_SUPERADMIN_EMAIL/PASSWORD` when absent.
* Invitations: single-use, 72 h, SHA-256 stored; link delivered through `NotificationPort` (dev outbox).

## Not implemented (M02)
SAML/OIDC SSO, MFA, enterprise RBAC, API keys/OAuth clients, IP allow-list enforcement.
