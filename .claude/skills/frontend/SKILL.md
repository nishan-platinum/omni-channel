---
name: frontend
description: Procedure for server-rendered UI work (Axum handlers + Askama templates + small HTMX enhancements) for Super Admin and Tenant Admin screens.
---

# Server-rendered UI procedure

No SPA, no npm. Askama templates in `templates/`, CSS in `static/css/app.css`, tiny JS in
`static/js/app.js`, HTMX vendored at `static/js/htmx.min.js`.

1. **Authorisation first.** Use the extractors from `bootstrap_auth::extract`: `SuperAdmin` for
   `/admin/*`, `TenantAdmin` for `/tenant/*`. Hiding links is never authorisation. TA routes never take
   a tenant id; they use the session's tenant.
2. **Handler** (`src/modules/m01_tenancy/web/`): parse the form → call an application service → map the
   result to a view model. No SQL, no business rules.
3. **CSRF**: every state-changing form includes `<input type="hidden" name="_csrf" value="{{ csrf }}">`.
   HTMX requests send the `X-CSRF-Token` header (set globally via `hx-headers` on `<body>`). Validate
   with the `CsrfForm<T>` extractor (or `csrf_matches` for multipart forms).
4. **Template**: extend `layouts/base.html`; reuse partials in `templates/partials/`. Keep Askama
   escaping on; never mark user data `|safe`. Show per-field validation errors (STD-001) and keep the
   user's input on error; flash success/error banners; status badges; empty states (STD-006);
   confirmation dialogs (`data-confirm`) on destructive lifecycle actions (STD-005).
5. **HTMX** only where it helps (feature toggles, lifecycle buttons, branding preview, filtering).
   The non-HTMX form post must still work (progressive enhancement): check `HX-Request` and return a
   partial, otherwise redirect (POST/redirect/GET).
6. **Accessibility**: labels bound to inputs, `aria-live` for flash messages, focus-visible styles,
   keyboard-reachable controls, colour contrast.
7. **Test**: add an HTML smoke assertion in `tests/m01/ui.rs` (status code, key text, CSRF rejection).
