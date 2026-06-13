# Authentication & access control

Two surfaces are protected by **Microsoft (Azure AD) login**:

- **Web UI (`:3000`)** — built-in OAuth in the gateway. Roles: **viewer** (read/query only) and **admin** (everything). Allow-listed per email under **Admin → users**.
- **Dagster console (`:3001`)** — fronted by **oauth2-proxy** (same Azure app), so only allow-listed Microsoft accounts reach it.
- **API tokens** — for programmatic access; each token is scoped to **one connection** and carries a role.

Auth is **opt-in**: with `AZURE_*` unset the UI stays open (anonymous = admin), exactly as before. Set the vars to turn it on.

---

## 1. Create the Azure AD App Registration

Azure Portal → **Entra ID → App registrations → New registration**:

- Name: `Viamedia Data Lake`
- Supported accounts: *Single tenant* (your org).
- **Redirect URIs** (type **Web**) — add **both**:
  - `https://<gateway-host>/auth/callback`  (web UI)
  - `https://<dagster-host>/oauth2/callback`  (Dagster via oauth2-proxy)
- After creating: **Certificates & secrets → New client secret** → copy the **Value**.
- Note **Application (client) ID** and **Directory (tenant) ID** from Overview.
- **API permissions** → Microsoft Graph → delegated **openid**, **email**, **profile** (default is usually enough); grant admin consent.

---

## 2. Configure the gateway (web UI login) — #7

Add to `.env.localaws` (the gateway reads these):

```
AZURE_TENANT_ID=<tenant id>
AZURE_CLIENT_ID=<client id>
AZURE_CLIENT_SECRET=<client secret value>
GATEWAY_PUBLIC_URL=https://<gateway-host>      # redirect = <this>/auth/callback
SESSION_SECRET=<openssl rand -hex 32>
```

Rebuild + recreate the gateway, **run the migration** (adds the users/tokens tables), then:

```bash
docker compose -f docker-compose.localaws.yml build gateway
docker compose -f docker-compose.localaws.yml run --rm migrate
docker compose -f docker-compose.localaws.yml up -d --force-recreate gateway
```

**Bootstrap the first admin** — otherwise you lock yourself out (login is required, but the Admin tab that adds users itself needs an admin). Easiest: set
```
AUTH_BOOTSTRAP_ADMINS=you@org.com
```
in `.env.localaws` and recreate the gateway. That email is always allowed as admin and is saved into the users table on first sign-in; add everyone else from the **Admin** tab afterward. (Alternatively, insert the first row directly:
`docker compose ... exec metadata-postgres psql -U pipeline -d pipeline_metadata -c "INSERT INTO pipeline.pipeline_users(email,role,enabled) VALUES('you@org.com','admin',true) ON CONFLICT(email) DO UPDATE SET role='admin',enabled=true;"`)

Notes:
- The session cookie is `Secure`+`HttpOnly`+`SameSite=Lax`, so the gateway **must be reached over HTTPS** (terminate TLS at your proxy/load balancer in front of `:3000`).
- Viewers see only **Query / Runs / Guide**; admins see everything.

---

## 3. Protect Dagster (`:3001`) with oauth2-proxy — #6

Dagster OSS has no built-in auth, so put oauth2-proxy in front. Generate a cookie secret:
`openssl rand -base64 32 | tr '+/' '-_'` → `OAUTH2_PROXY_COOKIE_SECRET`.

**Stop publishing Dagster directly** — change the `dagster` service to expose only the internal port (remove the host mapping):

```yaml
  dagster:
    # ports:           # <-- remove the "3001:3000" mapping; oauth2-proxy fronts it
    #   - "3001:3000"
    expose:
      - "3000"
```

**Add the proxy service:**

```yaml
  dagster-auth:
    image: quay.io/oauth2-proxy/oauth2-proxy:v7.6.0
    restart: unless-stopped
    depends_on: [dagster]
    env_file: .env.localaws
    command:
      - --provider=oidc
      - --oidc-issuer-url=https://login.microsoftonline.com/${AZURE_TENANT_ID}/v2.0
      - --client-id=${AZURE_CLIENT_ID}
      - --client-secret=${AZURE_CLIENT_SECRET}
      - --redirect-url=${DAGSTER_PUBLIC_URL}/oauth2/callback
      - --upstream=http://dagster:3000
      - --http-address=0.0.0.0:4180
      - --cookie-secret=${OAUTH2_PROXY_COOKIE_SECRET}
      - --cookie-secure=true
      - --scope=openid email profile
      - --email-domain=*                       # or set your org domain to restrict
      - --pass-access-token=true
      - --set-xauthrequest=true
    ports:
      - "3001:4180"
```

Set `DAGSTER_PUBLIC_URL=https://<dagster-host>` in `.env.localaws`, then:

```bash
docker compose -f docker-compose.localaws.yml up -d dagster dagster-auth
```

Now `https://<dagster-host>` (mapped to `:3001`) requires Microsoft sign-in. To limit to specific people (not a whole domain), use `--authenticated-emails-file` mounted into the container instead of `--email-domain=*`.

> oauth2-proxy also needs HTTPS in front (for `--cookie-secure=true`). Terminate TLS at your proxy/LB ahead of `:3001`.

---

## 4. API tokens — #10

**Admin tab → API tokens**: name + email + **connection** + role → **Create token**. The token is shown **once** (only its SHA-256 hash is stored). Use it as a bearer token:

```bash
curl -H "Authorization: Bearer vmt_xxx" https://<gateway-host>/connections/<id>/sync-status
```

A token can only:
- act on **its own connection** (runs, selection, wipe, status) — others return 403;
- **query only its connection's namespace** (the SQL guard restricts schemas);
- do what its **role** allows — `viewer` is read/query only; `admin` can trigger syncs etc.

Revoke any token from the same tab (immediate).

---

## Summary of new env vars

| Var | Where | Purpose |
|---|---|---|
| `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` | gateway + oauth2-proxy | Azure AD app |
| `GATEWAY_PUBLIC_URL` | gateway | web-UI OAuth redirect base |
| `SESSION_SECRET` | gateway | signs the UI session cookie |
| `DAGSTER_PUBLIC_URL` | oauth2-proxy | Dagster OAuth redirect base |
| `OAUTH2_PROXY_COOKIE_SECRET` | oauth2-proxy | proxy cookie secret |
