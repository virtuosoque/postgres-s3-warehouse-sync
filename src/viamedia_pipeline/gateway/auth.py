"""JWT verification using a remote JWKS (e.g. Cognito, Okta, Auth0).

We cache JWKS in-process with a short TTL. For production, mount a sidecar
that pre-fetches and rotates JWKS files instead.
"""

import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
from jose.exceptions import JWTError

from viamedia_pipeline.common.settings import get_settings

_JWKS_CACHE: dict[str, tuple[float, dict]] = {}
_JWKS_TTL = 300.0

bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    sub: str
    roles: tuple[str, ...] = ()
    email: str = ""
    connection_id: int | None = None   # set for connection-scoped API tokens
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def is_admin(self) -> bool:
        return "admin" in self.roles


def _fetch_jwks(url: str) -> dict:
    now = time.time()
    cached = _JWKS_CACHE.get(url)
    if cached and (now - cached[0]) < _JWKS_TTL:
        return cached[1]
    with httpx.Client(timeout=5.0) as client:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()
    _JWKS_CACHE[url] = (now, data)
    return data


def verify_jwt(token: str) -> Principal:
    s = get_settings()
    if not s.gateway_jwks_url:
        # Local-dev shortcut -- decode without verification. NEVER do this in prod.
        try:
            payload = jwt.get_unverified_claims(token)
        except JWTError as e:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token") from e
        return Principal(sub=payload.get("sub", "dev"), roles=tuple(payload.get("roles", ())), raw=payload)

    jwks = _fetch_jwks(s.gateway_jwks_url)
    try:
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
        key = next((k for k in jwks["keys"] if k.get("kid") == kid), None)
        if not key:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "no matching JWK")
        payload = jwt.decode(
            token,
            key,
            algorithms=[key.get("alg", "RS256")],
            audience=s.gateway_jwt_audience,
            issuer=s.gateway_jwt_issuer,
        )
    except JWTError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid token: {e}") from e
    return Principal(
        sub=payload["sub"],
        roles=tuple(payload.get("roles", ())),
        raw=payload,
    )


def current_principal(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> Principal:
    s = get_settings()

    # 1) Connection-scoped API token (Authorization: Bearer vmt_...).
    if creds is not None:
        from viamedia_pipeline.common import config_store
        tok = config_store.verify_api_token(creds.credentials)
        if tok is not None:
            principal = Principal(
                sub=f"token:{tok['id']}", roles=(tok["role"],), email=tok["email"],
                connection_id=tok["connection_id"], raw={"token_id": tok["id"]},
            )
            request.state.principal = principal
            return principal

    # 2) Microsoft (Azure AD) session cookie set by the OAuth login flow.
    sess = session_principal(request)
    if sess is not None:
        request.state.principal = sess
        return sess

    # 3) Bearer JWT verified against a configured OIDC issuer/JWKS.
    if creds is not None and (s.gateway_jwks_url or s.gateway_jwt_issuer):
        principal = verify_jwt(creds.credentials)
        request.state.principal = principal
        return principal

    # 4) No auth configured -> open mode: anonymous acts as admin (preserves the
    #    pre-auth behavior). Once Microsoft login is enabled, browser requests
    #    without a valid session are rejected (handled in session_principal/UI).
    auth_on = bool(s.gateway_jwks_url or s.gateway_jwt_issuer) or _microsoft_login_enabled()
    if not auth_on:
        principal = Principal(sub="anonymous", roles=("admin",), raw={})
        request.state.principal = principal
        return principal
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, "authentication required")


def _microsoft_login_enabled() -> bool:
    import os
    return bool(os.environ.get("AZURE_CLIENT_ID") and os.environ.get("AZURE_TENANT_ID"))


def session_principal(request: Request) -> Principal | None:
    """Resolve a Principal from the signed Microsoft-login session cookie, or
    None. Implemented by the OAuth module to avoid an import cycle; patched in at
    import time. Returns None when Microsoft login isn't enabled."""
    fn = globals().get("_session_resolver")
    return fn(request) if fn else None


def require_admin(principal: Principal = Depends(current_principal)) -> Principal:
    """Dependency for admin-only (mutating) endpoints. Viewers get 403."""
    if not principal.is_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "admin role required")
    return principal


def enforce_connection_scope(principal: Principal, connection_id: int) -> None:
    """A connection-scoped API token may only act on its own connection."""
    if principal.connection_id is not None and principal.connection_id != connection_id:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"this token is scoped to connection {principal.connection_id}",
        )
