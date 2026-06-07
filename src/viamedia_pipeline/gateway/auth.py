"""JWT verification using a remote JWKS (e.g. Cognito, Okta, Auth0).

We cache JWKS in-process with a short TTL. For production, mount a sidecar
that pre-fetches and rotates JWKS files instead.
"""

import time
from dataclasses import dataclass
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
    roles: tuple[str, ...]
    raw: dict[str, Any]


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
    if creds is None:
        # No token supplied. Allowed only when no IdP is configured (local dev);
        # in that mode every caller is an anonymous principal. With an issuer or
        # JWKS configured we still require a valid bearer token.
        if not s.gateway_jwks_url and not s.gateway_jwt_issuer:
            principal = Principal(sub="anonymous", roles=(), raw={})
            request.state.principal = principal
            return principal
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    principal = verify_jwt(creds.credentials)
    # Attach to request state so logs / rate-limit keys can use it
    request.state.principal = principal
    return principal
