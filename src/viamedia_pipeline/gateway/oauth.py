"""Microsoft (Azure AD) OAuth2 login for the web UI (#7).

Flow: /auth/login -> Microsoft authorize -> /auth/callback (code exchange) ->
check the email against the allow-list (pipeline_users) -> set a signed session
cookie carrying {email, role}. `session_principal` (registered into auth.py)
resolves that cookie back to a Principal.

Enabled only when AZURE_CLIENT_ID + AZURE_TENANT_ID are set. Required env:
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
    GATEWAY_PUBLIC_URL   e.g. https://lake.example.com   (for the redirect URI)
    SESSION_SECRET       random secret used to sign the session cookie
"""

import os
import secrets
import time

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from jose import jwt
from jose.exceptions import JWTError

from viamedia_pipeline.common import config_store
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.gateway import auth as auth_mod
from viamedia_pipeline.gateway.auth import Principal

log = get_logger(__name__)

router = APIRouter()

_SESSION_COOKIE = "vm_session"
_STATE_COOKIE = "vm_oauthstate"
_SESSION_TTL = 8 * 3600  # 8h


def _enabled() -> bool:
    return bool(os.environ.get("AZURE_CLIENT_ID") and os.environ.get("AZURE_TENANT_ID"))


def _secret() -> str:
    s = os.environ.get("SESSION_SECRET")
    if not s:
        raise HTTPException(500, "SESSION_SECRET is not configured")
    return s


def _public_url() -> str:
    return (os.environ.get("GATEWAY_PUBLIC_URL") or "").rstrip("/")


def _redirect_uri() -> str:
    return f"{_public_url()}/auth/callback"


def _authority() -> str:
    return f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}"


@router.get("/auth/login")
def login(request: Request):
    if not _enabled():
        raise HTTPException(400, "Microsoft login is not configured")
    state = secrets.token_urlsafe(24)
    params = {
        "client_id": os.environ["AZURE_CLIENT_ID"],
        "response_type": "code",
        "redirect_uri": _redirect_uri(),
        "response_mode": "query",
        "scope": "openid email profile",
        "state": state,
    }
    url = f"{_authority()}/oauth2/v2.0/authorize?" + "&".join(
        f"{k}={httpx.QueryParams({k: v})[k]}" for k, v in params.items()
    )
    resp = RedirectResponse(url, status_code=302)
    resp.set_cookie(_STATE_COOKIE, state, max_age=600, httponly=True,
                    secure=True, samesite="lax")
    return resp


@router.get("/auth/callback")
def callback(request: Request, code: str | None = None, state: str | None = None):
    if not _enabled():
        raise HTTPException(400, "Microsoft login is not configured")
    if not code or not state or state != request.cookies.get(_STATE_COOKIE):
        raise HTTPException(400, "invalid OAuth state")

    with httpx.Client(timeout=15.0) as client:
        tok = client.post(
            f"{_authority()}/oauth2/v2.0/token",
            data={
                "client_id": os.environ["AZURE_CLIENT_ID"],
                "client_secret": os.environ["AZURE_CLIENT_SECRET"],
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": _redirect_uri(),
                "scope": "openid email profile",
            },
        )
    if tok.status_code != 200:
        log.warning("oauth.token_exchange_failed", status=tok.status_code, body=tok.text[:300])
        raise HTTPException(401, "token exchange failed")
    id_token = tok.json().get("id_token")
    if not id_token:
        raise HTTPException(401, "no id_token returned")
    claims = jwt.get_unverified_claims(id_token)  # straight from MS over TLS
    email = (claims.get("email") or claims.get("preferred_username") or claims.get("upn") or "").lower()
    if not email:
        raise HTTPException(401, "no email claim in token")

    user = config_store.get_user(email)
    if user is None:
        # Bootstrap escape hatch: emails in AUTH_BOOTSTRAP_ADMINS are always
        # allowed as admin (and persisted on first login), so a fresh deployment
        # with an empty users table isn't locked out of its own Admin tab.
        bootstrap = {e.strip().lower() for e in
                     os.environ.get("AUTH_BOOTSTRAP_ADMINS", "").split(",") if e.strip()}
        if email in bootstrap:
            config_store.upsert_user(email, "admin", True)
            user = {"email": email, "role": "admin"}
            log.info("oauth.bootstrap_admin", email=email)
        else:
            log.warning("oauth.user_not_allowed", email=email)
            return JSONResponse(status_code=403, content={
                "detail": f"{email} is not allowed. Ask an admin to add you under the Admin tab."})

    session = jwt.encode(
        {"email": email, "role": user["role"], "exp": int(time.time()) + _SESSION_TTL},
        _secret(), algorithm="HS256",
    )
    resp = RedirectResponse("/", status_code=302)
    resp.set_cookie(_SESSION_COOKIE, session, max_age=_SESSION_TTL, httponly=True,
                    secure=True, samesite="lax")
    resp.delete_cookie(_STATE_COOKIE)
    log.info("oauth.login", email=email, role=user["role"])
    return resp


@router.get("/auth/logout")
def logout():
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie(_SESSION_COOKIE)
    return resp


@router.get("/auth/me")
def me(request: Request):
    p = session_principal(request)
    return {
        "enabled": _enabled(),
        "authenticated": p is not None,
        "email": p.email if p else None,
        "role": (p.roles[0] if p and p.roles else None),
    }


def session_principal(request: Request) -> Principal | None:
    if not _enabled():
        return None
    raw = request.cookies.get(_SESSION_COOKIE)
    if not raw:
        return None
    try:
        claims = jwt.decode(raw, _secret(), algorithms=["HS256"])
    except JWTError:
        return None
    return Principal(sub=claims["email"], roles=(claims.get("role", "viewer"),),
                     email=claims["email"], raw={"src": "session"})


# Register the resolver so auth.current_principal can use it without an import cycle.
auth_mod._session_resolver = session_principal
