"""
MyTube Backend v2 — FastAPI + yt-dlp + InnerTube + Google OAuth2

Streaming : proxy byte-range transparent.
  YouTube CDN → backend → navigateur (par chunks de 64 KB).
  Les URLs YouTube sont liées à l'IP du serveur qui les extrait :
  le proxy est donc obligatoire — la vidéo n'est PAS téléchargée
  avant d'être envoyée, elle transite en temps réel.
"""

import asyncio
import os
import random
import string
import time
import uuid
import secrets
import httpx
import aiosqlite
import base64
import hashlib
import json
from collections import OrderedDict
from fastapi import FastAPI, HTTPException, Query, Request, Response, Cookie
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from cryptography.fernet import Fernet
import yt_dlp

app = FastAPI(title="MyTube API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# ── Config ────────────────────────────────────────────────────────────────────
POT_PROVIDER_URL = os.getenv("POT_PROVIDER_URL", "http://pot-provider:4416")
SPONSORBLOCK_API = os.getenv("SPONSORBLOCK_API", "https://sponsor.ajay.app")
SESSION_SECRET   = os.getenv("SESSION_SECRET", secrets.token_hex(32))
PROXY_URL        = os.getenv("PROXY_URL", "").strip() or None
# Credentials OAuth Google — créer les vôtres sur console.cloud.google.com
# Type : "TV and Limited Input Devices" + scope youtube.readonly
# Laisser vide pour utiliser les credentials publics (peuvent être révoqués).
YOUTUBE_CLIENT_ID     = os.getenv("YOUTUBE_CLIENT_ID",     "").strip() or None
YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET", "").strip() or None
DB_PATH = os.getenv("DB_PATH", "/app/data/mytube.db")

# ── Helpers chiffrement ───────────────────────────────────────────────────────
def _fernet() -> Fernet:
    key = base64.urlsafe_b64encode(hashlib.sha256(SESSION_SECRET.encode()).digest())
    return Fernet(key)

def _encrypt(data: str) -> bytes:
    return _fernet().encrypt(data.encode())

def _decrypt(data: bytes) -> str:
    return _fernet().decrypt(data).decode()

# ── Sessions (RAM + SQLite pour persistance multi-redémarrage) ────────────────
_sessions: dict[str, dict] = {}

async def _db() -> aiosqlite.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return await aiosqlite.connect(DB_PATH)

async def _init_db() -> None:
    async with await _db() as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                avatar TEXT DEFAULT '',
                auth_method TEXT DEFAULT 'cookies',
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS user_cookies (
                user_id TEXT PRIMARY KEY,
                cookies_json_enc BLOB NOT NULL,
                cookies_txt_enc  BLOB NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                expires_at REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        """)
        await db.commit()

async def _get_session(sid: str | None) -> dict | None:
    if not sid:
        return None
    # Chemin rapide : RAM
    s = _sessions.get(sid)
    if s and s.get("expires_at", 0) > time.time():
        return s
    if s:
        _sessions.pop(sid, None)
    # Chemin lent : DB (redémarrage container)
    try:
        async with await _db() as db:
            async with db.execute(
                "SELECT user_id, expires_at FROM sessions WHERE session_id=?", (sid,)
            ) as cur:
                row = await cur.fetchone()
            if not row or row[1] < time.time():
                return None
            user_id, expires_at = row
            async with db.execute(
                "SELECT name, avatar, auth_method FROM users WHERE id=?", (user_id,)
            ) as cur:
                urow = await cur.fetchone()
            if not urow:
                return None
            async with db.execute(
                "SELECT cookies_json_enc, cookies_txt_enc FROM user_cookies WHERE user_id=?",
                (user_id,)
            ) as cur:
                crow = await cur.fetchone()
        cookies: dict = {}
        cookies_file: str = ""
        if crow and crow[0]:
            try:
                cookies = json.loads(_decrypt(crow[0]))
                # Écrire le fichier cookies.txt pour yt-dlp
                cookies_file = await _write_cookies_file(user_id, _decrypt(crow[1]))
            except Exception:
                pass
        s = {
            "user":         {"id": user_id, "name": urow[0], "email": "", "picture": urow[1]},
            "auth_method":  urow[2],
            "cookies":      cookies,
            "cookies_file": cookies_file,
            "access_token": "",
            "expires_at":   expires_at,
        }
        _sessions[sid] = s
        return s
    except Exception:
        return None

async def _save_session_db(sid: str, user_id: str, expires_at: float) -> None:
    async with await _db() as db:
        await db.execute(
            "INSERT OR REPLACE INTO sessions (session_id, user_id, expires_at) VALUES (?,?,?)",
            (sid, user_id, expires_at)
        )
        await db.commit()

async def _delete_session_db(sid: str) -> None:
    async with await _db() as db:
        await db.execute("DELETE FROM sessions WHERE session_id=?", (sid,))
        await db.commit()

# ── Cache URL flux ────────────────────────────────────────────────────────────
# iOS envoie 3-4 requêtes Range par lecture ; le cache évite de relancer yt-dlp.
_stream_cache: dict[str, tuple[str, float]] = {}

def _cache_get(video_id: str, quality: int) -> str | None:
    e = _stream_cache.get(f"{video_id}_{quality}")
    return e[0] if e and time.time() < e[1] else None

def _cache_set(video_id: str, quality: int, url: str) -> None:
    _stream_cache[f"{video_id}_{quality}"] = (url, time.time() + 3600)

def _cache_del(video_id: str, quality: int) -> None:
    _stream_cache.pop(f"{video_id}_{quality}", None)

# ── AXE 3 : Cache pot token (TTL 1h, lock async) ─────────────────────────────
# Évite une requête pot-provider (~400ms) à chaque lecture.
# Le lock empêche plusieurs requêtes simultanées de refetch en parallèle.
_pot_lock  = asyncio.Lock()
_pot_store: dict = {"token": {}, "expires_at": 0.0}

async def get_pot(video_id: str = "dQw4w9WgXcQ") -> dict:
    if time.time() < _pot_store["expires_at"] and _pot_store["token"]:
        return _pot_store["token"]
    async with _pot_lock:
        if time.time() < _pot_store["expires_at"] and _pot_store["token"]:
            return _pot_store["token"]
        tok = await fetch_pot_token(video_id)
        if tok.get("po_token"):
            _pot_store["token"]     = tok
            _pot_store["expires_at"] = time.time() + 3600
        return tok

def _pot_invalidate() -> None:
    _pot_store["expires_at"] = 0.0

# ── AXE 2 : Hot-cache premiers chunks (LRU, 50 entrées, 512 KB chacune) ──────
# Objectif : démarrage instantané (zero-lag) — les premiers octets de la vidéo
# sont déjà en RAM à la 2e lecture ou lorsque plusieurs clients regardent la
# même vidéo.
_HOT_BYTES = 512 * 1024  # 512 KB

class _LRU:
    def __init__(self, n: int = 50):
        self._c: OrderedDict = OrderedDict()
        self._n = n
    def get(self, k: str) -> bytes | None:
        if k in self._c:
            self._c.move_to_end(k)
            return self._c[k]
        return None
    def put(self, k: str, v: bytes) -> None:
        self._c[k] = v
        self._c.move_to_end(k)
        if len(self._c) > self._n:
            self._c.popitem(last=False)

_hot = _LRU(50)

# ── Helpers yt-dlp ────────────────────────────────────────────────────────────
def get_ydl_opts(extra: dict = {}, cookies_file: str = "") -> dict:
    opts: dict = {"quiet": True, "no_warnings": True, "extract_flat": False,
                  "nocheckcertificate": True}
    if PROXY_URL:
        opts["proxy"] = PROXY_URL
    if cookies_file and os.path.exists(cookies_file):
        opts["cookiefile"] = cookies_file
    return {**opts, **extra}

def _yt_client(**kw) -> httpx.AsyncClient:
    """Client httpx pour les requêtes YouTube — passe par le proxy VPN si configuré."""
    if PROXY_URL:
        kw["proxy"] = PROXY_URL
    return httpx.AsyncClient(**kw)

async def fetch_pot_token(video_id: str = "dQw4w9WgXcQ") -> dict:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{POT_PROVIDER_URL}/get_pot", json={"videoId": video_id})
            if r.status_code == 200:
                d = r.json()
                return {"po_token": d.get("potoken", ""), "visitor_data": d.get("visitorData", "")}
    except Exception:
        pass
    return {}

def _ios_format(quality: int) -> str:
    return (
        f"bestvideo[height<={quality}][vcodec^=avc][ext=mp4]"
        f"+bestaudio[acodec^=mp4a][ext=m4a]"
        f"/best[height<={quality}][vcodec^=avc][ext=mp4]"
        f"/best[height<={quality}][ext=mp4]/best"
    )

def _pot_args(pot: dict) -> dict:
    if not pot.get("po_token"):
        return {}
    return {"extractor_args": {"youtube": {
        "po_token": [f"web+{pot['po_token']}"],
        "visitor_data": [pot["visitor_data"]],
        "player_client": ["web"],
    }}}

def _cookie_opts(cookies_file: str) -> dict:
    """Options yt-dlp pour utiliser les cookies de session YouTube."""
    if cookies_file and os.path.exists(cookies_file):
        return {"cookiefile": cookies_file}
    return {}

def _fmt_entry(e: dict) -> dict:
    vid = e.get("id", "")
    return {
        "id": vid,
        "title": e.get("title", ""),
        "channel": e.get("channel") or e.get("uploader", ""),
        "duration": e.get("duration"),
        "thumbnail": e.get("thumbnail") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "views": e.get("view_count"),
        "published": e.get("upload_date", ""),
    }

# ── AXE 1 : InnerTube ────────────────────────────────────────────────────────
# Deux clients :
#   WEB      — requêtes anonymes (trending, recherche, related sans compte)
#   TVHTML5  — requêtes authentifiées (feed perso, related avec compte)
#              correspond exactement au device code flow utilisé pour l'auth.
_IT_BASE    = "https://www.youtube.com/youtubei/v1"
_IT_VER_WEB = "2.20250526.01.00"
_IT_VER_TV  = "7.20250526.00.00"

_IT_CTX_WEB = {"client": {
    "clientName": "WEB", "clientVersion": _IT_VER_WEB,
    "hl": "fr", "gl": "FR", "platform": "DESKTOP",
}}
_IT_CTX_TV  = {"client": {
    "clientName": "TVHTML5", "clientVersion": _IT_VER_TV,
    "hl": "fr", "gl": "FR", "platform": "TV",
}}

def _it_ctx(token: str = "", cookies: dict | None = None) -> dict:
    # Cookies réels = WEB client (structure familière + pleine personnalisation)
    # OAuth TV token = TVHTML5
    # Anonyme = WEB
    if cookies:
        return _IT_CTX_WEB
    return _IT_CTX_TV if token else _IT_CTX_WEB

def _sapisid_hash(cookies: dict) -> str | None:
    """Génère l'Authorization InnerTube depuis les cookies de session YouTube."""
    sapisid = (cookies.get("__Secure-3PAPISID")
               or cookies.get("SAPISID")
               or cookies.get("__Secure-1PAPISID", ""))
    if not sapisid:
        return None
    ts = int(time.time())
    digest = hashlib.sha1(
        f"{ts} {sapisid} https://www.youtube.com".encode()
    ).hexdigest()
    return f"SAPISIDHASH {ts}_{digest}"

async def _write_cookies_file(user_id: str, cookies_txt: str) -> str:
    """Écrit le fichier Netscape cookies.txt pour yt-dlp. Retourne le chemin."""
    path = f"/app/data/cookies"
    os.makedirs(path, exist_ok=True)
    fpath = f"{path}/{user_id}.txt"
    with open(fpath, "w") as f:
        f.write(cookies_txt)
    return fpath

def _it_headers(token: str = "", cookies: dict | None = None) -> dict:
    """Headers InnerTube.
    Priorité : cookies YouTube réels (SAPISIDHASH) > OAuth Bearer > anonyme.
    """
    if cookies:
        h = {
            "Content-Type": "application/json",
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/126.0.0.0 Safari/537.36"),
            "X-YouTube-Client-Name": "1",
            "X-YouTube-Client-Version": _IT_VER_WEB,
            "Origin": "https://www.youtube.com",
            "Referer": "https://www.youtube.com/",
            "X-Origin": "https://www.youtube.com",
            "Cookie": "; ".join(f"{k}={v}" for k, v in cookies.items()),
        }
        auth = _sapisid_hash(cookies)
        if auth:
            h["Authorization"] = auth
        return h
    if token:
        return {
            "Content-Type": "application/json",
            "User-Agent": ("Mozilla/5.0 (SMART-TV; LINUX; Tizen 6.0) "
                           "AppleWebKit/538.1 (KHTML, like Gecko) "
                           "Version/6.0 TV Safari/538.1"),
            "X-YouTube-Client-Name": "7",
            "X-YouTube-Client-Version": _IT_VER_TV,
            "Authorization": f"Bearer {token}",
            "Origin": "https://www.youtube.com",
            "Referer": "https://www.youtube.com/",
        }
    return {
        "Content-Type": "application/json",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/126.0.0.0 Safari/537.36"),
        "X-YouTube-Client-Name": "1",
        "X-YouTube-Client-Version": _IT_VER_WEB,
        "Origin": "https://www.youtube.com",
        "Referer": "https://www.youtube.com/",
    }

def _parse_renderer(r: dict) -> dict | None:
    vid = r.get("videoId")
    if not vid:
        return None
    title = (r.get("title", {}).get("simpleText")
             or (r.get("title", {}).get("runs") or [{}])[0].get("text", ""))
    ch = ((r.get("longBylineText") or r.get("shortBylineText") or {})
          .get("runs") or [{}])[0].get("text", "")
    dur_txt = (r.get("lengthText") or {}).get("simpleText", "")
    thumbs = r.get("thumbnail", {}).get("thumbnails", [])
    thumb = thumbs[-1]["url"] if thumbs else f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
    dur = None
    if dur_txt:
        try:
            p = dur_txt.split(":")
            dur = (int(p[0])*3600+int(p[1])*60+int(p[2]) if len(p)==3
                   else int(p[0])*60+int(p[1]))
        except Exception:
            pass
    return {"id": vid, "title": title, "channel": ch, "duration": dur, "thumbnail": thumb,
            "views_text": (r.get("viewCountText") or {}).get("simpleText", "")}

# Parcours récursif : trouve tous les videoRenderer/compactVideoRenderer
# quelle que soit la profondeur — résistant aux changements de structure YouTube.
_VIDEO_RENDERER_KEYS = ("videoRenderer", "gridVideoRenderer", "compactVideoRenderer")

def _collect_renderers(obj, out: list, limit: int = 60) -> None:
    if len(out) >= limit:
        return
    if isinstance(obj, dict):
        for key in _VIDEO_RENDERER_KEYS:
            if key in obj:
                v = _parse_renderer(obj[key])
                if v:
                    out.append(v)
                return
        for val in obj.values():
            _collect_renderers(val, out, limit)
    elif isinstance(obj, list):
        for item in obj:
            _collect_renderers(item, out, limit)

async def _it_next(video_id: str, token: str = "", cookies: dict | None = None) -> list[dict]:
    """Recommandations InnerTube — sidebar 'À suivre' de YouTube."""
    try:
        async with _yt_client(timeout=10) as c:
            r = await c.post(f"{_IT_BASE}/next",
                             json={"videoId": video_id, "context": _it_ctx(token, cookies)},
                             headers=_it_headers(token, cookies))
        data = r.json()
        secondary = (data
                     .get("contents", {})
                     .get("twoColumnWatchNextResults", {})
                     .get("secondaryResults", {})
                     .get("secondaryResults", {})
                     .get("results", []))
        out: list[dict] = []
        _collect_renderers(secondary, out, limit=20)
        # Fallback : parcours complet si la structure a changé
        if not out:
            _collect_renderers(data, out, limit=20)
        return [v for v in out if v["id"] != video_id]
    except Exception:
        return []

async def _it_browse(browse_id: str, token: str = "", cookies: dict | None = None) -> list[dict]:
    """Browse InnerTube : trending (FEtrending), accueil perso (FEwhat_to_watch)…"""
    try:
        async with _yt_client(timeout=12) as c:
            r = await c.post(f"{_IT_BASE}/browse",
                             json={"browseId": browse_id, "context": _it_ctx(token, cookies)},
                             headers=_it_headers(token, cookies))
        data = r.json()
        # Essai structure connue d'abord (plus rapide)
        contents = (data
                    .get("contents", {})
                    .get("twoColumnBrowseResultsRenderer", {})
                    .get("tabs", [{}])[0]
                    .get("tabRenderer", {})
                    .get("content", {})
                    .get("richGridRenderer", {})
                    .get("contents", []))
        out: list[dict] = []
        _collect_renderers(contents, out, limit=40)
        # Fallback : parcours complet si structure inconnue ou vide
        if not out:
            _collect_renderers(data, out, limit=40)
        return out
    except Exception:
        return []

# ── Auth Google — Device Code Flow ───────────────────────────────────────────
# Utilise les credentials fournis dans l'env, sinon tente les credentials
# publics YouTube TV (peuvent être révoqués par Google à tout moment).
_YTV_ID     = (YOUTUBE_CLIENT_ID
               or "861556708454-d6dlm3lh05idd8npek18k6be8ba3oc68.apps.googleusercontent.com")
_YTV_SECRET = (YOUTUBE_CLIENT_SECRET or "SboVhoG9s0rNafixCSGGKXAT")
_YTV_SCOPE  = "https://www.googleapis.com/auth/youtube.readonly"
_AUTH_CONFIGURED = bool(YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET)

_G_DEVICE = "https://oauth2.googleapis.com/device/code"
_G_TOKEN  = "https://oauth2.googleapis.com/token"
_YT_API   = "https://www.googleapis.com/youtube/v3"

# poll_id → {device_code, expires_at}
_pending_devices: dict[str, dict] = {}

# ── Startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    os.makedirs("/app/data", exist_ok=True)
    os.makedirs("/app/data/cookies", exist_ok=True)
    await _init_db()

# ── Routes auth Device Code Flow ──────────────────────────────────────────────
@app.get("/auth/device/start")
async def auth_device_start():
    """Lance le device code flow : retourne le code à saisir sur google.com/device."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(_G_DEVICE, data={"client_id": _YTV_ID, "scope": _YTV_SCOPE})
        d = r.json()
        if "error" in d:
            raise HTTPException(500, d.get("error_description", d["error"]))
        poll_id = str(uuid.uuid4())
        _pending_devices[poll_id] = {
            "device_code": d["device_code"],
            "expires_at":  time.time() + d.get("expires_in", 1800),
        }
        return {
            "poll_id":          poll_id,
            "user_code":        d["user_code"],
            "verification_url": d.get("verification_url", "https://www.google.com/device"),
            "expires_in":       d.get("expires_in", 1800),
            "interval":         d.get("interval", 5),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/auth/device/poll")
async def auth_device_poll(poll_id: str, response: Response):
    """Vérifie si l'utilisateur a validé la connexion sur Google."""
    pending = _pending_devices.get(poll_id)
    if not pending or time.time() > pending["expires_at"]:
        _pending_devices.pop(poll_id, None)
        return {"status": "expired"}
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            t = (await c.post(_G_TOKEN, data={
                "client_id":     _YTV_ID,
                "client_secret": _YTV_SECRET,
                "device_code":   pending["device_code"],
                "grant_type":    "urn:ietf:params:oauth2:grant-type:device_code",
            })).json()
        err = t.get("error")
        if err in ("authorization_pending", "slow_down"):
            return {"status": "pending"}
        if err:
            _pending_devices.pop(poll_id, None)
            return {"status": "error", "message": t.get("error_description", err)}

        access_token = t.get("access_token", "")
        user = {"id": "", "name": "Utilisateur YouTube", "email": "", "picture": ""}
        try:
            # Le client YouTube TV ne peut pas appeler userinfo Google.
            # On récupère nom + avatar depuis l'API YouTube Channels (mine=true).
            async with httpx.AsyncClient(timeout=5) as c:
                ch = (await c.get(f"{_YT_API}/channels",
                    params={"part": "snippet", "mine": "true"},
                    headers={"Authorization": f"Bearer {access_token}"}
                )).json()
            items = ch.get("items", [])
            if items:
                snippet = items[0].get("snippet", {})
                thumbs  = snippet.get("thumbnails", {})
                thumb   = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
                user = {"id": items[0].get("id", ""), "name": snippet.get("title", ""),
                        "email": "", "picture": thumb}
        except Exception:
            pass

        user_id = user.get("id") or str(uuid.uuid4())
        expires_at = time.time() + 86400 * 30
        sid = str(uuid.uuid4())
        s = {
            "user":          user,
            "auth_method":   "oauth",
            "cookies":       {},
            "cookies_file":  "",
            "access_token":  access_token,
            "refresh_token": t.get("refresh_token", ""),
            "expires_at":    expires_at,
        }
        _sessions[sid] = s

        # Sauvegarder en DB pour persistance
        try:
            async with await _db() as db:
                await db.execute(
                    "INSERT OR REPLACE INTO users (id, name, avatar, auth_method, created_at) "
                    "VALUES (?,?,?,?,?)",
                    (user_id, user["name"], user["picture"], "oauth", time.time())
                )
                await db.commit()
            await _save_session_db(sid, user_id, expires_at)
        except Exception:
            pass

        _pending_devices.pop(poll_id, None)
        response.set_cookie("session_id", sid, httponly=True,
                            max_age=86400*30, samesite="lax", path="/")
        return {"status": "authorized", "user": user}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/auth/status")
async def auth_status(session_id: str = Cookie(default=None)):
    s = await _get_session(session_id)
    users_count = 0
    try:
        async with await _db() as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                row = await cur.fetchone()
                users_count = row[0] if row else 0
    except Exception:
        pass
    return {
        "authenticated":   bool(s),
        "user":            s["user"] if s else None,
        "auth_method":     s.get("auth_method") if s else None,
        "auth_configured": _AUTH_CONFIGURED,
        "users_count":     users_count,
    }

@app.post("/auth/logout")
async def auth_logout(response: Response, session_id: str = Cookie(default=None)):
    if session_id:
        _sessions.pop(session_id, None)
        try:
            await _delete_session_db(session_id)
        except Exception:
            pass
    response.delete_cookie("session_id", path="/")
    return {"ok": True}

# ── Nouveaux endpoints auth cookies ──────────────────────────────────────────
@app.post("/auth/import-cookies")
async def import_cookies(request: Request, response: Response):
    """
    Importe les cookies YouTube d'un utilisateur (format Netscape cookies.txt).
    C'est la méthode ProTube : utilise la vraie session YouTube du navigateur.
    → Recommandations, historique, feed 100% identiques à YouTube.

    Comment exporter les cookies :
    1. Chrome/Firefox : installer l'extension "Get cookies.txt LOCALLY"
    2. Aller sur youtube.com en étant connecté
    3. Cliquer sur l'extension → exporter → copier le contenu
    4. Coller dans ce champ
    """
    body = await request.json()
    cookies_txt: str = body.get("cookies_txt", "").strip()
    if not cookies_txt:
        raise HTTPException(400, "cookies_txt manquant")

    # Parser le format Netscape cookies.txt → dict
    cookies: dict = {}
    for line in cookies_txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 7:
            name, value = parts[5], parts[6]
            cookies[name] = value

    # Vérifier la présence des cookies essentiels
    essential = {"SID", "HSID", "SSID", "SAPISID", "__Secure-3PAPISID"}
    found = essential & set(cookies.keys())
    if len(found) < 2:
        raise HTTPException(400,
            f"Cookies YouTube invalides ou incomplets. "
            f"Trouvés : {list(cookies.keys())[:10]}. "
            f"Attendus : SID, HSID, SSID, SAPISID, __Secure-3PAPISID")

    # Extraire le nom d'utilisateur via InnerTube avec ces cookies
    user_name   = "Utilisateur YouTube"
    user_avatar = ""
    user_id     = str(uuid.uuid4())
    try:
        async with _yt_client(timeout=8) as c:
            r = await c.post(f"{_IT_BASE}/account/accountsoverview",
                             json={"context": _it_ctx(cookies=cookies)},
                             headers=_it_headers(cookies=cookies))
        data = r.json()
        # Essayer de récupérer le nom depuis accountsoverview
        for acc in (data.get("contents", {})
                        .get("multiPageMenuRenderer", {})
                        .get("sections", [{}])[0]
                        .get("multiPageMenuSectionRenderer", {})
                        .get("items", [])):
            compact = acc.get("compactLinkRenderer", {})
            if "serviceEndpoint" in compact:
                continue
            title = compact.get("title", {}).get("simpleText", "")
            if title:
                user_name = title
                icon = compact.get("icon", {}).get("thumbnails", [{}])
                if icon:
                    user_avatar = icon[-1].get("url", "")
                break
    except Exception:
        pass

    # Fallback : essayer /api/account/account_menu
    if user_name == "Utilisateur YouTube":
        try:
            async with _yt_client(timeout=8) as c:
                r2 = await c.post(f"{_IT_BASE}/account/account_menu",
                                  json={"context": _IT_CTX_WEB},
                                  headers=_it_headers(cookies=cookies))
            data2 = r2.json()
            out: list = []
            _collect_renderers(data2, out, 3)
        except Exception:
            pass

    # Sauvegarder en DB
    expires_at = time.time() + 86400 * 365  # cookies valides 1 an
    cookies_file = await _write_cookies_file(user_id, cookies_txt)
    async with await _db() as db:
        await db.execute(
            "INSERT OR REPLACE INTO users (id, name, avatar, auth_method, created_at) "
            "VALUES (?,?,?,?,?)",
            (user_id, user_name, user_avatar, "cookies", time.time())
        )
        await db.execute(
            "INSERT OR REPLACE INTO user_cookies (user_id, cookies_json_enc, cookies_txt_enc, updated_at) "
            "VALUES (?,?,?,?)",
            (user_id, _encrypt(json.dumps(cookies)), _encrypt(cookies_txt), time.time())
        )
        await db.commit()

    sid = str(uuid.uuid4())
    s = {
        "user":         {"id": user_id, "name": user_name, "email": "", "picture": user_avatar},
        "auth_method":  "cookies",
        "cookies":      cookies,
        "cookies_file": cookies_file,
        "access_token": "",
        "expires_at":   expires_at,
    }
    _sessions[sid] = s
    await _save_session_db(sid, user_id, expires_at)
    response.set_cookie("session_id", sid, httponly=True,
                        max_age=86400*365, samesite="lax", path="/")
    return {"status": "ok", "user": s["user"]}


@app.get("/auth/users")
async def list_users():
    """Liste tous les comptes sauvegardés (pour le sélecteur multi-compte)."""
    try:
        async with await _db() as db:
            async with db.execute(
                "SELECT id, name, avatar, auth_method, created_at FROM users ORDER BY created_at DESC"
            ) as cur:
                rows = await cur.fetchall()
        return {"users": [
            {"id": r[0], "name": r[1], "avatar": r[2], "auth_method": r[3]}
            for r in rows
        ]}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/auth/switch/{user_id}")
async def switch_user(user_id: str, response: Response):
    """Bascule sur un compte déjà sauvegardé."""
    try:
        async with await _db() as db:
            async with db.execute(
                "SELECT name, avatar FROM users WHERE id=?", (user_id,)
            ) as cur:
                urow = await cur.fetchone()
            if not urow:
                raise HTTPException(404, "Compte introuvable")
            async with db.execute(
                "SELECT cookies_json_enc, cookies_txt_enc FROM user_cookies WHERE user_id=?",
                (user_id,)
            ) as cur:
                crow = await cur.fetchone()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

    cookies: dict = {}
    cookies_file: str = ""
    if crow and crow[0]:
        try:
            cookies = json.loads(_decrypt(crow[0]))
            cookies_file = await _write_cookies_file(user_id, _decrypt(crow[1]))
        except Exception:
            pass

    expires_at = time.time() + 86400 * 365
    sid = str(uuid.uuid4())
    s = {
        "user":         {"id": user_id, "name": urow[0], "email": "", "picture": urow[1]},
        "auth_method":  "cookies",
        "cookies":      cookies,
        "cookies_file": cookies_file,
        "access_token": "",
        "expires_at":   expires_at,
    }
    _sessions[sid] = s
    await _save_session_db(sid, user_id, expires_at)
    response.set_cookie("session_id", sid, httponly=True,
                        max_age=86400*365, samesite="lax", path="/")
    return {"status": "ok", "user": s["user"]}


@app.delete("/auth/users/{user_id}")
async def delete_user(user_id: str, response: Response,
                      session_id: str = Cookie(default=None)):
    """Supprime un compte et ses cookies."""
    async with await _db() as db:
        await db.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM user_cookies WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM users WHERE id=?", (user_id,))
        await db.commit()
    # Supprimer le fichier cookies
    fpath = f"/app/data/cookies/{user_id}.txt"
    if os.path.exists(fpath):
        os.unlink(fpath)
    # Vider la session courante si c'est ce user
    s = _sessions.get(session_id or "")
    if s and s.get("user", {}).get("id") == user_id:
        _sessions.pop(session_id, None)
        response.delete_cookie("session_id", path="/")
    return {"ok": True}

# ── Routes principales ────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "service": "MyTube API", "version": "2.0.0"}

@app.get("/api/search")
async def search(q: str = Query(..., min_length=1), limit: int = 20):
    try:
        with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True})) as ydl:
            info = ydl.extract_info(f"ytsearch{limit}:{q}", download=False)
        return {"results": [_fmt_entry(e) for e in (info.get("entries") or []) if e], "query": q}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/video/{video_id}")
async def get_video_info(video_id: str, session_id: str = Cookie(default=None)):
    try:
        s = await _get_session(session_id)
        cookies_file = s.get("cookies_file", "") if s else ""
        pot  = await fetch_pot_token(video_id)
        opts = {**get_ydl_opts(cookies_file=cookies_file), **_pot_args(pot)}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)

        formats = sorted(
            [{"format_id": f.get("format_id"), "quality": f.get("height"),
              "ext": f.get("ext"), "url": f.get("url")}
             for f in info.get("formats", [])
             if f.get("vcodec") != "none" and f.get("acodec") != "none"],
            key=lambda x: x.get("quality") or 0, reverse=True
        )

        # Pré-cache iOS pour éviter une 2e extraction yt-dlp au premier stream
        for f in info.get("formats", []):
            vc, ac, h, u = f.get("vcodec",""), f.get("acodec",""), f.get("height"), f.get("url")
            if vc.startswith("avc") and ac.startswith("mp4a") and h and u and f.get("ext")=="mp4":
                for q in (360, 480, 720, 1080, 1440, 2160):
                    if h <= q and not _cache_get(video_id, q):
                        _cache_set(video_id, q, u)

        return {
            "id":          video_id,
            "title":       info.get("title", ""),
            "description": (info.get("description") or "")[:600],
            "channel":     info.get("channel") or info.get("uploader", ""),
            "channel_id":  info.get("channel_id", ""),
            "duration":    info.get("duration"),
            "thumbnail":   info.get("thumbnail", f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"),
            "views":       info.get("view_count"),
            "likes":       info.get("like_count"),
            "published":   info.get("upload_date", ""),
            "tags":        (info.get("tags") or [])[:8],
            "formats":     formats[:5],
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/stream/{video_id}")
async def stream_video(video_id: str, quality: int = 720, request: Request = None,
                       session_id: str = Cookie(default=None)):
    """
    AXE 2+3 : Proxy byte-range avec hot-cache, retry 403 et gestion backpressure.
    - Hot-cache : premiers 512 KB en RAM → démarrage instantané à la 2e lecture.
    - Retry 403  : si le CDN refuse (token expiré), on invalide et on réextrait.
    - Backpressure : on détecte la déconnexion client pour libérer la connexion CDN.
    """
    rng       = request.headers.get("Range") if request else None
    is_start  = not rng or rng.startswith("bytes=0-")
    cache_key = f"{video_id}_{quality}"

    s = await _get_session(session_id)
    cookies_file = s.get("cookies_file", "") if s else ""

    async def _extract_url() -> str:
        pot = await get_pot(video_id)
        opts = {**get_ydl_opts({"format": _ios_format(quality)}, cookies_file=cookies_file),
                **_pot_args(pot)}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}", download=False
            )
        url = info.get("url") or (
            info["requested_formats"][0]["url"] if info.get("requested_formats") else None
        )
        if not url:
            raise HTTPException(502, "URL de flux introuvable")
        return url

    # ── Résolution de l'URL (depuis cache ou extraction) ──────────────────────
    for attempt in range(2):
        try:
            stream_url = _cache_get(video_id, quality)
            if not stream_url:
                stream_url = await _extract_url()
                _cache_set(video_id, quality, stream_url)

            up_headers: dict = {}
            if rng:
                up_headers["Range"] = rng

            # ── Cas 1 : début de lecture — servir le hot-cache si disponible ──
            cached_head = _hot.get(cache_key) if is_start else None
            if cached_head and not rng:
                # On renvoie les octets déjà en cache + on continue en streaming
                async def gen_hot():
                    yield cached_head
                    # Continuer depuis là où le cache s'arrête
                    cont_headers = {"Range": f"bytes={len(cached_head)}-"}
                    client2 = _yt_client(timeout=None)
                    try:
                        resp2 = await client2.send(
                            httpx.Request("GET", stream_url, headers=cont_headers),
                            stream=True, follow_redirects=True,
                        )
                        async for chunk in resp2.aiter_bytes(65536):
                            if request and await request.is_disconnected():
                                break
                            yield chunk
                    except (asyncio.CancelledError, Exception):
                        pass
                    finally:
                        await client2.aclose()
                return StreamingResponse(gen_hot(), media_type="video/mp4",
                                         headers={"Accept-Ranges": "bytes"})

            # ── Cas 2 : requête normale ───────────────────────────────────────
            client = _yt_client(timeout=None)
            up_resp = await client.send(
                httpx.Request("GET", stream_url, headers=up_headers),
                stream=True, follow_redirects=True,
            )

            if up_resp.status_code == 403:
                await up_resp.aclose()
                await client.aclose()
                # Token ou URL expirée → invalider et réessayer
                _pot_invalidate()
                _cache_del(video_id, quality)
                if attempt == 0:
                    continue
                raise HTTPException(503, "CDN YouTube a refusé la requête (réessayez)")

            resp_h: dict = {"Accept-Ranges": "bytes"}
            for h in ("content-length", "content-range"):
                if h in up_resp.headers:
                    resp_h[h.title()] = up_resp.headers[h]
            ctype = up_resp.headers.get("content-type", "video/mp4")

            async def gen():
                buf = bytearray() if is_start else None
                try:
                    async for chunk in up_resp.aiter_bytes(65536):
                        # AXE 2 : arrêt propre si le client se déconnecte
                        if request and await request.is_disconnected():
                            break
                        # Accumulation hot-cache uniquement pour le début
                        if buf is not None and len(buf) < _HOT_BYTES:
                            buf.extend(chunk)
                            if len(buf) >= _HOT_BYTES:
                                _hot.put(cache_key, bytes(buf[:_HOT_BYTES]))
                        yield chunk
                except (asyncio.CancelledError, ConnectionResetError):
                    pass
                finally:
                    await up_resp.aclose()
                    await client.aclose()

            return StreamingResponse(gen(), status_code=up_resp.status_code,
                                     headers=resp_h, media_type=ctype)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, str(e))

@app.post("/api/watch/stats")
async def report_watch_stats(
    video_id: str  = Query(...),
    position: float = Query(0),
    duration: float = Query(0),
    session_id: str = Cookie(default=None),
):
    """
    AXE 1 : Synchronise la progression de lecture vers YouTube.
    YouTube utilise ces rapports pour mettre à jour l'historique et
    alimenter l'algorithme de recommandations.
    Appelé par le frontend toutes les 15 s pendant la lecture.
    """
    s = await _get_session(session_id)
    if not s:
        return {"ok": False}
    try:
        cpn = "".join(random.choices(string.ascii_letters + string.digits, k=16))
        params = {
            "ns": "yt", "el": "detailpage", "cpn": cpn, "ver": "2",
            "cmt": f"{position:.3f}", "fs": "0", "rt": "1",
            "of": cpn[:8], "euri": "", "lact": "1",
            "len": f"{duration:.3f}", "docid": video_id,
            "cbr": "Chrome", "cbrver": "126.0.0.0",
            "c": "TVHTML5", "cver": _IT_VER_TV,
            "cos": "Linux", "cosver": "3.0",
            "cplatform": "TV", "hl": "fr", "cr": "FR",
        }
        cookies = s.get("cookies") or {}
        access_token = s.get("access_token", "")
        headers: dict = {}
        if cookies:
            auth = _sapisid_hash(cookies)
            if auth:
                headers["Authorization"] = auth
            headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
        elif access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        async with _yt_client(timeout=5) as c:
            await c.get(
                "https://www.youtube.com/api/stats/watchtime",
                params=params,
                headers=headers,
            )
    except Exception:
        pass
    return {"ok": True}

@app.get("/api/sponsorblock/{video_id}")
async def get_sponsorblock(video_id: str):
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{SPONSORBLOCK_API}/api/skipSegments", params={
                "videoID": video_id,
                "categories": '["sponsor","selfpromo","interaction","intro","outro","preview","filler"]'
            })
        return {"segments": r.json() if r.status_code == 200 else []}
    except Exception:
        return {"segments": []}

@app.get("/api/related/{video_id}")
async def get_related(video_id: str, q: str = "", session_id: str = Cookie(default=None)):
    """
    3 niveaux de recommandations :
    1. InnerTube /next  → vrai algo YouTube (sidebar "À suivre")
    2. YouTube Mix RD   → playlist algorithmique de YT
    3. Recherche        → fallback si les deux premiers échouent
    """
    s        = await _get_session(session_id)
    token    = s.get("access_token", "") if s else ""
    cookies  = s.get("cookies") if s else None
    cookies_file = s.get("cookies_file", "") if s else ""
    try:
        # 1. InnerTube
        videos = await _it_next(video_id, token, cookies)
        if videos:
            return {"results": videos[:12]}

        # 2. YouTube Mix (RD{video_id})
        try:
            mix_opts = get_ydl_opts({"extract_flat": True, "playlist_items": "2-14"},
                                    cookies_file=cookies_file)
            with yt_dlp.YoutubeDL(mix_opts) as ydl:
                mix = ydl.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}&list=RD{video_id}",
                    download=False
                )
            results = [_fmt_entry(e) for e in (mix.get("entries") or [])
                       if e and e.get("id") != video_id]
            if results:
                return {"results": results[:12]}
        except Exception:
            pass

        # 3. Recherche titre/tags
        if not q:
            with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True},
                                                cookies_file=cookies_file)) as ydl:
                meta = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}",
                                        download=False)
            tags = meta.get("tags") or []
            q = " ".join(tags[:4]) if tags else meta.get("title", "")
        with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True})) as ydl:
            search = ydl.extract_info(f"ytsearch15:{q}", download=False)
        return {"results": [_fmt_entry(e) for e in (search.get("entries") or [])
                             if e and e.get("id") != video_id][:12]}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/trending")
async def get_trending(session_id: str = Cookie(default=None)):
    """InnerTube FEtrending → yt-dlp avec pot token → ytsearch."""
    s       = await _get_session(session_id)
    token   = s.get("access_token", "") if s else ""
    cookies = s.get("cookies") if s else None

    # 1. InnerTube (le plus rapide, pas de quota)
    try:
        videos = await _it_browse("FEtrending", token, cookies)
        if videos:
            return {"results": videos[:40]}
    except Exception:
        pass

    # 2. yt-dlp avec pot token (contourne le bot-detection)
    try:
        pot = await fetch_pot_token()
        with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True, **_pot_args(pot)})) as ydl:
            info = ydl.extract_info("https://www.youtube.com/feed/trending", download=False)
        results = [_fmt_entry(e) for e in (info.get("entries") or [])[:40] if e]
        if results:
            return {"results": results}
    except Exception:
        pass

    # 3. Recherche populaire en dernier recours
    try:
        with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True})) as ydl:
            info = ydl.extract_info("ytsearch40:tendances musique france 2025", download=False)
        return {"results": [_fmt_entry(e) for e in (info.get("entries") or [])[:40] if e]}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/feed")
async def get_feed(session_id: str = Cookie(default=None)):
    """Page d'accueil personnalisée YouTube (nécessite connexion Google)."""
    s = await _get_session(session_id)
    if not s:
        raise HTTPException(401, "Connexion Google requise")
    try:
        cookies = s.get("cookies")
        token   = s.get("access_token", "")
        videos = await _it_browse("FEwhat_to_watch", token, cookies)
        if videos:
            return {"results": videos[:40]}
        return await get_trending(session_id)
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/subscriptions")
async def get_subscriptions(session_id: str = Cookie(default=None)):
    """Abonnements YouTube — Data API v3 (OAuth) ou InnerTube (cookies)."""
    s = await _get_session(session_id)
    if not s:
        raise HTTPException(401, "Connexion Google requise")

    auth_method = s.get("auth_method", "oauth")

    # Méthode cookies : InnerTube /browse avec browseId FEsubscriptions
    if auth_method == "cookies":
        try:
            cookies = s.get("cookies") or {}
            videos = await _it_browse("FEsubscriptions", cookies=cookies)
            if videos:
                return {"subscriptions": videos}
            # Fallback : récupérer la liste des chaînes via guide
            async with _yt_client(timeout=10) as c:
                r = await c.post(f"{_IT_BASE}/guide",
                                 json={"context": _it_ctx(cookies=cookies)},
                                 headers=_it_headers(cookies=cookies))
            data = r.json()
            subs = []
            for item in (data.get("items") or []):
                renderer = item.get("guideSubscriptionsSectionRenderer", {})
                for entry in renderer.get("items", []):
                    ge = entry.get("guideEntryRenderer", {})
                    title = ge.get("formattedTitle", {}).get("simpleText", "")
                    nav = ge.get("navigationEndpoint", {}).get("browseEndpoint", {})
                    channel_id = nav.get("browseId", "")
                    thumbs = ge.get("thumbnail", {}).get("thumbnails", [])
                    thumb = thumbs[-1].get("url", "") if thumbs else ""
                    if title and channel_id:
                        subs.append({"id": channel_id, "title": title, "thumbnail": thumb})
            return {"subscriptions": subs}
        except Exception as e:
            raise HTTPException(500, str(e))

    # Méthode OAuth : YouTube Data API v3
    try:
        async with _yt_client(timeout=10) as c:
            r = await c.get(f"{_YT_API}/subscriptions", params={
                "part": "snippet", "mine": "true",
                "maxResults": "50", "order": "alphabetical",
            }, headers={"Authorization": f"Bearer {s['access_token']}"})
        data = r.json()
        if "error" in data:
            raise HTTPException(403, data["error"].get("message", "Erreur API YouTube"))
        return {"subscriptions": [
            {"id":        item["snippet"]["resourceId"]["channelId"],
             "title":     item["snippet"]["title"],
             "thumbnail": item["snippet"].get("thumbnails",{}).get("default",{}).get("url","")}
            for item in data.get("items", [])
        ]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/channel/{channel_id}")
async def get_channel_videos(channel_id: str):
    """20 dernières vidéos d'une chaîne."""
    try:
        with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True,
                                              "playlist_items": "1-20"})) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/channel/{channel_id}/videos", download=False
            )
        return {
            "results": [_fmt_entry(e) for e in (info.get("entries") or []) if e],
            "channel": info.get("channel") or info.get("title", ""),
        }
    except Exception as e:
        raise HTTPException(500, str(e))
