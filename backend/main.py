"""
MyTube Backend v2 — FastAPI + yt-dlp + InnerTube + Google OAuth2

Streaming : proxy byte-range transparent.
  YouTube CDN → backend → navigateur (par chunks de 64 KB).
  Les URLs YouTube sont liées à l'IP du serveur qui les extrait :
  le proxy est donc obligatoire — la vidéo n'est PAS téléchargée
  avant d'être envoyée, elle transite en temps réel.
"""

import os
import time
import uuid
import secrets
import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response, Cookie
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
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
# Proxy VPN optionnel — ex: "http://vpn:8888" (gluetun HTTP proxy)
# Laisser vide pour ne pas utiliser de VPN.
PROXY_URL        = os.getenv("PROXY_URL", "").strip() or None

# ── Sessions ──────────────────────────────────────────────────────────────────
_sessions: dict[str, dict] = {}

def _get_session(sid: str | None) -> dict | None:
    if not sid:
        return None
    s = _sessions.get(sid)
    if s and s.get("expires_at", 0) > time.time():
        return s
    if s:
        _sessions.pop(sid, None)
    return None

# ── Cache URL flux ────────────────────────────────────────────────────────────
# iOS envoie 3-4 requêtes Range par lecture ; le cache évite de relancer yt-dlp.
_stream_cache: dict[str, tuple[str, float]] = {}

def _cache_get(video_id: str, quality: int) -> str | None:
    e = _stream_cache.get(f"{video_id}_{quality}")
    return e[0] if e and time.time() < e[1] else None

def _cache_set(video_id: str, quality: int, url: str) -> None:
    _stream_cache[f"{video_id}_{quality}"] = (url, time.time() + 3600)

# ── Helpers yt-dlp ────────────────────────────────────────────────────────────
def get_ydl_opts(extra: dict = {}) -> dict:
    opts: dict = {"quiet": True, "no_warnings": True, "extract_flat": False,
                  "nocheckcertificate": True}
    if PROXY_URL:
        opts["proxy"] = PROXY_URL
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

# ── InnerTube ─────────────────────────────────────────────────────────────────
# InnerTube est l'API interne de YouTube (utilisée par leur propre site web).
# On utilise la clé publique du client web — pas besoin de compte développeur.
_IT_KEY  = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
_IT_BASE = "https://www.youtube.com/youtubei/v1"
_IT_CTX  = {"client": {"clientName": "WEB", "clientVersion": "2.20240101.00.00",
                        "hl": "fr", "gl": "FR"}}

def _it_headers(token: str = "") -> dict:
    h = {"Content-Type": "application/json",
         "Origin": "https://www.youtube.com",
         "Referer": "https://www.youtube.com/"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

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

async def _it_next(video_id: str, token: str = "") -> list[dict]:
    """Recommandations InnerTube — identiques à la sidebar 'À suivre' de YouTube."""
    try:
        async with _yt_client(timeout=10) as c:
            r = await c.post(f"{_IT_BASE}/next", params={"key": _IT_KEY},
                             json={"videoId": video_id, "context": _IT_CTX},
                             headers=_it_headers(token))
        items = (r.json()
                 .get("contents", {})
                 .get("twoColumnWatchNextResults", {})
                 .get("secondaryResults", {})
                 .get("secondaryResults", {})
                 .get("results", []))
        out = []
        for item in items:
            rd = (item.get("compactVideoRenderer")
                  or (item.get("compactAutoplayRenderer") or {})
                     .get("contents", [{}])[0].get("compactVideoRenderer"))
            if rd:
                v = _parse_renderer(rd)
                if v and v["id"] != video_id:
                    out.append(v)
        return out
    except Exception:
        return []

async def _it_browse(browse_id: str, token: str = "") -> list[dict]:
    """Browse InnerTube : trending (FEtrending), accueil perso (FEwhat_to_watch)…"""
    try:
        async with _yt_client(timeout=12) as c:
            r = await c.post(f"{_IT_BASE}/browse", params={"key": _IT_KEY},
                             json={"browseId": browse_id, "context": _IT_CTX},
                             headers=_it_headers(token))
        contents = (r.json()
                    .get("contents", {})
                    .get("twoColumnBrowseResultsRenderer", {})
                    .get("tabs", [{}])[0]
                    .get("tabRenderer", {})
                    .get("content", {})
                    .get("richGridRenderer", {})
                    .get("contents", []))
        out = []
        for item in contents:
            rd = item.get("richItemRenderer", {}).get("content", {}).get("videoRenderer")
            if rd:
                v = _parse_renderer(rd)
                if v:
                    out.append(v)
                continue
            # Sections imbriquées (ex. "Meilleures tendances")
            for sub in (item.get("richSectionRenderer", {})
                           .get("content", {})
                           .get("richShelfRenderer", {})
                           .get("contents", [])):
                rd2 = sub.get("richItemRenderer", {}).get("content", {}).get("videoRenderer")
                if rd2:
                    v = _parse_renderer(rd2)
                    if v:
                        out.append(v)
        return out
    except Exception:
        return []

# ── Auth Google — Device Code Flow (comme SmartTube / YouTube TV) ─────────────
# Credentials publics de l'app "YouTube on TV" — aucune config nécessaire.
_YTV_ID     = "861556708454-d6dlm3lh05idd8npek18k6be8ba3oc68.apps.googleusercontent.com"
_YTV_SECRET = "SboVhoG9s0rNafixCSGGKXAT"
# Uniquement le scope YouTube — les scopes userinfo.* causent restricted_client
# avec ce client ID (credentials publics de l'app YouTube TV).
_YTV_SCOPE  = "https://www.googleapis.com/auth/youtube.readonly"

_G_DEVICE = "https://oauth2.googleapis.com/device/code"
_G_TOKEN  = "https://oauth2.googleapis.com/token"
_YT_API   = "https://www.googleapis.com/youtube/v3"

# poll_id → {device_code, expires_at}
_pending_devices: dict[str, dict] = {}

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

        sid = str(uuid.uuid4())
        _sessions[sid] = {
            "user":          user,
            "access_token":  access_token,
            "refresh_token": t.get("refresh_token", ""),
            "expires_at":    time.time() + 86400 * 30,
        }
        _pending_devices.pop(poll_id, None)
        response.set_cookie("session_id", sid, httponly=True,
                            max_age=86400*30, samesite="lax", path="/")
        return {"status": "authorized", "user": user}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/auth/status")
async def auth_status(session_id: str = Cookie(default=None)):
    s = _get_session(session_id)
    return {"authenticated": bool(s), "user": s["user"] if s else None}

@app.post("/auth/logout")
async def auth_logout(response: Response, session_id: str = Cookie(default=None)):
    if session_id:
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
async def get_video_info(video_id: str):
    try:
        pot  = await fetch_pot_token(video_id)
        opts = {**get_ydl_opts(), **_pot_args(pot)}
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
async def stream_video(video_id: str, quality: int = 720, request: Request = None):
    """
    Proxy byte-range transparent : pas de téléchargement préalable.
    Le navigateur reçoit les données au fur et à mesure (streaming réel).
    Seek = nouvelle requête Range → YouTube CDN répond avec 206 Partial Content.
    """
    try:
        stream_url = _cache_get(video_id, quality)
        if not stream_url:
            pot  = await fetch_pot_token(video_id)
            opts = {**get_ydl_opts({"format": _ios_format(quality)}), **_pot_args(pot)}
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
                stream_url = info.get("url")
                if not stream_url and info.get("requested_formats"):
                    stream_url = info["requested_formats"][0]["url"]
            _cache_set(video_id, quality, stream_url)

        up_headers = {}
        rng = request.headers.get("Range") if request else None
        if rng:
            up_headers["Range"] = rng

        client = _yt_client(timeout=None)
        up_resp = await client.send(
            httpx.Request("GET", stream_url, headers=up_headers),
            stream=True, follow_redirects=True
        )
        ctype = up_resp.headers.get("content-type", "video/mp4")
        resp_h = {"Accept-Ranges": "bytes"}
        for h in ("content-length", "content-range"):
            if h in up_resp.headers:
                resp_h[h.title()] = up_resp.headers[h]

        async def gen():
            try:
                async for chunk in up_resp.aiter_bytes(65536):
                    yield chunk
            finally:
                await up_resp.aclose()
                await client.aclose()

        return StreamingResponse(gen(), status_code=up_resp.status_code,
                                 headers=resp_h, media_type=ctype)
    except Exception as e:
        raise HTTPException(500, str(e))

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
    s     = _get_session(session_id)
    token = s["access_token"] if s else ""
    try:
        # 1. InnerTube
        videos = await _it_next(video_id, token)
        if videos:
            return {"results": videos[:12]}

        # 2. YouTube Mix (RD{video_id})
        try:
            with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True,
                                                  "playlist_items": "2-14"})) as ydl:
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
            with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True})) as ydl:
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
    """InnerTube FEtrending → fallback yt-dlp."""
    s     = _get_session(session_id)
    token = s["access_token"] if s else ""
    try:
        videos = await _it_browse("FEtrending", token)
        if videos:
            return {"results": videos[:40]}
        with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True})) as ydl:
            info = ydl.extract_info("https://www.youtube.com/feed/trending", download=False)
        return {"results": [_fmt_entry(e) for e in (info.get("entries") or [])[:40] if e]}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/feed")
async def get_feed(session_id: str = Cookie(default=None)):
    """Page d'accueil personnalisée YouTube (nécessite connexion Google)."""
    s = _get_session(session_id)
    if not s:
        raise HTTPException(401, "Connexion Google requise")
    try:
        videos = await _it_browse("FEwhat_to_watch", s["access_token"])
        if videos:
            return {"results": videos[:40]}
        return await get_trending(session_id)
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/subscriptions")
async def get_subscriptions(session_id: str = Cookie(default=None)):
    """Abonnements YouTube (YouTube Data API v3)."""
    s = _get_session(session_id)
    if not s:
        raise HTTPException(401, "Connexion Google requise")
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
