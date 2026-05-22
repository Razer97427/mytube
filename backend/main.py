"""
MyTube Backend — FastAPI + yt-dlp
Proxy YouTube sans pubs, avec SponsorBlock
"""

import os
import time
import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp

app = FastAPI(title="MyTube API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

POT_PROVIDER_URL = os.getenv("POT_PROVIDER_URL", "http://pot-provider:4416")
SPONSORBLOCK_API = os.getenv("SPONSORBLOCK_API", "https://sponsor.ajay.app")

# ─── Cache URL flux (iOS envoie 3-4 requêtes Range par lecture) ───────────────
# Les URLs YouTube sont valides ~6 h ; on cache 1 h pour être conservateur.
_stream_cache: dict[str, tuple[str, float]] = {}

def _cache_get(video_id: str, quality: int) -> str | None:
    entry = _stream_cache.get(f"{video_id}_{quality}")
    if entry and time.time() < entry[1]:
        return entry[0]
    return None

def _cache_set(video_id: str, quality: int, url: str) -> None:
    _stream_cache[f"{video_id}_{quality}"] = (url, time.time() + 3600)

# ─── Helpers yt-dlp ──────────────────────────────────────────────────────────

def get_ydl_opts(extra: dict = {}) -> dict:
    """Options yt-dlp de base avec po_token si disponible"""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        "nocheckcertificate": True,
        **extra
    }
    return opts


async def fetch_pot_token(video_id: str = "dQw4w9WgXcQ") -> dict:
    """Récupère le po_token depuis le provider"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                f"{POT_PROVIDER_URL}/get_pot",
                json={"videoId": video_id}
            )
            if r.status_code == 200:
                data = r.json()
                return {
                    "po_token": data.get("potoken", ""),
                    "visitor_data": data.get("visitorData", "")
                }
    except Exception:
        pass
    return {}


def _ios_format(quality: int) -> str:
    # iOS Safari n'accepte que H.264 (avc1) + AAC (mp4a) dans un conteneur MP4.
    # VP9 / AV1 / WebM → écran noir ou "impossible de lire la vidéo".
    return (
        f"bestvideo[height<={quality}][vcodec^=avc][ext=mp4]"
        f"+bestaudio[acodec^=mp4a][ext=m4a]"
        f"/best[height<={quality}][vcodec^=avc][ext=mp4]"
        f"/best[height<={quality}][ext=mp4]"
        f"/best"
    )


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "service": "MyTube API"}


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1), limit: int = 20):
    """Recherche YouTube via yt-dlp"""
    try:
        results = []
        opts = get_ydl_opts({
            "extract_flat": True,
            "quiet": True,
        })

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch{limit}:{q}", download=False)
            if info and "entries" in info:
                for entry in info["entries"]:
                    if not entry:
                        continue
                    results.append({
                        "id": entry.get("id", ""),
                        "title": entry.get("title", ""),
                        "channel": entry.get("channel") or entry.get("uploader", ""),
                        "duration": entry.get("duration"),
                        "thumbnail": entry.get("thumbnail") or f"https://i.ytimg.com/vi/{entry.get('id', '')}/hqdefault.jpg",
                        "views": entry.get("view_count"),
                        "published": entry.get("upload_date", ""),
                    })

        return {"results": results, "query": q}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/video/{video_id}")
async def get_video_info(video_id: str):
    """Récupère les métadonnées d'une vidéo"""
    try:
        pot = await fetch_pot_token(video_id)
        opts = get_ydl_opts()

        if pot.get("po_token"):
            opts["extractor_args"] = {
                "youtube": {
                    "po_token": [f"web+{pot['po_token']}"],
                    "visitor_data": [pot["visitor_data"]],
                    "player_client": ["web"],
                }
            }

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)

        # Sélectionner le meilleur format vidéo+audio combiné ou séparer
        formats = []
        for f in info.get("formats", []):
            if f.get("vcodec") != "none" and f.get("acodec") != "none":
                formats.append({
                    "format_id": f.get("format_id"),
                    "quality": f.get("height"),
                    "ext": f.get("ext"),
                    "url": f.get("url"),
                })

        # Trier par qualité décroissante
        formats.sort(key=lambda x: x.get("quality") or 0, reverse=True)

        # Pré-cache les URLs iOS : évite un 2e appel yt-dlp quand le lecteur
        # demande /api/stream juste après. Les URLs YouTube sont valides ~6 h.
        for f in info.get("formats", []):
            vc, ac, h, u = (f.get("vcodec", ""), f.get("acodec", ""),
                            f.get("height"), f.get("url"))
            if vc.startswith("avc") and ac.startswith("mp4a") and h and u and f.get("ext") == "mp4":
                for q in (360, 480, 720, 1080, 1440, 2160):
                    if h <= q and not _cache_get(video_id, q):
                        _cache_set(video_id, q, u)

        return {
            "id": video_id,
            "title": info.get("title", ""),
            "description": info.get("description", "")[:500] if info.get("description") else "",
            "channel": info.get("channel") or info.get("uploader", ""),
            "channel_id": info.get("channel_id", ""),
            "duration": info.get("duration"),
            "thumbnail": info.get("thumbnail", f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"),
            "views": info.get("view_count"),
            "published": info.get("upload_date", ""),
            "formats": formats[:5],
            "stream_url": f"/api/stream/{video_id}",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stream/{video_id}")
async def stream_video(video_id: str, quality: int = 720, request: Request = None):
    """Proxy flux vidéo compatible iOS : H.264/AAC, Range requests, cache URL."""
    try:
        stream_url = _cache_get(video_id, quality)

        if not stream_url:
            pot = await fetch_pot_token(video_id)
            opts = get_ydl_opts({"format": _ios_format(quality)})

            if pot.get("po_token"):
                opts["extractor_args"] = {
                    "youtube": {
                        "po_token": [f"web+{pot['po_token']}"],
                        "visitor_data": [pot["visitor_data"]],
                        "player_client": ["web"],
                    }
                }

            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}", download=False
                )
                stream_url = info.get("url")
                if not stream_url and info.get("requested_formats"):
                    stream_url = info["requested_formats"][0]["url"]

            _cache_set(video_id, quality, stream_url)

        # Transmettre le header Range pour que iOS puisse seeker dans la vidéo.
        # iOS envoie typiquement Range: bytes=0-1 pour tester, puis d'autres plages.
        upstream_headers = {}
        range_header = request.headers.get("Range") if request else None
        if range_header:
            upstream_headers["Range"] = range_header

        # send(..., stream=True) retourne les headers immédiatement sans lire le body.
        # Cela nous permet de connaître status_code / Content-Range avant de streamer.
        client = httpx.AsyncClient(timeout=None)
        upstream_req = httpx.Request("GET", stream_url, headers=upstream_headers)
        upstream_resp = await client.send(upstream_req, stream=True, follow_redirects=True)

        content_type = upstream_resp.headers.get("content-type", "video/mp4")
        resp_headers = {"Accept-Ranges": "bytes"}
        for h in ("content-length", "content-range"):
            if h in upstream_resp.headers:
                resp_headers[h.title()] = upstream_resp.headers[h]

        async def generator():
            try:
                async for chunk in upstream_resp.aiter_bytes(65536):
                    yield chunk
            finally:
                await upstream_resp.aclose()
                await client.aclose()

        return StreamingResponse(
            generator(),
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=content_type,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sponsorblock/{video_id}")
async def get_sponsorblock(video_id: str):
    """Récupère les segments SponsorBlock pour une vidéo"""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                f"{SPONSORBLOCK_API}/api/skipSegments",
                params={
                    "videoID": video_id,
                    "categories": '["sponsor","selfpromo","interaction","intro","outro","preview","filler"]'
                }
            )
            if r.status_code == 200:
                return {"segments": r.json()}
            elif r.status_code == 404:
                return {"segments": []}
            else:
                return {"segments": []}
    except Exception:
        return {"segments": []}


@app.get("/api/related/{video_id}")
async def get_related(video_id: str, q: str = ""):
    """Recommandations basées sur le titre / tags de la vidéo.
    Accepte ?q=... pour éviter une extraction yt-dlp supplémentaire
    quand le client connaît déjà le titre."""
    try:
        if not q:
            opts = get_ydl_opts({"extract_flat": True})
            with yt_dlp.YoutubeDL(opts) as ydl:
                meta = ydl.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}", download=False
                )
            tags = meta.get("tags") or []
            q = " ".join(tags[:4]) if tags else meta.get("title", "")

        results = []
        with yt_dlp.YoutubeDL(get_ydl_opts({"extract_flat": True})) as ydl:
            search = ydl.extract_info(f"ytsearch15:{q}", download=False)
            for entry in (search.get("entries") or []):
                if not entry or entry.get("id") == video_id:
                    continue
                results.append({
                    "id": entry.get("id", ""),
                    "title": entry.get("title", ""),
                    "channel": entry.get("channel") or entry.get("uploader", ""),
                    "duration": entry.get("duration"),
                    "thumbnail": (entry.get("thumbnail")
                                  or f"https://i.ytimg.com/vi/{entry.get('id', '')}/hqdefault.jpg"),
                    "views": entry.get("view_count"),
                })

        return {"results": results[:12]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trending")
async def get_trending():
    """Récupère les vidéos tendances"""
    try:
        opts = get_ydl_opts({"extract_flat": True})
        results = []

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info("https://www.youtube.com/feed/trending", download=False)
            if info and "entries" in info:
                for entry in (info["entries"] or [])[:40]:
                    if not entry:
                        continue
                    results.append({
                        "id": entry.get("id", ""),
                        "title": entry.get("title", ""),
                        "channel": entry.get("channel") or entry.get("uploader", ""),
                        "duration": entry.get("duration"),
                        "thumbnail": f"https://i.ytimg.com/vi/{entry.get('id', '')}/hqdefault.jpg",
                        "views": entry.get("view_count"),
                    })

        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
