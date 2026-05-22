"""
MyTube Backend — FastAPI + yt-dlp
Proxy YouTube sans pubs, avec SponsorBlock
"""

import os
import json
import asyncio
import httpx
from fastapi import FastAPI, HTTPException, Query
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
            "formats": formats[:5],  # Top 5 qualités
            "stream_url": f"/api/stream/{video_id}",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stream/{video_id}")
async def stream_video(video_id: str, quality: int = 720):
    """Proxy stream vidéo — contourne les restrictions de domaine"""
    try:
        pot = await fetch_pot_token(video_id)
        opts = get_ydl_opts({
            "format": f"bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]/best[height<={quality}][ext=mp4]/best",
        })

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
            stream_url = info.get("url") or info["requested_formats"][0]["url"]

        # Proxy le flux vers le client
        async def stream_generator():
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", stream_url) as response:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        yield chunk

        return StreamingResponse(
            stream_generator(),
            media_type="video/mp4",
            headers={"Accept-Ranges": "bytes"}
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
