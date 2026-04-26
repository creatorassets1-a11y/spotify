"""ClipFX Spotify search + download queue producer.

This Render-hosted FastAPI app stays intentionally light:
- It searches Spotify metadata.
- It finds a likely YouTube URL for a selected track.
- It enqueues a remote RQ job into Upstash Redis.

The DigitalOcean worker must listen to the `downloads` queue and expose:
    worker.process_track(youtube_url: str, spotify_id: str)
"""

import os
from functools import lru_cache
from typing import Any

import spotipy
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from redis import Redis, RedisError
from rq import Queue
from spotipy.oauth2 import SpotifyClientCredentials

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # python-dotenv is helpful locally, but Render should use environment variables.
    pass

QUEUE_NAME = "downloads"
WORKER_FUNCTION_PATH = "worker.process_track"

app = FastAPI(
    title="ClipFX Spotify Producer API",
    version="1.0.0",
    description="Lightweight Spotify metadata API and RQ producer for ClipFX downloads.",
)


class SearchResponseTrack(BaseModel):
    spotify_track_id: str
    track_name: str
    artist_names: list[str]
    album_name: str | None = None
    preview_url: str | None = None
    image_url: str | None = None
    spotify_url: str | None = None


class DownloadRequest(BaseModel):
    spotify_id: str = Field(..., min_length=1, max_length=160)
    track_name: str = Field(..., min_length=1, max_length=300)
    artist_name: str = Field(..., min_length=1, max_length=300)


class DownloadResponse(BaseModel):
    job_id: str
    status: str = "queued"
    queue: str = QUEUE_NAME
    spotify_id: str
    youtube_url: str


@lru_cache(maxsize=1)
def get_redis_connection() -> Redis:
    """Return a cached Redis connection using REDIS_URL."""

    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        raise RuntimeError("REDIS_URL is not configured.")

    return Redis.from_url(
        redis_url,
        socket_connect_timeout=8,
        socket_timeout=30,
        retry_on_timeout=True,
        health_check_interval=30,
    )


@lru_cache(maxsize=1)
def get_download_queue() -> Queue:
    """Return the RQ queue used by the DigitalOcean worker."""

    return Queue(QUEUE_NAME, connection=get_redis_connection())


@lru_cache(maxsize=1)
def get_spotify_client() -> spotipy.Spotify:
    """Return a cached Spotipy client configured from environment variables."""

    client_id = os.getenv("SPOTIPY_CLIENT_ID", "").strip()
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        raise RuntimeError("SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET must be configured.")

    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret,
    )

    return spotipy.Spotify(auth_manager=auth_manager, requests_timeout=12, retries=2)


def normalize_spotify_track(item: dict[str, Any]) -> SearchResponseTrack:
    """Convert Spotify's raw track payload into the small shape the frontend needs."""

    artists = [artist.get("name", "") for artist in item.get("artists", []) if artist.get("name")]
    images = item.get("album", {}).get("images", []) or []
    image_url = images[0].get("url") if images else None

    return SearchResponseTrack(
        spotify_track_id=item.get("id", ""),
        track_name=item.get("name", ""),
        artist_names=artists,
        album_name=item.get("album", {}).get("name"),
        preview_url=item.get("preview_url"),
        image_url=image_url,
        spotify_url=item.get("external_urls", {}).get("spotify"),
    )


def find_best_youtube_url(track_name: str, artist_name: str) -> str:
    """Return the best YouTube URL for the song.

    This is intentionally lightweight. The Render app should not download or process
    media. The worker can resolve `ytsearch1:` using yt-dlp, or you can later replace
    this with YouTube Data API search and return a direct video URL.
    """

    clean_track = " ".join(track_name.split())
    clean_artist = " ".join(artist_name.split())
    query = f"{clean_artist} - {clean_track} official audio"

    # yt-dlp supports this search URL/query form directly on the worker side.
    return f"ytsearch1:{query}"


@app.get("/")
def root() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "clipfx-spotify-producer",
        "queue": QUEUE_NAME,
    }


@app.get("/health")
def health() -> dict[str, str]:
    """Render health check endpoint."""

    try:
        get_redis_connection().ping()
    except (RedisError, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Redis is unavailable: {exc}",
        ) from exc

    return {"status": "ok", "redis": "connected"}


@app.get("/search", response_model=list[SearchResponseTrack])
def search_tracks(query: str, limit: int = 10) -> list[SearchResponseTrack]:
    """Search Spotify tracks by text query."""

    clean_query = query.strip()
    if not clean_query:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="query is required.")

    safe_limit = min(max(limit, 1), 25)

    try:
        spotify = get_spotify_client()
        results = spotify.search(q=clean_query, type="track", limit=safe_limit)
        tracks = results.get("tracks", {}).get("items", []) or []
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Spotify search failed.",
        ) from exc

    return [normalize_spotify_track(track) for track in tracks if track.get("id") and track.get("name")]


@app.post("/download", response_model=DownloadResponse, status_code=status.HTTP_202_ACCEPTED)
def queue_download(payload: DownloadRequest) -> DownloadResponse:
    """Find a YouTube source and enqueue the processing task.

    This endpoint does not process audio. It only produces a job for the remote
    DigitalOcean worker through Upstash Redis/RQ.
    """

    youtube_url = find_best_youtube_url(payload.track_name, payload.artist_name)

    try:
        queue = get_download_queue()
        job = queue.enqueue(
            WORKER_FUNCTION_PATH,
            youtube_url,
            payload.spotify_id,
            job_timeout="30m",
            result_ttl=24 * 60 * 60,
            failure_ttl=7 * 24 * 60 * 60,
            meta={
                "spotify_id": payload.spotify_id,
                "track_name": payload.track_name,
                "artist_name": payload.artist_name,
                "youtube_url": youtube_url,
                "source": "clipfx-render-fastapi-producer",
            },
        )
    except (RedisError, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not connect to Redis or enqueue the job: {exc}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="The download job could not be queued.",
        ) from exc

    return DownloadResponse(
        job_id=job.id,
        spotify_id=payload.spotify_id,
        youtube_url=youtube_url,
    )
