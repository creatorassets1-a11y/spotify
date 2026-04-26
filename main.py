"""ClipFX Spotify search + download queue producer.

This Render-hosted FastAPI app stays intentionally light:
- It searches Spotify metadata.
- It accepts legacy ClipFX spotify_url download requests.
- It finds a likely YouTube URL for a selected track.
- It enqueues a remote RQ job into Upstash Redis.

The DigitalOcean worker must listen to the `downloads` queue and expose:
    worker.process_track(youtube_url: str, spotify_id: str)
"""

import os
import re
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import spotipy
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, HttpUrl, model_validator
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
    version="1.1.1",
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
    # New producer contract
    spotify_id: str | None = Field(default=None, min_length=1, max_length=160)
    track_name: str | None = Field(default=None, min_length=1, max_length=300)
    artist_name: str | None = Field(default=None, min_length=1, max_length=300)

    # Legacy ClipFX backend contract
    spotify_url: HttpUrl | None = None
    url: HttpUrl | None = None
    spotifyUrl: HttpUrl | None = None
    track_url: HttpUrl | None = None
    trackUrl: HttpUrl | None = None

    @model_validator(mode="after")
    def require_identifier_or_url(self) -> "DownloadRequest":
        if self.spotify_id or self.spotify_url or self.url or self.spotifyUrl or self.track_url or self.trackUrl:
            return self
        raise ValueError("Provide either spotify_id plus track metadata, or spotify_url.")


class DownloadResponse(BaseModel):
    job_id: str
    status: str = "queued"
    queue: str = QUEUE_NAME
    spotify_id: str
    youtube_url: str
    track_name: str | None = None
    artist_name: str | None = None
    message: str = "Job queued. Poll the worker/status service until the file is ready."


@lru_cache(maxsize=1)
def get_redis_connection() -> Redis:
    """Return a cached Upstash Redis connection using REDIS_URL.

    Upstash usually requires rediss://. The extra timeout, retry, and health check
    settings keep Render/RQ connections from failing after idle periods.
    """

    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        raise RuntimeError("REDIS_URL is not configured.")

    connection_options: dict[str, Any] = {
        "socket_connect_timeout": 5,
        "socket_timeout": 30,
        "retry_on_timeout": True,
        "health_check_interval": 30,
    }

    if redis_url.lower().startswith("rediss://"):
        connection_options["ssl_cert_reqs"] = None

    return Redis.from_url(redis_url, **connection_options)


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


def get_payload_spotify_url(payload: DownloadRequest) -> str:
    for value in [payload.spotify_url, payload.url, payload.spotifyUrl, payload.track_url, payload.trackUrl]:
        if value:
            return str(value)
    return ""


def extract_spotify_track_id(value: str) -> str:
    clean_value = str(value or "").strip()
    if not clean_value:
        return ""

    if re.fullmatch(r"[A-Za-z0-9]{10,40}", clean_value):
        return clean_value

    spotify_uri_match = re.search(r"spotify:track:([A-Za-z0-9]+)", clean_value, re.IGNORECASE)
    if spotify_uri_match:
        return spotify_uri_match.group(1)

    try:
        parsed = urlparse(clean_value)
        parts = [part for part in parsed.path.split("/") if part]
        if "track" in parts:
            index = parts.index("track")
            if index + 1 < len(parts):
                return parts[index + 1]
    except Exception:
        pass

    return ""


def get_track_metadata_from_spotify_id(spotify_id: str) -> tuple[str, str]:
    try:
        track = get_spotify_client().track(spotify_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Spotify metadata lookup failed.") from exc

    track_name = str(track.get("name") or "").strip()
    artists = [artist.get("name", "") for artist in track.get("artists", []) if artist.get("name")]
    artist_name = artists[0] if artists else ""

    if not track_name or not artist_name:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spotify track metadata is incomplete.")

    return track_name, artist_name


def resolve_download_payload(payload: DownloadRequest) -> tuple[str, str, str]:
    spotify_id = str(payload.spotify_id or "").strip()
    spotify_url = get_payload_spotify_url(payload)

    if not spotify_id and spotify_url:
        spotify_id = extract_spotify_track_id(spotify_url)

    if not spotify_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="A valid Spotify track ID or Spotify track URL is required.")

    track_name = str(payload.track_name or "").strip()
    artist_name = str(payload.artist_name or "").strip()

    if not track_name or not artist_name:
        track_name, artist_name = get_track_metadata_from_spotify_id(spotify_id)

    return spotify_id, track_name, artist_name


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
    """Accept old ClipFX spotify_url requests and new metadata requests.

    This endpoint does not process audio. It only produces a job for the remote
    DigitalOcean worker through Upstash Redis/RQ.
    """

    spotify_id, track_name, artist_name = resolve_download_payload(payload)
    youtube_url = find_best_youtube_url(track_name, artist_name)

    try:
        queue = get_download_queue()
        job = queue.enqueue(
            WORKER_FUNCTION_PATH,
            youtube_url,
            spotify_id,
            job_timeout="30m",
            result_ttl=24 * 60 * 60,
            failure_ttl=7 * 24 * 60 * 60,
            meta={
                "spotify_id": spotify_id,
                "track_name": track_name,
                "artist_name": artist_name,
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
        spotify_id=spotify_id,
        youtube_url=youtube_url,
        track_name=track_name,
        artist_name=artist_name,
    )
