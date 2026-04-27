"""ClipFX Spotify download worker.

Run this as a separate worker process connected to the same Redis queue used by
main.py:

    rq worker downloads --url "$REDIS_URL"

The producer queues jobs as:
    worker.process_track(youtube_url, spotify_id)

This worker downloads audio with yt-dlp, uploads it to Cloudflare R2, and marks
matching ClipFX music_tracks rows as ready in Postgres.
"""

from __future__ import annotations

import base64
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import quote

import boto3
import psycopg2
from botocore.client import Config

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clipfx-spotify-worker")

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "900"))
YTDLP_SEARCH_LIMIT = max(1, min(10, int(os.getenv("YTDLP_SEARCH_LIMIT", "3"))))
YTDLP_PLAYER_CLIENTS = [value.strip() for value in os.getenv("YTDLP_PLAYER_CLIENTS", "ios,android,web,tv").split(",") if value.strip()]
YTDLP_PROXY = os.getenv("YTDLP_PROXY", "").strip()
YTDLP_COOKIES_BASE64 = os.getenv("YTDLP_COOKIES_BASE64", "").strip() or os.getenv("YOUTUBE_COOKIES_B64", "").strip()
YTDLP_COOKIES_TEXT = os.getenv("YTDLP_COOKIES_TEXT", "").strip()
YTDLP_COOKIE_FILE = os.getenv("YTDLP_COOKIE_FILE", "").strip()

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "").strip()
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "").strip()
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").strip().rstrip("/")
R2_SIGNED_URL_EXPIRES = int(os.getenv("R2_SIGNED_URL_EXPIRES", str(7 * 24 * 60 * 60)))
R2_CACHE_PREFIX = os.getenv("R2_CACHE_PREFIX", "spotify-cache").strip().strip("/") or "spotify-cache"

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def _safe_name(value: str, fallback: str = "song") -> str:
    clean = re.sub(r"[^A-Za-z0-9._ -]+", "", str(value or fallback)).strip()
    clean = re.sub(r"\s+", " ", clean).strip(" ._-")
    return clean[:140] or fallback


def _clipfx_file_name(value: str) -> str:
    base = re.sub(r"\.mp3$", "", _safe_name(value, "song"), flags=re.IGNORECASE)
    base = re.sub(r"\s*-?\s*ClipFX$", "", base, flags=re.IGNORECASE).strip() or "song"
    return f"{base} ClipFX.mp3"


def _media_type_for(path: Path) -> str:
    return mimetypes.guess_type(str(path))[0] or "audio/mpeg"


def _db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured for the Spotify worker.")
    return psycopg2.connect(DATABASE_URL)


def _r2_client():
    missing = [
        name
        for name, value in {
            "R2_ACCOUNT_ID": R2_ACCOUNT_ID,
            "R2_ACCESS_KEY_ID": R2_ACCESS_KEY_ID,
            "R2_SECRET_ACCESS_KEY": R2_SECRET_ACCESS_KEY,
            "R2_BUCKET_NAME": R2_BUCKET_NAME,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing R2 worker env vars: {', '.join(missing)}")

    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def _download_url_for_key(client: Any, key: str, filename: str, media_type: str) -> str:
    if R2_PUBLIC_BASE_URL:
        return f"{R2_PUBLIC_BASE_URL}/{quote(key, safe='/')}"

    return client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": R2_BUCKET_NAME,
            "Key": key,
            "ResponseContentType": media_type,
            "ResponseContentDisposition": f'attachment; filename="{filename}"',
        },
        ExpiresIn=R2_SIGNED_URL_EXPIRES,
    )


def _write_cookie_file(output_dir: Path) -> str | None:
    if YTDLP_COOKIE_FILE:
        if Path(YTDLP_COOKIE_FILE).exists():
            return YTDLP_COOKIE_FILE
        logger.warning("Configured YTDLP_COOKIE_FILE does not exist: %s", YTDLP_COOKIE_FILE)
        return None

    cookie_text = ""
    if YTDLP_COOKIES_BASE64:
        cookie_text = base64.b64decode(YTDLP_COOKIES_BASE64).decode("utf-8")
    elif YTDLP_COOKIES_TEXT:
        cookie_text = YTDLP_COOKIES_TEXT.replace("\\n", "\n")

    if not cookie_text.strip():
        return None

    cookie_path = output_dir / "youtube-cookies.txt"
    cookie_path.write_text(cookie_text, encoding="utf-8")
    return str(cookie_path)


def _find_audio_file(output_dir: Path) -> Path:
    candidates: list[Path] = []
    for pattern in ("*.mp3", "*.m4a", "*.opus", "*.ogg", "*.wav", "*.webm"):
        candidates.extend(output_dir.rglob(pattern))
    if not candidates:
        raise RuntimeError("Downloader finished without producing an audio file.")
    return max(candidates, key=lambda path: path.stat().st_size)


def _run_ytdlp(youtube_url: str, output_dir: Path) -> Path:
    cookie_file = _write_cookie_file(output_dir)
    output_template = str(output_dir / "%(title).180s.%(ext)s")
    clients = YTDLP_PLAYER_CLIENTS or ["ios", "android", "web", "tv"]
    last_error = ""

    # If producer sent ytsearch1:, keep that query. If it sent a normal URL, yt-dlp handles it too.
    for client in clients:
        cmd = [
            "python",
            "-m",
            "yt_dlp",
            youtube_url,
            "--extract-audio",
            "--audio-format",
            "mp3",
            "--audio-quality",
            "128K",
            "--no-playlist",
            "--force-overwrites",
            "--no-warnings",
            "--no-check-certificate",
            "--output",
            output_template,
            "--extractor-args",
            f"youtube:player_client={client}",
        ]
        if youtube_url.startswith("ytsearch"):
            cmd.extend(["--playlist-items", f"1-{YTDLP_SEARCH_LIMIT}"])
        if cookie_file:
            cmd.extend(["--cookies", cookie_file])
        if YTDLP_PROXY:
            cmd.extend(["--proxy", YTDLP_PROXY])

        logger.info("Running yt-dlp client=%s query=%s", client, youtube_url)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=DOWNLOAD_TIMEOUT)
        combined = "\n".join([result.stdout or "", result.stderr or ""])
        last_error = combined[-3000:]
        if result.returncode == 0:
            try:
                return _find_audio_file(output_dir)
            except RuntimeError as exc:
                last_error = f"{exc}\n{last_error}"
                continue
        logger.warning("yt-dlp failed for client=%s: %s", client, last_error[-1000:])

    raise RuntimeError(last_error or "yt-dlp failed for every configured client.")


def _upload_to_r2(audio_file: Path, spotify_id: str, filename: str) -> tuple[str, str]:
    client = _r2_client()
    media_type = _media_type_for(audio_file)
    key = f"{R2_CACHE_PREFIX}/track/{spotify_id}/audio.mp3"
    client.upload_file(
        str(audio_file),
        R2_BUCKET_NAME,
        key,
        ExtraArgs={
            "ContentType": media_type,
            "ContentDisposition": f'attachment; filename="{filename}"',
            "CacheControl": "public, max-age=31536000, immutable",
            "Metadata": {"spotify-id": spotify_id, "source": "clipfx-spotify-worker"},
        },
    )
    return _download_url_for_key(client, key, filename, media_type), key


def _mark_processing(spotify_id: str, youtube_url: str) -> None:
    with _db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE music_tracks
               SET download_status = 'processing',
                   download_error = NULL,
                   metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                   updated_at = now()
             WHERE spotify_track_id = %s
            """,
            [f'{ {"worker_started": True, "youtube_url": youtube_url} }'.replace("'", '"'), spotify_id],
        )


def _mark_ready(spotify_id: str, download_url: str, filename: str, r2_key: str) -> None:
    with _db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE music_tracks
               SET r2_download_url = %s,
                   file_name = %s,
                   download_status = 'ready',
                   download_error = NULL,
                   metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                   updated_at = now()
             WHERE spotify_track_id = %s
            """,
            [download_url, filename, f'{ {"r2_key": r2_key, "worker_completed": True} }'.replace("'", '"'), spotify_id],
        )


def _mark_failed(spotify_id: str, error: Exception) -> None:
    message = str(error)[:1200] or "Spotify worker failed."
    with _db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE music_tracks
               SET download_status = 'failed',
                   download_error = %s,
                   metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                   updated_at = now()
             WHERE spotify_track_id = %s
            """,
            [message, f'{ {"worker_failed": True} }'.replace("'", '"'), spotify_id],
        )


def process_track(youtube_url: str, spotify_id: str, track_name: str | None = None, artist_name: str | None = None) -> dict[str, str]:
    """RQ entrypoint called by the producer service."""

    clean_spotify_id = str(spotify_id or "").strip()
    if not clean_spotify_id:
        raise ValueError("spotify_id is required")

    title = " - ".join(part for part in [artist_name, track_name] if part) or clean_spotify_id
    filename = _clipfx_file_name(title)
    tmp_dir = Path(tempfile.mkdtemp(prefix="clipfx_spotify_"))

    try:
        _mark_processing(clean_spotify_id, youtube_url)
        audio_file = _run_ytdlp(youtube_url, tmp_dir)
        download_url, r2_key = _upload_to_r2(audio_file, clean_spotify_id, filename)
        _mark_ready(clean_spotify_id, download_url, filename, r2_key)
        logger.info("Spotify track ready spotify_id=%s r2_key=%s", clean_spotify_id, r2_key)
        return {"status": "ready", "spotify_id": clean_spotify_id, "download_url": download_url, "file_name": filename}
    except Exception as exc:
        logger.exception("Spotify worker failed spotify_id=%s", clean_spotify_id)
        _mark_failed(clean_spotify_id, exc)
        raise
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
