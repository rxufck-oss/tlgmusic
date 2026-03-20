import asyncio
import base64
import json
import logging
import os
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import urllib.parse
import urllib.error
import uuid
import threading

from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Update,
)
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)
from flask import Flask, jsonify, request, send_from_directory

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TEMP_DIR = os.getenv("TEMP_DIR", os.path.join(tempfile.gettempdir(), "music_bot"))
PROXY = os.getenv("YTDLP_PROXY", "").strip()
DB_PATH = os.getenv("DB_PATH", "/app/music_bot.db")
MAX_SEARCH_RESULTS = int(os.getenv("MAX_SEARCH_RESULTS", "20"))
BOT_RESULTS_LIMIT = int(os.getenv("BOT_RESULTS_LIMIT", "10"))
ARTIST_SEARCH_RESULTS = int(os.getenv("ARTIST_SEARCH_RESULTS", "40"))
DOWNLOAD_CACHE_TTL = int(os.getenv("DOWNLOAD_CACHE_TTL", "3600"))
SEARCH_COOLDOWN_SEC = int(os.getenv("SEARCH_COOLDOWN_SEC", "3"))
SC_SEARCH_TIMEOUT_SEC = int(os.getenv("SC_SEARCH_TIMEOUT_SEC", "20"))
WEBAPP_API_PORT = int(os.getenv("WEBAPP_API_PORT", "8080"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8443"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram").strip()
SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", "300"))
SEARCH_CACHE_MAX_ITEMS = int(os.getenv("SEARCH_CACHE_MAX_ITEMS", "200"))
NEW_RELEASES_CACHE_TTL = int(os.getenv("NEW_RELEASES_CACHE_TTL", "600"))
WEBAPP_STATIC_DIR = os.getenv("WEBAPP_STATIC_DIR", "").strip()
SPOTIFY_META_LIMIT = int(os.getenv("SPOTIFY_META_LIMIT", "10"))
SPOTIFY_ARTIST_ALBUM_LIMIT = int(os.getenv("SPOTIFY_ARTIST_ALBUM_LIMIT", "25"))
SPOTIFY_ARTIST_TRACK_LIMIT = int(os.getenv("SPOTIFY_ARTIST_TRACK_LIMIT", "200"))
SPOTIFY_ARTIST_EXPAND_ALBUMS = os.getenv("SPOTIFY_ARTIST_EXPAND_ALBUMS", "1").strip() == "1"
TRENDING_ARTISTS = [
    a.strip()
    for a in os.getenv(
        "TRENDING_ARTISTS",
        "Travis Scott,The Weeknd,Drake,Taylor Swift,Billie Eilish,"
        "Post Malone,Dua Lipa,Ed Sheeran,Doja Cat,Bad Bunny",
    ).split(",")
    if a.strip()
]
SC_NEW_RELEASES_LIMIT = int(os.getenv("SC_NEW_RELEASES_LIMIT", "30"))
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "").strip()
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "").strip()
SPOTIFY_PROXY = os.getenv("SPOTIFY_PROXY", "").strip()
SPOTIFY_ARTIST_CACHE_TTL = int(os.getenv("SPOTIFY_ARTIST_CACHE_TTL", "900"))
SPOTIFY_ALBUM_TRACKS_CACHE_TTL = int(os.getenv("SPOTIFY_ALBUM_TRACKS_CACHE_TTL", "21600"))
SPOTIFY_MIN_INTERVAL_MS = int(os.getenv("SPOTIFY_MIN_INTERVAL_MS", "150"))
SPOTIFY_MAX_CONCURRENCY = int(os.getenv("SPOTIFY_MAX_CONCURRENCY", "1"))
SC_CLIENT_ID = os.getenv("SC_CLIENT_ID", "").strip()
SC_API_TIMEOUT_SEC = int(os.getenv("SC_API_TIMEOUT_SEC", "12"))
MAX_TRACK_DURATION_SEC = int(os.getenv("MAX_TRACK_DURATION_SEC", "0"))
MIN_TRACK_DURATION_SEC = int(os.getenv("MIN_TRACK_DURATION_SEC", "0"))
YTDLP_AUDIO_BITRATE = int(os.getenv("YTDLP_AUDIO_BITRATE", "64"))
YTDLP_FORMAT = os.getenv("YTDLP_FORMAT", "bestaudio[abr<=96]/bestaudio").strip()
YTDLP_CONCURRENT_FRAGMENTS = int(os.getenv("YTDLP_CONCURRENT_FRAGMENTS", "8"))
FFMPEG_THREADS = int(os.getenv("FFMPEG_THREADS", "2"))
SEND_THUMBNAIL = os.getenv("SEND_THUMBNAIL", "0").strip() == "1"
TELEGRAM_POOL_SIZE = int(os.getenv("TELEGRAM_POOL_SIZE", "20"))
TELEGRAM_POOL_TIMEOUT = float(os.getenv("TELEGRAM_POOL_TIMEOUT", "30"))

os.makedirs(TEMP_DIR, exist_ok=True)
DOWNLOAD_CACHE: dict[str, dict] = {}
USER_LAST_SEARCH_TS: dict[int, float] = {}
SEARCH_CACHE: dict[str, dict] = {}
SPOTIFY_TOKEN_CACHE: dict[str, float | str] = {"token": "", "expires_at": 0}
SPOTIFY_ARTIST_CACHE: dict[str, dict] = {}
SPOTIFY_ALBUM_TRACKS_CACHE: dict[str, dict] = {}
SPOTIFY_RATE_LIMITED_UNTIL = 0.0
SPOTIFY_LAST_REQUEST_AT = 0.0
SPOTIFY_LOCK = threading.Lock()
SPOTIFY_SEM = threading.Semaphore(max(1, SPOTIFY_MAX_CONCURRENCY))
NEW_RELEASES_CACHE: dict[str, dict] = {}
DB_CONN = None
HTTP_API_THREAD = None
SEND_LOOP = None
SEND_LOOP_THREAD = None
SEND_LOOP_READY = threading.Event()
SEND_BOT = None


def _send_loop_worker():
    global SEND_LOOP, SEND_BOT
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    SEND_LOOP = loop
    request = HTTPXRequest(
        connection_pool_size=TELEGRAM_POOL_SIZE,
        pool_timeout=TELEGRAM_POOL_TIMEOUT,
    )
    SEND_BOT = Bot(BOT_TOKEN, request=request)
    SEND_LOOP_READY.set()
    loop.run_forever()


def ensure_send_loop():
    global SEND_LOOP_THREAD
    if SEND_LOOP_THREAD and SEND_LOOP_READY.is_set():
        return
    SEND_LOOP_THREAD = threading.Thread(target=_send_loop_worker, daemon=True)
    SEND_LOOP_THREAD.start()
    SEND_LOOP_READY.wait(timeout=10)


def init_db() -> None:
    global DB_CONN
    DB_CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
    DB_CONN.execute(
        """
        CREATE TABLE IF NOT EXISTS audio_cache (
            track_key TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    DB_CONN.commit()


def make_track_key(item: dict) -> str:
    url = (item.get("url") or "").strip()
    source = (item.get("source") or "soundcloud").strip()
    artist = (item.get("artist") or "").strip()
    title = (item.get("title") or "").strip()
    if url:
        return f"{source}|{url}"
    return f"{source}|{artist}|{title}"


def get_cached_file_id(track_key: str) -> str | None:
    cur = DB_CONN.cursor()
    cur.execute("SELECT file_id FROM audio_cache WHERE track_key = ?", (track_key,))
    row = cur.fetchone()
    return row[0] if row else None


def save_cached_file_id(track_key: str, file_id: str) -> None:
    DB_CONN.execute(
        """
        INSERT INTO audio_cache (track_key, file_id, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(track_key) DO UPDATE SET
          file_id=excluded.file_id,
          updated_at=excluded.updated_at
        """,
        (track_key, file_id, int(time.time())),
    )
    DB_CONN.commit()


def prune_download_cache() -> None:
    now = time.time()
    stale = [k for k, v in DOWNLOAD_CACHE.items() if now - v.get("created_at", now) > DOWNLOAD_CACHE_TTL]
    for k in stale:
        DOWNLOAD_CACHE.pop(k, None)


def make_search_cache_key(
    query: str, limit: int, artist_mode: bool, include_covers: bool, source: str, offset: int
) -> str:
    return f"{source}|{query.lower().strip()}|{limit}|{offset}|{int(artist_mode)}|{int(include_covers)}"


def prune_search_cache() -> None:
    now = time.time()
    stale_keys = [k for k, v in SEARCH_CACHE.items() if now - v.get("created_at", now) > SEARCH_CACHE_TTL]
    for k in stale_keys:
        SEARCH_CACHE.pop(k, None)

    if len(SEARCH_CACHE) > SEARCH_CACHE_MAX_ITEMS:
        # Drop oldest entries first.
        old_items = sorted(SEARCH_CACHE.items(), key=lambda item: item[1].get("created_at", 0))
        over = len(SEARCH_CACHE) - SEARCH_CACHE_MAX_ITEMS
        for k, _ in old_items[:over]:
            SEARCH_CACHE.pop(k, None)


def get_search_cache(key: str) -> list | None:
    prune_search_cache()
    item = SEARCH_CACHE.get(key)
    if not item:
        return None
    return item.get("results")


def set_search_cache(key: str, results: list) -> None:
    prune_search_cache()
    SEARCH_CACHE[key] = {"results": results, "created_at": time.time()}


def put_download_item(
    url: str,
    title: str,
    artist: str | None = None,
    cover_url: str | None = None,
    source: str = "soundcloud",
) -> str:
    prune_download_cache()
    key = uuid.uuid4().hex[:12]
    DOWNLOAD_CACHE[key] = {
        "url": url,
        "title": title,
        "artist": artist,
        "cover_url": cover_url,
        "source": source or "soundcloud",
        "created_at": time.time(),
    }
    return key


def get_download_item(key: str) -> dict | None:
    prune_download_cache()
    return DOWNLOAD_CACHE.pop(key, None)


def build_common_yt_dlp_args() -> list[str]:
    args = ["--no-warnings"]
    if PROXY:
        args.extend(["--proxy", PROXY])
    return args


def http_json_request(
    url: str,
    method: str = "GET",
    headers: dict | None = None,
    data: bytes | None = None,
    timeout: int = 25,
    proxy: str | None = None,
    max_retries: int = 2,
) -> dict | None:
    attempt = 0
    is_spotify = "api.spotify.com" in (url or "") or "accounts.spotify.com" in (url or "")
    global SPOTIFY_RATE_LIMITED_UNTIL
    while True:
        try:
            if is_spotify:
                SPOTIFY_SEM.acquire()
                with SPOTIFY_LOCK:
                    now = time.time()
                    wait_s = max(0.0, (SPOTIFY_MIN_INTERVAL_MS / 1000.0) - (now - SPOTIFY_LAST_REQUEST_AT))
                    if wait_s > 0:
                        time.sleep(wait_s)
                    SPOTIFY_LAST_REQUEST_AT = time.time()
            req = urllib.request.Request(url, headers=headers or {}, method=method, data=data)
            opener = None
            if proxy:
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
            if opener:
                resp = opener.open(req, timeout=timeout)
            else:
                resp = urllib.request.urlopen(req, timeout=timeout)
            with resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries:
                retry_after = 0
                try:
                    retry_after = int(e.headers.get("Retry-After", "0"))
                except (TypeError, ValueError):
                    retry_after = 0
                delay = retry_after if retry_after > 0 else (2 ** attempt)
                SPOTIFY_RATE_LIMITED_UNTIL = max(SPOTIFY_RATE_LIMITED_UNTIL, time.time() + delay)
                logger.warning("HTTP 429 rate limit (%s), retrying in %ss", url, delay)
                time.sleep(min(delay, 10))
                attempt += 1
                continue
            logger.error("HTTP JSON request failed (%s): %s", url, e)
            return None
        except Exception as e:
            logger.error("HTTP JSON request failed (%s): %s", url, e)
            return None
        finally:
            if is_spotify:
                try:
                    SPOTIFY_SEM.release()
                except Exception:
                    pass


def is_spotify_configured() -> bool:
    return bool(SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET)


def get_spotify_token() -> str | None:
    if not is_spotify_configured():
        return None

    now = time.time()
    cached_token = str(SPOTIFY_TOKEN_CACHE.get("token") or "")
    expires_at = float(SPOTIFY_TOKEN_CACHE.get("expires_at") or 0)
    if cached_token and expires_at - 20 > now:
        return cached_token

    auth_raw = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode("utf-8")
    auth_b64 = base64.b64encode(auth_raw).decode("ascii")
    headers = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    token_payload = http_json_request(
        "https://accounts.spotify.com/api/token",
        method="POST",
        headers=headers,
        data=b"grant_type=client_credentials",
        proxy=SPOTIFY_PROXY or None,
    )
    if not token_payload:
        return None

    token = str(token_payload.get("access_token") or "")
    expires_in = int(token_payload.get("expires_in") or 3600)
    if not token:
        return None
    SPOTIFY_TOKEN_CACHE["token"] = token
    SPOTIFY_TOKEN_CACHE["expires_at"] = now + expires_in
    return token


def get_new_releases_cache_key(limit: int, country: str) -> str:
    return f"{country.upper()}|{limit}"


def get_cached_new_releases(key: str) -> dict | None:
    item = NEW_RELEASES_CACHE.get(key)
    if not item:
        return None
    if time.time() - item.get("created_at", 0) > NEW_RELEASES_CACHE_TTL:
        NEW_RELEASES_CACHE.pop(key, None)
        return None
    return item.get("payload")


def set_cached_new_releases(key: str, payload: dict) -> None:
    albums = payload.get("albums") or []
    tracks = payload.get("tracks") or []
    if not albums and not tracks:
        return
    NEW_RELEASES_CACHE[key] = {"payload": payload, "created_at": time.time()}


def get_spotify_new_releases(limit: int = 12, country: str = "US") -> dict:
    token = get_spotify_token()
    if not token:
        return {"albums": [], "tracks": []}

    safe_limit = max(1, min(limit, 30))
    safe_country = (country or "US").upper()
    cache_key = get_new_releases_cache_key(safe_limit, safe_country)
    cached = get_cached_new_releases(cache_key)
    if cached is not None:
        return cached

    url = (
        "https://api.spotify.com/v1/browse/new-releases"
        f"?limit={safe_limit}&country={urllib.parse.quote(safe_country)}"
    )
    payload = http_json_request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        proxy=SPOTIFY_PROXY or None,
    )
    if not payload:
        payload = {"albums": {"items": []}}

    albums_raw = ((payload.get("albums") or {}).get("items")) or []
    if not albums_raw:
        album_search = http_json_request(
            f"https://api.spotify.com/v1/search?q=tag%3Anew&type=album&limit={safe_limit}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            proxy=SPOTIFY_PROXY or None,
        )
        albums_raw = (((album_search or {}).get("albums") or {}).get("items")) or []
    if not albums_raw:
        current_year = time.gmtime().tm_year
        for year in (current_year, current_year - 1):
            year_search = http_json_request(
                f"https://api.spotify.com/v1/search?q=year%3A{year}&type=album&limit={safe_limit}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
                proxy=SPOTIFY_PROXY or None,
            )
            albums_raw = (((year_search or {}).get("albums") or {}).get("items")) or []
            if albums_raw:
                break
    if not albums_raw:
        album_fallback = http_json_request(
            f"https://api.spotify.com/v1/search?q=new%20music&type=album&limit={safe_limit}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            proxy=SPOTIFY_PROXY or None,
        )
        albums_raw = (((album_fallback or {}).get("albums") or {}).get("items")) or []
    albums = []
    tracks = []
    for album in albums_raw:
        artists = album.get("artists") or []
        artist = ", ".join([a.get("name", "") for a in artists if a.get("name")]).strip() or "Unknown Artist"
        images = album.get("images") or []
        cover = images[0].get("url") if images else None
        albums.append(
            {
                "type": "album",
                "album_id": album.get("id"),
                "album_name": album.get("name"),
                "artist": artist,
                "cover_url": cover,
                "release_date": album.get("release_date"),
                "url": ((album.get("external_urls") or {}).get("spotify") or ""),
                "source": "spotify",
            }
        )

    # Use a lightweight "new" search to get track cards without per-album requests.
    track_search = http_json_request(
        f"https://api.spotify.com/v1/search?q=tag%3Anew&type=track&limit={safe_limit}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        proxy=SPOTIFY_PROXY or None,
    )
    items = (((track_search or {}).get("tracks") or {}).get("items")) or []
    if not items:
        current_year = time.gmtime().tm_year
        for year in (current_year, current_year - 1):
            track_year = http_json_request(
                f"https://api.spotify.com/v1/search?q=year%3A{year}&type=track&limit={safe_limit}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
                proxy=SPOTIFY_PROXY or None,
            )
            items = (((track_year or {}).get("tracks") or {}).get("items")) or []
            if items:
                break
    if not items:
        track_fallback = http_json_request(
            f"https://api.spotify.com/v1/search?q=new%20music&type=track&limit={safe_limit}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            proxy=SPOTIFY_PROXY or None,
        )
        items = (((track_fallback or {}).get("tracks") or {}).get("items")) or []

    if not albums_raw and not items:
        trending_tracks = []
        for artist in TRENDING_ARTISTS:
            artist_query = f"artist:{artist}"
            results = search_spotify(artist_query, limit=1, include_meta=True, offset=0)
            if results:
                trending_tracks.extend(results)
            if len(trending_tracks) >= safe_limit:
                break
        items = [
            {
                "id": t.get("spotify_id") or t.get("id"),
                "name": t.get("title"),
                "artists": [{"name": t.get("artist")}],
                "duration_ms": None,
                "external_urls": {"spotify": t.get("spotify_url") or t.get("url")},
                "album": {
                    "name": t.get("album_name"),
                    "id": t.get("album_id"),
                    "release_date": t.get("release_date"),
                    "images": [{"url": t.get("cover_url")}],
                },
            }
            for t in trending_tracks
        ]
    for it in items:
        artists = it.get("artists") or []
        artist = ", ".join([a.get("name", "") for a in artists if a.get("name")]).strip() or "Unknown Artist"
        duration_ms = int(it.get("duration_ms") or 0)
        mins = duration_ms // 60000
        secs = (duration_ms % 60000) // 1000
        images = (it.get("album") or {}).get("images") or []
        cover = images[0].get("url") if images else None
        tracks.append(
            {
                "type": "track",
                "id": it.get("id"),
                "spotify_id": it.get("id"),
                "title": it.get("name", "Без названия"),
                "artist": artist,
                "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
                "url": ((it.get("external_urls") or {}).get("spotify") or ""),
                "cover_url": cover,
                "source": "spotify",
            }
        )

    result = {"albums": albums, "tracks": tracks}
    set_cached_new_releases(cache_key, result)
    return result


def search_spotify(query: str, limit: int = 20, include_meta: bool = False, offset: int = 0) -> list:
    token = get_spotify_token()
    if not token:
        return []

    safe_limit = max(1, min(limit, 50))
    safe_offset = max(0, min(offset, 1000))
    encoded_q = urllib.parse.quote(query)
    url = (
        f"https://api.spotify.com/v1/search?q={encoded_q}&type=track&limit={safe_limit}"
        f"&offset={safe_offset}"
    )
    payload = http_json_request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        proxy=SPOTIFY_PROXY or None,
    )
    if not payload:
        return []

    items = (((payload.get("tracks") or {}).get("items")) or [])
    out = []
    for it in items:
        artists = it.get("artists") or []
        artist = ", ".join([a.get("name", "") for a in artists if a.get("name")]).strip() or "Unknown Artist"
        images = (it.get("album") or {}).get("images") or []
        cover = images[0].get("url") if images else None
        duration_ms = int(it.get("duration_ms") or 0)
        mins = duration_ms // 60000
        secs = (duration_ms % 60000) // 1000
        item = {
            "id": it.get("id"),
            "spotify_id": it.get("id"),
            "title": it.get("name", "Без названия"),
            "artist": artist,
            "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
            "url": ((it.get("external_urls") or {}).get("spotify") or ""),
            "cover_url": cover,
            "source": "spotify",
        }
        if include_meta:
            album = it.get("album") or {}
            external_ids = it.get("external_ids") or {}
            item.update(
                {
                    "album_name": album.get("name"),
                    "album_id": album.get("id"),
                    "release_date": album.get("release_date"),
                    "popularity": it.get("popularity"),
                    "explicit": it.get("explicit"),
                    "preview_url": it.get("preview_url"),
                    "isrc": external_ids.get("isrc"),
                }
            )
        out.append(item)
    return [x for x in out if x.get("url")]


def normalize_artist_name(name: str) -> str:
    name = (name or "").replace("$", "s").replace("&", "and")
    return "".join(ch.lower() if ch.isalnum() else "" for ch in name)


def get_cached_artist_payload(query: str) -> dict | None:
    if SPOTIFY_ARTIST_CACHE_TTL <= 0:
        return None
    key = normalize_artist_name(query)
    cached = SPOTIFY_ARTIST_CACHE.get(key)
    if not cached:
        return None
    if time.time() - cached.get("created_at", 0) > SPOTIFY_ARTIST_CACHE_TTL:
        SPOTIFY_ARTIST_CACHE.pop(key, None)
        return None
    return cached.get("payload")


def set_cached_artist_payload(query: str, payload: dict) -> None:
    if SPOTIFY_ARTIST_CACHE_TTL <= 0:
        return
    key = normalize_artist_name(query)
    SPOTIFY_ARTIST_CACHE[key] = {"payload": payload, "created_at": time.time()}


def search_spotify_artist_candidates(query: str, limit: int = 5) -> list:
    token = get_spotify_token()
    if not token or not query:
        return []
    safe_limit = max(1, min(limit, 10))
    encoded_q = urllib.parse.quote(query)
    url = f"https://api.spotify.com/v1/search?q={encoded_q}&type=artist&limit={safe_limit}"
    payload = http_json_request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        proxy=SPOTIFY_PROXY or None,
    )
    items = (((payload or {}).get("artists") or {}).get("items")) or []
    out = []
    for artist in items:
        images = artist.get("images") or []
        out.append(
            {
                "id": artist.get("id"),
                "name": artist.get("name"),
                "image": images[0].get("url") if images else None,
                "followers": (artist.get("followers") or {}).get("total"),
                "genres": artist.get("genres") or [],
                "url": ((artist.get("external_urls") or {}).get("spotify") or ""),
            }
        )
    return out


def search_spotify_artist(query: str) -> dict | None:
    if not query:
        return None
    candidates = search_spotify_artist_candidates(query, limit=8)
    if not candidates:
        alt = query.replace("ASAP", "A$AP").replace("A$AP", "A$AP").strip()
        if alt and alt.lower() != query.lower():
            candidates = search_spotify_artist_candidates(alt, limit=8)
    if not candidates:
        return None
    qn = normalize_artist_name(query)
    for artist in candidates:
        if qn and qn == normalize_artist_name(artist.get("name", "")):
            return artist
    for artist in candidates:
        if qn and qn in normalize_artist_name(artist.get("name", "")):
            return artist
    return candidates[0]


def get_spotify_artist_albums(artist_id: str, limit: int = 20) -> list:
    token = get_spotify_token()
    if not token or not artist_id:
        return []
    safe_limit = max(1, min(limit, 50))
    url = (
        f"https://api.spotify.com/v1/artists/{artist_id}/albums"
        f"?include_groups=album,single&limit={safe_limit}"
    )
    payload = http_json_request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        proxy=SPOTIFY_PROXY or None,
    )
    items = (payload or {}).get("items") or []
    albums = []
    for it in items:
        images = it.get("images") or []
        albums.append(
            {
                "id": it.get("id"),
                "name": it.get("name"),
                "release_date": it.get("release_date"),
                "total_tracks": it.get("total_tracks"),
                "cover_url": images[0].get("url") if images else None,
                "url": ((it.get("external_urls") or {}).get("spotify") or ""),
            }
        )
    return albums


def get_spotify_album_tracks(album_id: str) -> list:
    token = get_spotify_token()
    if not token or not album_id:
        return []
    if SPOTIFY_ALBUM_TRACKS_CACHE_TTL > 0:
        cached = SPOTIFY_ALBUM_TRACKS_CACHE.get(album_id)
        if cached and time.time() - cached.get("created_at", 0) <= SPOTIFY_ALBUM_TRACKS_CACHE_TTL:
            return cached.get("tracks") or []
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=50"
    payload = http_json_request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        proxy=SPOTIFY_PROXY or None,
    )
    items = (payload or {}).get("items") or []
    tracks = []
    for it in items:
        artists = it.get("artists") or []
        artist = ", ".join([a.get("name", "") for a in artists if a.get("name")]).strip() or "Unknown Artist"
        duration_ms = int(it.get("duration_ms") or 0)
        duration_sec = duration_ms / 1000 if duration_ms else 0
        if MIN_TRACK_DURATION_SEC and duration_sec and duration_sec < MIN_TRACK_DURATION_SEC:
            continue
        mins = duration_ms // 60000
        secs = (duration_ms % 60000) // 1000
        external_urls = it.get("external_urls") or {}
        url = external_urls.get("spotify") or (f"https://open.spotify.com/track/{it.get('id')}" if it.get("id") else "")
        tracks.append(
            {
                "id": it.get("id"),
                "title": it.get("name", "Без названия"),
                "artist": artist,
                "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
                "url": url,
                "source": "spotify",
            }
        )
    if SPOTIFY_ALBUM_TRACKS_CACHE_TTL > 0 and tracks:
        SPOTIFY_ALBUM_TRACKS_CACHE[album_id] = {"tracks": tracks, "created_at": time.time()}
    return tracks


def build_artist_catalog_from_artist(artist_id: str) -> list:
    if not artist_id:
        return []
    albums = get_spotify_artist_albums(artist_id, limit=SPOTIFY_ARTIST_ALBUM_LIMIT)
    if not albums:
        return []
    out = []
    for album in albums:
        album_id = album.get("id")
        tracks = get_spotify_album_tracks(album_id) if album_id else []
        if not tracks:
            continue
        out.append(
            {
                "id": album_id,
                "name": album.get("name") or "Альбом",
                "release_date": album.get("release_date"),
                "cover_url": album.get("cover_url"),
                "tracks": tracks,
            }
        )
    return out


def build_artist_catalog_from_search(query: str, limit: int = 50) -> list:
    if not query:
        return []
    max_limit = max(20, min(limit, SPOTIFY_ARTIST_TRACK_LIMIT))
    results = []
    offset = 0
    while offset < max_limit:
        batch = search_spotify(query, limit=min(50, max_limit - offset), include_meta=True, offset=offset)
        if not batch:
            break
        results.extend(batch)
        if len(batch) < 50:
            break
        offset += 50
    if not results:
        return []
    albums_map = {}
    ql = normalize_artist_name(query)
    for track in results:
        artist_name = normalize_artist_name(track.get("artist") or "")
        if ql and ql not in artist_name:
            continue
        if not track.get("album_id") or not track.get("album_name"):
            meta = spotify_lookup_track(track.get("title", ""), track.get("artist"))
            if meta:
                track["album_id"] = meta.get("album_id") or track.get("album_id")
                track["album_name"] = meta.get("album_name") or track.get("album_name")
                track["cover_url"] = meta.get("cover_url") or track.get("cover_url")
                track["release_date"] = meta.get("release_date") or track.get("release_date")
        album_id = track.get("album_id") or track.get("album_name") or "singles"
        album = albums_map.get(album_id)
        if not album:
            album = {
                "id": track.get("album_id"),
                "name": track.get("album_name") or "Синглы",
                "release_date": track.get("release_date"),
                "cover_url": track.get("cover_url"),
                "tracks": [],
            }
            albums_map[album_id] = album
        album["tracks"].append(
            {
                "id": track.get("spotify_id") or track.get("id"),
                "title": track.get("title"),
                "artist": track.get("artist"),
                "duration": track.get("duration"),
                "url": track.get("url"),
                "source": "spotify",
            }
        )
    albums = list(albums_map.values())
    for album in albums:
        seen = set()
        deduped = []
        for t in album["tracks"]:
            key = f"{(t.get('title') or '').lower()}|{(t.get('artist') or '').lower()}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(t)
        album["tracks"] = deduped
    if SPOTIFY_ARTIST_EXPAND_ALBUMS:
        for album in albums[: min(len(albums), SPOTIFY_ARTIST_ALBUM_LIMIT)]:
            album_id = album.get("id")
            if not album_id:
                continue
            full_tracks = get_spotify_album_tracks(album_id)
            if full_tracks:
                album["tracks"] = full_tracks
    return albums


def spotify_lookup_track(title: str, artist: str | None = None) -> dict | None:
    token = get_spotify_token()
    if not token:
        return None

    query = f"{artist or ''} {title or ''}".strip()
    if not query:
        return None

    encoded_q = urllib.parse.quote(query)
    url = f"https://api.spotify.com/v1/search?q={encoded_q}&type=track&limit=1"
    payload = http_json_request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        proxy=SPOTIFY_PROXY or None,
    )
    if not payload:
        return None

    items = (((payload.get("tracks") or {}).get("items")) or [])
    if not items:
        return None

    it = items[0]
    artists = it.get("artists") or []
    artist_name = ", ".join([a.get("name", "") for a in artists if a.get("name")]).strip() or "Unknown Artist"
    images = (it.get("album") or {}).get("images") or []
    cover = images[0].get("url") if images else None
    external_ids = it.get("external_ids") or {}
    album = it.get("album") or {}
    return {
        "spotify_id": it.get("id"),
        "spotify_url": ((it.get("external_urls") or {}).get("spotify") or ""),
        "artist": artist_name,
        "cover_url": cover,
        "album_name": album.get("name"),
        "album_id": album.get("id"),
        "release_date": album.get("release_date"),
        "popularity": it.get("popularity"),
        "explicit": it.get("explicit"),
        "preview_url": it.get("preview_url"),
        "isrc": external_ids.get("isrc"),
    }


def resolve_spotify_to_soundcloud(track_url: str, title: str | None = None, artist: str | None = None) -> str | None:
    query = f"{artist or ''} {title or ''}".strip()
    if not query and "open.spotify.com/track/" in (track_url or ""):
        token = get_spotify_token()
        if token:
            parsed = urllib.parse.urlparse(track_url)
            parts = [p for p in (parsed.path or "").split("/") if p]
            if len(parts) >= 2 and parts[0] == "track":
                spotify_id = parts[1]
                meta = http_json_request(
                    f"https://api.spotify.com/v1/tracks/{spotify_id}",
                    headers={"Authorization": f"Bearer {token}"},
                )
                if meta:
                    m_title = str(meta.get("name") or "")
                    m_artists = meta.get("artists") or []
                    m_artist = ", ".join([a.get("name", "") for a in m_artists if a.get("name")]).strip()
                    query = f"{m_artist} {m_title}".strip()
    if not query:
        return None

    candidates = search_soundcloud(query, 1, include_covers=False)
    if not candidates:
        return None
    return candidates[0].get("url")


def run_yt_dlp(cmd: list[str], timeout: int) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.error("YT-DLP Error (cmd=%s): %s", " ".join(cmd), result.stderr.strip())
        return result
    except subprocess.TimeoutExpired:
        logger.error("YT-DLP Timeout (cmd=%s)", " ".join(cmd))
        return subprocess.CompletedProcess(args=cmd, returncode=124, stdout="", stderr=f"timeout {timeout}s")


def parse_search_results(stdout: str, include_covers: bool = True) -> list:
    results = []
    for line in stdout.strip().split("\n"):
        if not line:
            continue
        try:
            data = json.loads(line)
            duration_sec = data.get("duration")
            if MIN_TRACK_DURATION_SEC and isinstance(duration_sec, (int, float)):
                if duration_sec < MIN_TRACK_DURATION_SEC:
                    continue
            if MAX_TRACK_DURATION_SEC and isinstance(duration_sec, (int, float)):
                if duration_sec > MAX_TRACK_DURATION_SEC:
                    continue
            cover = None
            if include_covers:
                cover = data.get("thumbnail")
                if not cover:
                    thumbs = data.get("thumbnails") or []
                    if thumbs and isinstance(thumbs, list):
                        cover = thumbs[-1].get("url")
            results.append(
                {
                    "id": data.get("id"),
                    "title": data.get("title", "Без названия"),
                    "artist": data.get("uploader") or data.get("channel"),
                    "duration": data.get("duration_string", "?:??"),
                    "duration_sec": duration_sec,
                    "url": data.get("webpage_url"),
                    "cover_url": cover,
                    "source": "soundcloud",
                }
            )
        except json.JSONDecodeError:
            continue
    return [r for r in results if r.get("url")]


def search_soundcloud(query: str, limit: int, include_covers: bool = True) -> list:
    safe_limit = max(1, min(limit, 200))
    timeout_sec = max(SC_SEARCH_TIMEOUT_SEC, 14 + safe_limit // 3)
    cmd = [
        "yt-dlp",
        *build_common_yt_dlp_args(),
        "--flat-playlist",
        "--dump-json",
        "--playlist-end",
        str(safe_limit),
        "--skip-download",
        f"scsearch{safe_limit}:{query}",
    ]
    result = run_yt_dlp(cmd, timeout=timeout_sec)
    if result.returncode != 0:
        return []
    return parse_search_results(result.stdout, include_covers=include_covers)


def is_soundcloud_api_configured() -> bool:
    return bool(SC_CLIENT_ID)


def normalize_sc_cover(url: str | None) -> str | None:
    if not url:
        return None
    # Upgrade artwork size when possible.
    return url.replace("-large", "-t500x500")


def search_soundcloud_api(query: str, limit: int, include_covers: bool = True) -> list:
    if not is_soundcloud_api_configured():
        return []
    safe_limit = max(1, min(limit, 200))
    encoded_q = urllib.parse.quote(query)
    url = (
        "https://api-v2.soundcloud.com/search/tracks"
        f"?q={encoded_q}&limit={safe_limit}&client_id={SC_CLIENT_ID}"
    )
    payload = http_json_request(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=SC_API_TIMEOUT_SEC)
    if not payload:
        return []
    items = payload.get("collection") or []
    results = []
    for it in items:
        artist = (it.get("user") or {}).get("username")
        duration_ms = int(it.get("duration") or 0)
        if MIN_TRACK_DURATION_SEC and duration_ms:
            if duration_ms < MIN_TRACK_DURATION_SEC * 1000:
                continue
        if MAX_TRACK_DURATION_SEC and duration_ms:
            if duration_ms > MAX_TRACK_DURATION_SEC * 1000:
                continue
        mins = duration_ms // 60000
        secs = (duration_ms % 60000) // 1000
        cover = normalize_sc_cover(it.get("artwork_url")) if include_covers else None
        results.append(
            {
                "id": it.get("id"),
                "title": it.get("title", "Без названия"),
                "artist": artist or "Unknown Artist",
                "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
                "url": it.get("permalink_url"),
                "cover_url": cover,
                "source": "soundcloud",
            }
        )
    return [r for r in results if r.get("url")]


def get_soundcloud_new_releases(limit: int = 20) -> list:
    if not is_soundcloud_api_configured():
        return []

    safe_limit = max(1, min(limit, 50))
    url = (
        "https://api-v2.soundcloud.com/charts"
        f"?kind=top&genre=soundcloud%3Agenres%3Aall-music&limit={safe_limit}"
        f"&client_id={urllib.parse.quote(SC_CLIENT_ID)}"
    )
    payload = http_json_request(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=SC_API_TIMEOUT_SEC)
    items = (payload or {}).get("collection") or []
    tracks = []
    for item in items:
        track = item.get("track") or {}
        if not track:
            continue
        title = track.get("title") or "Без названия"
        user = track.get("user") or {}
        artist = user.get("username") or "Unknown Artist"
        cover = normalize_sc_cover(track.get("artwork_url")) or normalize_sc_cover(user.get("avatar_url"))
        duration_ms = int(track.get("duration") or 0)
        mins = duration_ms // 60000
        secs = (duration_ms % 60000) // 1000
        tracks.append(
            {
                "type": "track",
                "id": track.get("id"),
                "title": title,
                "artist": artist,
                "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
                "url": track.get("permalink_url") or "",
                "cover_url": cover,
                "source": "soundcloud",
            }
        )

    if tracks:
        return [t for t in tracks if t.get("url")]

    # Fallback: use search API if charts are unavailable.
    fallback = search_soundcloud_api("new", safe_limit, include_covers=True)
    results = []
    for it in fallback:
        results.append(
            {
                "type": "track",
                "id": it.get("id"),
                "title": it.get("title"),
                "artist": it.get("artist"),
                "duration": it.get("duration"),
                "url": it.get("url"),
                "cover_url": it.get("cover_url"),
                "source": "soundcloud",
            }
        )
    return [t for t in results if t.get("url")]


def search_music(
    query: str,
    limit: int | None = None,
    artist_mode: bool = False,
    include_covers: bool = True,
    source: str = "soundcloud",
    include_meta: bool = False,
    offset: int = 0,
) -> tuple[list, str | None]:
    try:
        target_limit = max(1, min(limit or MAX_SEARCH_RESULTS, 200))
        if artist_mode:
            target_limit = max(target_limit, ARTIST_SEARCH_RESULTS)

        if "soundcloud.com/" in query:
            return [
                {
                    "id": query,
                    "title": "Трек по ссылке SoundCloud",
                    "artist": None,
                    "duration": "?",
                    "url": query,
                    "cover_url": None,
                    "source": "soundcloud",
                }
            ], None

        cache_key = make_search_cache_key(query, target_limit, artist_mode, include_covers, source, offset)
        cached = get_search_cache(cache_key)
        if cached is not None:
            return cached, None

        if source == "spotify":
            if not is_spotify_configured():
                return [], "Spotify не настроен. Добавьте SPOTIFY_CLIENT_ID и SPOTIFY_CLIENT_SECRET."
            spotify_results = search_spotify(query, target_limit, include_meta=include_meta, offset=offset)
            if spotify_results:
                set_search_cache(cache_key, spotify_results)
                return spotify_results, None
            # Fallback to SoundCloud if Spotify is blocked or returns nothing.
            source = "soundcloud"

        videos = search_soundcloud_api(query, target_limit, include_covers=include_covers)
        if not videos:
            videos = search_soundcloud(query, target_limit, include_covers=include_covers)
        if videos:
            if include_meta and is_spotify_configured():
                enriched = []
                limit = max(0, min(SPOTIFY_META_LIMIT, len(videos)))
                for idx, item in enumerate(videos):
                    if limit and idx >= limit:
                        enriched.append(item)
                        continue
                    meta = spotify_lookup_track(item.get("title", ""), item.get("artist"))
                    if meta:
                        item.update(
                            {
                                "spotify_id": meta.get("spotify_id"),
                                "spotify_url": meta.get("spotify_url"),
                                "album_name": meta.get("album_name"),
                                "album_id": meta.get("album_id"),
                                "release_date": meta.get("release_date"),
                                "popularity": meta.get("popularity"),
                                "explicit": meta.get("explicit"),
                                "preview_url": meta.get("preview_url"),
                                "isrc": meta.get("isrc"),
                                "cover_url": meta.get("cover_url") or item.get("cover_url"),
                            }
                        )
                    enriched.append(item)
                videos = enriched
            set_search_cache(cache_key, videos)
            return videos, None
        return [], "SoundCloud не вернул результаты. Попробуйте другой запрос."
    except Exception as e:
        logger.error("Search exception: %s", e)
        return [], "Внутренняя ошибка поиска."


def download_audio(source_url: str) -> str | None:
    try:
        file_id = uuid.uuid4().hex[:10]
        output_template = os.path.join(TEMP_DIR, f"{file_id}.%(ext)s")
        final_mp3 = os.path.join(TEMP_DIR, f"{file_id}.mp3")

        cmd = [
            "yt-dlp",
            *build_common_yt_dlp_args(),
            "-f",
            YTDLP_FORMAT or "bestaudio",
            "--extract-audio",
            "--audio-format",
            "mp3",
            "--audio-quality",
            f"{YTDLP_AUDIO_BITRATE}K",
            "--no-playlist",
            "-o",
            output_template,
            source_url,
        ]
        if FFMPEG_THREADS > 0:
            cmd.extend(["--postprocessor-args", f"ffmpeg:-threads {FFMPEG_THREADS}"])
        if YTDLP_CONCURRENT_FRAGMENTS > 1:
            cmd.extend(["--concurrent-fragments", str(YTDLP_CONCURRENT_FRAGMENTS)])
        result = run_yt_dlp(cmd, timeout=180)
        if result.returncode != 0:
            return None
        return final_mp3 if os.path.exists(final_mp3) else None
    except Exception as e:
        logger.error("Download exception: %s", e)
        return None


def download_thumbnail(cover_url: str | None) -> str | None:
    if not cover_url:
        return None
    try:
        file_id = uuid.uuid4().hex[:10]
        out_path = os.path.join(TEMP_DIR, f"{file_id}.jpg")
        req = urllib.request.Request(cover_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp, open(out_path, "wb") as f:
            f.write(resp.read())
        return out_path if os.path.exists(out_path) else None
    except Exception:
        return None


def allow_user_search(user_id: int) -> tuple[bool, int]:
    now = time.time()
    last = USER_LAST_SEARCH_TS.get(user_id, 0)
    delta = now - last
    if delta < SEARCH_COOLDOWN_SEC:
        return False, int(SEARCH_COOLDOWN_SEC - delta + 1)
    USER_LAST_SEARCH_TS[user_id] = now
    return True, 0


async def send_track_to_user(context: ContextTypes.DEFAULT_TYPE, chat_id: int, item: dict) -> None:
    status = await context.bot.send_message(chat_id=chat_id, text="⏳ Загружаю MP3...")
    track_key = make_track_key(item)

    cached = get_cached_file_id(track_key)
    if cached:
        await context.bot.send_audio(
            chat_id=chat_id,
            audio=cached,
            title=item.get("title", "audio"),
            performer=item.get("artist"),
            caption=f"🎵 {item.get('artist', 'Unknown')} - {item.get('title', 'Track')}",
        )
        await status.delete()
        return

    source_url = item["url"]
    if item.get("source") == "spotify" or "open.spotify.com/track/" in (source_url or ""):
        source_url = await asyncio.to_thread(
            resolve_spotify_to_soundcloud,
            source_url,
            item.get("title"),
            item.get("artist"),
        )
        if not source_url:
            await status.edit_text("❌ Не удалось найти MP3-источник для Spotify трека.")
            return

    audio_file = await asyncio.to_thread(download_audio, source_url)
    if not audio_file:
        await status.edit_text("❌ Ошибка при скачивании MP3.")
        return

    thumb_path = None
    if SEND_THUMBNAIL:
        thumb_path = await asyncio.to_thread(download_thumbnail, item.get("cover_url"))
    await status.edit_text("📤 Отправляю MP3...")
    try:
        with open(audio_file, "rb") as audio:
            thumb_file = open(thumb_path, "rb") if thumb_path and os.path.exists(thumb_path) else None
            try:
                sent = await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=audio,
                    title=item.get("title", "audio"),
                    performer=item.get("artist"),
                    thumbnail=thumb_file,
                    caption=f"🎵 {item.get('artist', 'Unknown')} - {item.get('title', 'Track')}",
                )
                if sent and sent.audio and sent.audio.file_id:
                    save_cached_file_id(track_key, sent.audio.file_id)
            finally:
                if thumb_file:
                    thumb_file.close()
        await status.delete()
    except Exception as e:
        logger.error("Telegram send_audio failed: %s", e)
        await status.edit_text("❌ Ошибка отправки в Telegram.")
        return
    finally:
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)
        if audio_file and os.path.exists(audio_file):
            os.remove(audio_file)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args and context.args[0].startswith("dl_"):
        dl_key = context.args[0].replace("dl_", "", 1)
        item = get_download_item(dl_key)
        if not item:
            await update.message.reply_text("❌ Ссылка устарела. Открой результаты заново.")
            return
        await send_track_to_user(context, update.effective_chat.id, item)
        return

    await update.message.reply_text(
        "🎵 Отправь название трека или ссылку SoundCloud/Spotify.\n"
        "Поиск: SoundCloud + Spotify (если настроен)\n"
        "Скачивание MP3: через SoundCloud-источник\n"
        "Команды:\n"
        "/help"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Как пользоваться:\n"
        "1) Напиши название трека (поиск в SoundCloud).\n"
        "2) Выбери кнопку скачать.\n"
        "3) Бот отправит MP3-файл.\n\n"
        "Команды:\n"
        "/start\n"
        "/help"
    )


async def handle_inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = (update.inline_query.query or "").strip()
    if not query_text:
        return

    allowed, _ = allow_user_search(update.inline_query.from_user.id)
    if not allowed:
        return

    source = "spotify" if query_text.lower().startswith("sp ") or "spotify.com/" in query_text.lower() else "soundcloud"
    clean_query = query_text[3:].strip() if query_text.lower().startswith("sp ") else query_text
    videos, error = await asyncio.to_thread(search_music, clean_query, BOT_RESULTS_LIMIT, False, True, source)
    username = context.bot.username

    results = []
    for i, video in enumerate(videos[:BOT_RESULTS_LIMIT]):
        dl_key = put_download_item(
            video["url"],
            video["title"],
            video.get("artist"),
            video.get("cover_url"),
            video.get("source", "soundcloud"),
        )
        deep_link = f"https://t.me/{username}?start=dl_{dl_key}"
        artist = video.get("artist") or "Unknown Artist"
        body = f"{artist} | {video['duration']} | {video.get('source', 'soundcloud')}"
        if error:
            body = f"{body}\n{error}"

        results.append(
            InlineQueryResultArticle(
                id=f"{i}-{dl_key}",
                title=f"{artist} - {video['title']}"[:64],
                description=body[:256],
                input_message_content=InputTextMessageContent(
                    f"🎵 {artist} - {video['title']}\nНажми кнопку ниже, чтобы получить MP3 в ЛС."
                ),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬇️ Скачать MP3", url=deep_link)]]),
                thumbnail_url=video.get("cover_url"),
            )
        )

    await update.inline_query.answer(results=results, cache_time=10, is_personal=True)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = (update.message.text or "").strip()
    allowed, wait_sec = allow_user_search(update.effective_user.id)
    if not allowed:
        await update.message.reply_text(f"⏱ Подожди {wait_sec} сек перед новым поиском.")
        return

    source = "spotify" if query.lower().startswith("sp ") or "spotify.com/" in query.lower() else "soundcloud"
    clean_query = query[3:].strip() if query.lower().startswith("sp ") else query

    search_msg = await update.message.reply_text(
        f"🔍 Ищу в *{source}*: *{clean_query}*...",
        parse_mode="Markdown",
    )
    videos, error = await asyncio.to_thread(search_music, clean_query, BOT_RESULTS_LIMIT, False, True, source)

    if not videos:
        await search_msg.edit_text(f"😔 Ошибка поиска: {error}")
        return

    text = "🎵 *Результаты:*\n"
    keyboard = []
    for i, video in enumerate(videos, 1):
        title = video["title"][:35]
        artist = (video.get("artist") or "Unknown Artist")[:25]
        text += f"{i}. {artist} - {title} ({video['duration']})\n"
        dl_key = put_download_item(
            video["url"],
            video["title"],
            video.get("artist"),
            video.get("cover_url"),
            video.get("source", "soundcloud"),
        )
        keyboard.append([InlineKeyboardButton(f"⬇️ {i}. {artist} - {title}", callback_data=f"dl_{dl_key}")])

    await search_msg.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("dl_"):
        dl_key = query.data.replace("dl_", "", 1)
        item = get_download_item(dl_key)
        if not item:
            await query.message.reply_text("❌ Ссылка устарела. Повторите поиск.")
            return
        await send_track_to_user(context, query.message.chat_id, item)


def start_http_api() -> None:
    app = Flask(__name__)
    ensure_send_loop()
    app_root = os.path.dirname(os.path.abspath(__file__))
    web_root = WEBAPP_STATIC_DIR or app_root

    @app.get("/")
    def webapp_index():
        resp = send_from_directory(web_root, "index.html")
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.get("/api/health")
    def api_health():
        return jsonify({"ok": True})

    @app.post("/api/search")
    def api_search():
        payload = request.get_json(silent=True) or {}
        query = str(payload.get("query", "")).strip()
        if not query:
            return jsonify({"ok": False, "error": "query is required", "results": []}), 400

        raw_limit = payload.get("limit")
        raw_offset = payload.get("offset")
        try:
            requested_limit = int(raw_limit) if raw_limit is not None else MAX_SEARCH_RESULTS
        except (TypeError, ValueError):
            requested_limit = MAX_SEARCH_RESULTS
        try:
            requested_offset = int(raw_offset) if raw_offset is not None else 0
        except (TypeError, ValueError):
            requested_offset = 0

        artist_mode = bool(payload.get("artistMode")) or bool(payload.get("artist"))
        include_covers = bool(payload.get("includeCovers", False))
        include_meta = bool(payload.get("includeMeta", False))
        source = str(payload.get("source") or "soundcloud").strip().lower()
        if artist_mode:
            requested_limit = max(10, min(requested_limit, 200))
        else:
            requested_limit = max(5, min(requested_limit, 60))
        videos, error = search_music(
            query,
            requested_limit,
            artist_mode,
            include_covers,
            source,
            include_meta,
            requested_offset,
        )
        return jsonify({"ok": len(videos) > 0, "error": error, "results": videos})

    @app.get("/api/artist")
    def api_artist():
        if not is_spotify_configured():
            return jsonify({"ok": False, "error": "Spotify не настроен"}), 400
        query = str(request.args.get("query") or "").strip()
        if not query:
            return jsonify({"ok": False, "error": "query is required"}), 400
        cached_payload = get_cached_artist_payload(query)
        if cached_payload is not None:
            return jsonify(cached_payload)
        if time.time() < SPOTIFY_RATE_LIMITED_UNTIL:
            return jsonify({"ok": False, "error": "spotify rate limited"}), 429
        artist = search_spotify_artist(query)
        albums_with_tracks = []
        if artist and artist.get("id"):
            albums_with_tracks = build_artist_catalog_from_artist(artist.get("id"))
        if not albums_with_tracks:
            albums_with_tracks = build_artist_catalog_from_search(query, limit=50)
        if not artist and albums_with_tracks:
            artist = {
                "id": None,
                "name": query,
                "image": None,
                "followers": None,
                "genres": [],
                "url": "",
            }
        if time.time() < SPOTIFY_RATE_LIMITED_UNTIL and (not artist or not albums_with_tracks):
            return jsonify({"ok": False, "error": "spotify rate limited"}), 429
        if not artist:
            payload = {"ok": False, "error": "artist not found"}
            set_cached_artist_payload(query, payload)
            return jsonify(payload), 404
        if not albums_with_tracks:
            payload = {"ok": False, "error": "artist catalog empty"}
            set_cached_artist_payload(query, payload)
            return jsonify(payload), 200
        payload = {"ok": True, "artist": artist, "albums": albums_with_tracks}
        set_cached_artist_payload(query, payload)
        return jsonify(payload)

    @app.get("/api/new-releases")
    def api_new_releases():
        if not (is_spotify_configured() or is_soundcloud_api_configured()):
            return jsonify({"ok": False, "error": "Нет доступных источников", "albums": [], "tracks": []}), 400

        raw_limit = request.args.get("limit")
        raw_country = request.args.get("country")
        try:
            requested_limit = int(raw_limit) if raw_limit is not None else 12
        except (TypeError, ValueError):
            requested_limit = 12
        country = str(raw_country or "US").strip()[:2]
        sc_tracks = get_soundcloud_new_releases(min(SC_NEW_RELEASES_LIMIT, requested_limit * 2))
        if sc_tracks:
            enriched = []
            for idx, item in enumerate(sc_tracks[:requested_limit]):
                meta = spotify_lookup_track(item.get("title", ""), item.get("artist"))
                if meta:
                    item.update(
                        {
                            "spotify_id": meta.get("spotify_id"),
                            "spotify_url": meta.get("spotify_url"),
                            "album_name": meta.get("album_name"),
                            "album_id": meta.get("album_id"),
                            "release_date": meta.get("release_date"),
                            "popularity": meta.get("popularity"),
                            "explicit": meta.get("explicit"),
                            "preview_url": meta.get("preview_url"),
                            "isrc": meta.get("isrc"),
                            "cover_url": meta.get("cover_url") or item.get("cover_url"),
                            "source": "soundcloud",
                        }
                    )
                enriched.append(item)
            return jsonify({"ok": True, "albums": [], "tracks": enriched})

        payload = get_spotify_new_releases(requested_limit, country)
        return jsonify({"ok": True, "albums": payload.get("albums", []), "tracks": payload.get("tracks", [])})

    @app.post("/api/download")
    def api_download():
        payload = request.get_json(silent=True) or {}
        track_url = (payload.get("url") or payload.get("trackUrl") or "").strip()
        if not track_url:
            return jsonify({"ok": False, "error": "url is required"}), 400

        chat_id_raw = payload.get("chatId") or payload.get("userId")
        try:
            chat_id = int(chat_id_raw)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "chatId/userId must be integer"}), 400

        item = {
            "url": track_url,
            "title": payload.get("title") or "audio",
            "artist": payload.get("artist") or None,
            "cover_url": payload.get("cover_url") or payload.get("cover") or None,
            "source": str(payload.get("source") or "soundcloud"),
        }
        track_key = make_track_key(item)

        try:
            cached = get_cached_file_id(track_key)
            if cached:
                fut = asyncio.run_coroutine_threadsafe(
                    SEND_BOT.send_audio(
                        chat_id=chat_id,
                        audio=cached,
                        title=item.get("title"),
                        performer=item.get("artist"),
                    ),
                    SEND_LOOP,
                )
                fut.result(timeout=60)
                return jsonify({"ok": True, "cached": True})

            resolved_url = track_url
            if item.get("source") == "spotify" or "open.spotify.com/track/" in track_url:
                resolved_url = resolve_spotify_to_soundcloud(
                    track_url,
                    item.get("title"),
                    item.get("artist"),
                )
                if not resolved_url:
                    return jsonify({"ok": False, "error": "spotify track resolve failed"}), 400

            audio_file = download_audio(resolved_url)
            if not audio_file:
                return jsonify({"ok": False, "error": "download failed"}), 500

            thumb_path = None
            if SEND_THUMBNAIL:
                thumb_path = download_thumbnail(item.get("cover_url"))
            try:
                with open(audio_file, "rb") as audio:
                    thumb_file = open(thumb_path, "rb") if thumb_path and os.path.exists(thumb_path) else None
                    try:
                        fut = asyncio.run_coroutine_threadsafe(
                            SEND_BOT.send_audio(
                                chat_id=chat_id,
                                audio=audio,
                                title=item.get("title"),
                                performer=item.get("artist"),
                                thumbnail=thumb_file,
                            ),
                            SEND_LOOP,
                        )
                        sent = fut.result(timeout=180)
                        if sent and sent.audio and sent.audio.file_id:
                            save_cached_file_id(track_key, sent.audio.file_id)
                    finally:
                        if thumb_file:
                            thumb_file.close()
            except Exception as e:
                logger.error("HTTP send_audio failed: %s", e)
                return jsonify({"ok": False, "error": "telegram send failed"}), 500
            finally:
                if thumb_path and os.path.exists(thumb_path):
                    os.remove(thumb_path)
                if audio_file and os.path.exists(audio_file):
                    os.remove(audio_file)

            return jsonify({"ok": True, "cached": False})
        except Exception as e:
            logger.error("HTTP download error: %s", e)
            return jsonify({"ok": False, "error": "internal error"}), 500

    app.run(host="0.0.0.0", port=WEBAPP_API_PORT, debug=False, use_reloader=False)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Добавьте его в .env.")

    init_db()
    global HTTP_API_THREAD
    HTTP_API_THREAD = threading.Thread(target=start_http_api, daemon=True)
    HTTP_API_THREAD.start()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(InlineQueryHandler(handle_inline_query))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    print("🤖 Бот запущен!")
    if WEBHOOK_URL:
        # WEBHOOK_URL should be like https://your.domain or https://your.domain/path
        webhook_full = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
        app.run_webhook(
            listen="0.0.0.0",
            port=WEBHOOK_PORT,
            url_path=WEBHOOK_PATH.lstrip("/"),
            webhook_url=webhook_full,
        )
    else:
        app.run_polling()


if __name__ == "__main__":
    main()



