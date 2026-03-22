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
from flask import Flask, jsonify, request, send_from_directory, redirect

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
SPOTIFY_ARTIST_ALBUM_LIMIT = int(os.getenv("SPOTIFY_ARTIST_ALBUM_LIMIT", "200"))
SPOTIFY_ARTIST_INCLUDE_GROUPS = os.getenv(
    "SPOTIFY_ARTIST_INCLUDE_GROUPS",
    "album,single,appears_on,compilation",
).strip()
SPOTIFY_ARTIST_TRACK_LIMIT = int(os.getenv("SPOTIFY_ARTIST_TRACK_LIMIT", "1000"))
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
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "").strip()
SPOTIFY_OAUTH_SCOPES = os.getenv(
    "SPOTIFY_OAUTH_SCOPES",
    "user-read-email user-read-private user-library-read",
).strip()
SPOTIFY_ARTIST_CACHE_TTL = int(os.getenv("SPOTIFY_ARTIST_CACHE_TTL", "900"))
SPOTIFY_ALBUM_TRACKS_CACHE_TTL = int(os.getenv("SPOTIFY_ALBUM_TRACKS_CACHE_TTL", "21600"))
SPOTIFY_TRACK_META_CACHE_TTL = int(os.getenv("SPOTIFY_TRACK_META_CACHE_TTL", "86400"))
SPOTIFY_ARTIST_META_LIMIT = int(os.getenv("SPOTIFY_ARTIST_META_LIMIT", "20"))
SPOTIFY_MIN_INTERVAL_MS = int(os.getenv("SPOTIFY_MIN_INTERVAL_MS", "150"))
SPOTIFY_MAX_CONCURRENCY = int(os.getenv("SPOTIFY_MAX_CONCURRENCY", "1"))
SC_CLIENT_ID = os.getenv("SC_CLIENT_ID", "").strip()
SC_API_TIMEOUT_SEC = int(os.getenv("SC_API_TIMEOUT_SEC", "12"))
SC_ARTIST_TRACK_LIMIT = int(os.getenv("SC_ARTIST_TRACK_LIMIT", "600"))
SC_ARTIST_PAGE_SIZE = int(os.getenv("SC_ARTIST_PAGE_SIZE", "200"))
SC_SET_TRACK_LIMIT = int(os.getenv("SC_SET_TRACK_LIMIT", "200"))
SC_SET_MAX_PER_PAGE = int(os.getenv("SC_SET_MAX_PER_PAGE", "4"))
SC_USER_TRACK_SOURCE = os.getenv("SC_USER_TRACK_SOURCE", "yt-dlp").strip().lower()
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
SPOTIFY_TRACK_META_CACHE: dict[str, dict] = {}
SPOTIFY_RATE_LIMITED_UNTIL = 0.0
SPOTIFY_LAST_REQUEST_AT = 0.0
SPOTIFY_LAST_ERROR_TS = 0.0
SPOTIFY_LAST_ERROR_MSG = ""
SPOTIFY_LOCK = threading.Lock()
SPOTIFY_SEM = threading.Semaphore(max(1, SPOTIFY_MAX_CONCURRENCY))
SPOTIFY_OAUTH_STATE: dict[str, dict] = {}
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
    DB_CONN.execute(
        """
        CREATE TABLE IF NOT EXISTS spotify_tokens (
            user_id TEXT PRIMARY KEY,
            access_token TEXT NOT NULL,
            refresh_token TEXT,
            expires_at INTEGER NOT NULL,
            scope TEXT,
            token_type TEXT
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
    query: str,
    limit: int,
    artist_mode: bool,
    include_covers: bool,
    source: str,
    offset: int,
    user_key: str | None = None,
) -> str:
    user_part = (user_key or "").strip()
    return (
        f"{source}|{query.lower().strip()}|{limit}|{offset}|"
        f"{int(artist_mode)}|{int(include_covers)}|{user_part}"
    )


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
    global SPOTIFY_LAST_REQUEST_AT
    global SPOTIFY_LAST_ERROR_TS
    global SPOTIFY_LAST_ERROR_MSG
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
            error_body = ""
            try:
                error_body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                error_body = ""
            error_message = ""
            if error_body:
                try:
                    parsed = json.loads(error_body)
                    if isinstance(parsed, dict):
                        err = parsed.get("error") or {}
                        if isinstance(err, dict):
                            error_message = str(err.get("message") or "").strip()
                        elif isinstance(err, str):
                            error_message = err.strip()
                        if not error_message:
                            error_message = parsed.get("message") or ""
                    elif isinstance(parsed, list):
                        error_message = " ".join([str(x) for x in parsed if x])
                except Exception:
                    error_message = error_body.strip()
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
            if error_message:
                logger.error("HTTP JSON request failed (%s): %s (%s)", url, e, error_message)
            else:
                logger.error("HTTP JSON request failed (%s): %s", url, e)
            if is_spotify:
                SPOTIFY_LAST_ERROR_TS = time.time()
                SPOTIFY_LAST_ERROR_MSG = f"HTTP {e.code}" + (f": {error_message}" if error_message else "")
            return None
        except Exception as e:
            logger.error("HTTP JSON request failed (%s): %s", url, e)
            if is_spotify:
                SPOTIFY_LAST_ERROR_TS = time.time()
                SPOTIFY_LAST_ERROR_MSG = str(e)
            return None
        finally:
            if is_spotify:
                try:
                    SPOTIFY_SEM.release()
                except Exception:
                    pass


def is_spotify_configured() -> bool:
    return bool(SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET)


def is_spotify_rate_limited() -> bool:
    return time.time() < SPOTIFY_RATE_LIMITED_UNTIL


def spotify_error_hint() -> str | None:
    if time.time() - SPOTIFY_LAST_ERROR_TS > 120:
        return None
    msg = (SPOTIFY_LAST_ERROR_MSG or "").strip()
    if not msg:
        return None
    if "401" in msg:
        return "Токен Spotify истек — подключите снова"
    if "403" in msg:
        return "Spotify запретил запрос (403). Проверьте доступы приложения."
    if "429" in msg:
        return "Spotify ограничил запросы, попробуйте позже"
    return f"Ошибка Spotify: {msg}"


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


def search_spotify(
    query: str,
    limit: int = 20,
    include_meta: bool = False,
    offset: int = 0,
    token: str | None = None,
) -> list:
    token = token or get_spotify_token()
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
        artist_ids = [a.get("id") for a in artists if a.get("id")]
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
            "artist_ids": artist_ids,
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


def make_artist_cache_key(query: str, offset: int, limit: int, source: str | None = None) -> str:
    base = normalize_artist_name(query)
    return f"{(source or '').strip()}|{base}|{max(0, int(offset))}|{max(0, int(limit))}"


def get_cached_artist_payload(query: str, offset: int = 0, limit: int = 0, source: str | None = None) -> dict | None:
    if SPOTIFY_ARTIST_CACHE_TTL <= 0:
        return None
    key = make_artist_cache_key(query, offset, limit, source)
    cached = SPOTIFY_ARTIST_CACHE.get(key)
    if not cached:
        return None
    if time.time() - cached.get("created_at", 0) > SPOTIFY_ARTIST_CACHE_TTL:
        SPOTIFY_ARTIST_CACHE.pop(key, None)
        return None
    payload = cached.get("payload")
    if not payload or not payload.get("ok"):
        return None
    return payload


def set_cached_artist_payload(
    query: str, payload: dict, offset: int = 0, limit: int = 0, source: str | None = None
) -> None:
    if SPOTIFY_ARTIST_CACHE_TTL <= 0:
        return
    if not payload or not payload.get("ok"):
        return
    key = make_artist_cache_key(query, offset, limit, source)
    SPOTIFY_ARTIST_CACHE[key] = {"payload": payload, "created_at": time.time()}


def _track_meta_cache_key(title: str, artist: str | None) -> str:
    title_key = (title or "").strip().lower()
    artist_key = normalize_artist_name(artist or "")
    return f"{artist_key}|{title_key}"


def get_cached_spotify_track_meta(title: str, artist: str | None) -> dict | None:
    if SPOTIFY_TRACK_META_CACHE_TTL <= 0:
        return None
    key = _track_meta_cache_key(title, artist)
    cached = SPOTIFY_TRACK_META_CACHE.get(key)
    if not cached:
        return None
    if time.time() - cached.get("created_at", 0) > SPOTIFY_TRACK_META_CACHE_TTL:
        SPOTIFY_TRACK_META_CACHE.pop(key, None)
        return None
    return cached.get("meta")


def set_cached_spotify_track_meta(title: str, artist: str | None, meta: dict) -> None:
    if SPOTIFY_TRACK_META_CACHE_TTL <= 0:
        return
    if not meta:
        return
    key = _track_meta_cache_key(title, artist)
    SPOTIFY_TRACK_META_CACHE[key] = {"meta": meta, "created_at": time.time()}


def search_spotify_artist_candidates(query: str, limit: int = 5, token: str | None = None) -> list:
    token = token or get_spotify_token()
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


def search_spotify_artist(query: str, token: str | None = None) -> dict | None:
    if not query:
        return None
    candidates = search_spotify_artist_candidates(query, limit=8, token=token)
    if not candidates:
        alt = query.replace("ASAP", "A$AP").replace("A$AP", "A$AP").strip()
        if alt and alt.lower() != query.lower():
            candidates = search_spotify_artist_candidates(alt, limit=8, token=token)
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


def get_spotify_artist_tracks_by_search(
    artist_name: str,
    artist_id: str | None = None,
    limit: int | None = None,
    token: str | None = None,
) -> list:
    if not artist_name:
        return []
    max_limit = max(20, min(limit or SPOTIFY_ARTIST_TRACK_LIMIT, 1000))
    results = []
    offset = 0
    query = f'artist:"{artist_name}"'
    while offset < max_limit:
        page_size = min(50, max_limit - offset)
        batch_raw = search_spotify(query, limit=page_size, include_meta=True, offset=offset, token=token)
        if not batch_raw:
            break
        batch = batch_raw
        if artist_id:
            batch = [t for t in batch_raw if artist_id in (t.get("artist_ids") or [])]
        results.extend(batch)
        if len(batch_raw) < page_size:
            break
        offset += page_size
    return results


def group_tracks_by_album(tracks: list) -> list:
    albums_map: dict[str, dict] = {}
    for track in tracks:
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
    return albums


def get_spotify_artist_albums(
    artist_id: str,
    limit: int = 20,
    include_groups: str | None = None,
    token: str | None = None,
) -> list:
    token = token or get_spotify_token()
    if not token or not artist_id:
        return []
    groups = include_groups or SPOTIFY_ARTIST_INCLUDE_GROUPS or "album,single,appears_on,compilation"
    safe_limit = max(1, min(limit, 200))
    offset = 0
    per_page = 50
    albums = []
    seen = set()
    while offset < safe_limit:
        page_limit = min(per_page, safe_limit - offset)
        url = (
            f"https://api.spotify.com/v1/artists/{artist_id}/albums"
            f"?include_groups={urllib.parse.quote(groups)}&limit={page_limit}&offset={offset}"
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
        if not items:
            break
        for it in items:
            album_id = it.get("id")
            if album_id and album_id in seen:
                continue
            if album_id:
                seen.add(album_id)
            images = it.get("images") or []
            albums.append(
                {
                    "id": album_id,
                    "name": it.get("name"),
                    "release_date": it.get("release_date"),
                    "total_tracks": it.get("total_tracks"),
                    "cover_url": images[0].get("url") if images else None,
                    "url": ((it.get("external_urls") or {}).get("spotify") or ""),
                }
            )
        if len(items) < page_limit:
            break
        offset += page_limit
    return albums


def get_spotify_album_tracks(
    album_id: str,
    artist_id: str | None = None,
    token: str | None = None,
) -> list:
    token = token or get_spotify_token()
    if not token or not album_id:
        return []
    if SPOTIFY_ALBUM_TRACKS_CACHE_TTL > 0:
        cached = SPOTIFY_ALBUM_TRACKS_CACHE.get(album_id)
        if cached and time.time() - cached.get("created_at", 0) <= SPOTIFY_ALBUM_TRACKS_CACHE_TTL:
            tracks = cached.get("tracks") or []
            if artist_id:
                filtered = []
                for t in tracks:
                    ids = t.get("artist_ids") or []
                    if artist_id in ids:
                        filtered.append(t)
                return filtered
            return tracks
    tracks = []
    offset = 0
    per_page = 50
    while True:
        url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit={per_page}&offset={offset}"
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
        if not items:
            break
        for it in items:
            artists = it.get("artists") or []
            artist_ids = [a.get("id") for a in artists if a.get("id")]
            if artist_id and artist_id not in artist_ids:
                continue
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
                    "artist_ids": artist_ids,
                    "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
                    "url": url,
                    "source": "spotify",
                }
            )
        if len(items) < per_page:
            break
        offset += per_page
    if SPOTIFY_ALBUM_TRACKS_CACHE_TTL > 0 and tracks:
        SPOTIFY_ALBUM_TRACKS_CACHE[album_id] = {"tracks": tracks, "created_at": time.time()}
    if artist_id:
        return [t for t in tracks if artist_id in (t.get("artist_ids") or [])]
    return tracks


def build_artist_catalog_from_artist(
    artist_id: str, artist_name: str | None = None, token: str | None = None
) -> list:
    if not artist_id:
        return []
    tracks = get_spotify_artist_tracks_by_search(
        artist_name or "", artist_id, SPOTIFY_ARTIST_TRACK_LIMIT, token=token
    )
    albums_from_tracks = group_tracks_by_album(tracks)
    if not SPOTIFY_ARTIST_EXPAND_ALBUMS:
        return albums_from_tracks
    albums = get_spotify_artist_albums(artist_id, limit=SPOTIFY_ARTIST_ALBUM_LIMIT, token=token)
    if not albums:
        return albums_from_tracks
    fallback_map = {}
    for album in albums_from_tracks:
        key = album.get("id") or album.get("name") or "singles"
        fallback_map[key] = album
    out = []
    for album in albums:
        album_id = album.get("id")
        tracks = get_spotify_album_tracks(album_id, artist_id=artist_id, token=token) if album_id else []
        if not tracks:
            fb = None
            if album_id and album_id in fallback_map:
                fb = fallback_map.get(album_id)
            elif album.get("name") and album.get("name") in fallback_map:
                fb = fallback_map.get(album.get("name"))
            if fb and fb.get("tracks"):
                tracks = fb.get("tracks") or []
        if not tracks:
            continue
        out.append(
            {
                "id": album_id,
                "name": album.get("name") or "Альбом",
                "release_date": album.get("release_date"),
                "cover_url": album.get("cover_url"),
                "tracks": [{k: v for k, v in t.items() if k != "artist_ids"} for t in tracks],
            }
        )
    return out or albums_from_tracks


def build_artist_catalog_from_search(query: str, limit: int = 50, token: str | None = None) -> list:
    if not query:
        return []
    max_limit = max(20, min(limit, SPOTIFY_ARTIST_TRACK_LIMIT))
    results = []
    offset = 0
    while offset < max_limit:
        batch = search_spotify(
            query,
            limit=min(50, max_limit - offset),
            include_meta=True,
            offset=offset,
            token=token,
        )
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
            full_tracks = get_spotify_album_tracks(album_id, token=token)
            if full_tracks:
                album["tracks"] = [
                    {k: v for k, v in t.items() if k != "artist_ids"} for t in full_tracks
                ]
    return albums


def spotify_lookup_track(title: str, artist: str | None = None) -> dict | None:
    token = get_spotify_token()
    if not token:
        return None
    if is_spotify_rate_limited():
        return None

    query = f"{artist or ''} {title or ''}".strip()
    if not query:
        return None

    cached = get_cached_spotify_track_meta(title, artist)
    if cached:
        return cached

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
        timeout=10,
        max_retries=0,
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
    meta = {
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
    set_cached_spotify_track_meta(title, artist, meta)
    return meta


def build_spotify_auth_url(state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": SPOTIFY_CLIENT_ID,
        "redirect_uri": SPOTIFY_REDIRECT_URI,
        "scope": SPOTIFY_OAUTH_SCOPES,
        "state": state,
        "show_dialog": "true",
    }
    return "https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(params)


def spotify_exchange_code(code: str) -> dict | None:
    if not code or not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET or not SPOTIFY_REDIRECT_URI:
        return None
    auth_raw = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode("utf-8")
    auth_b64 = base64.b64encode(auth_raw).decode("ascii")
    data = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": SPOTIFY_REDIRECT_URI,
        }
    ).encode("utf-8")
    payload = http_json_request(
        "https://accounts.spotify.com/api/token",
        method="POST",
        headers={
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=data,
        timeout=15,
        max_retries=0,
    )
    return payload


def spotify_refresh_token(refresh_token: str) -> dict | None:
    if not refresh_token or not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return None
    auth_raw = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode("utf-8")
    auth_b64 = base64.b64encode(auth_raw).decode("ascii")
    data = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
    ).encode("utf-8")
    payload = http_json_request(
        "https://accounts.spotify.com/api/token",
        method="POST",
        headers={
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=data,
        timeout=15,
        max_retries=0,
    )
    if payload and "refresh_token" not in payload:
        payload["refresh_token"] = refresh_token
    return payload


def save_spotify_user_token(user_id: str, payload: dict) -> None:
    if not DB_CONN or not user_id or not payload:
        return
    access_token = payload.get("access_token") or ""
    refresh_token = payload.get("refresh_token") or ""
    expires_in = int(payload.get("expires_in") or 3600)
    expires_at = int(time.time()) + max(60, expires_in)
    scope = payload.get("scope") or ""
    token_type = payload.get("token_type") or "Bearer"
    if not access_token:
        return
    DB_CONN.execute(
        """
        INSERT INTO spotify_tokens (user_id, access_token, refresh_token, expires_at, scope, token_type)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          access_token=excluded.access_token,
          refresh_token=excluded.refresh_token,
          expires_at=excluded.expires_at,
          scope=excluded.scope,
          token_type=excluded.token_type
        """,
        (str(user_id), access_token, refresh_token, expires_at, scope, token_type),
    )
    DB_CONN.commit()


def get_spotify_user_token(user_id: str) -> str | None:
    if not DB_CONN or not user_id:
        return None
    cur = DB_CONN.cursor()
    cur.execute(
        "SELECT access_token, refresh_token, expires_at FROM spotify_tokens WHERE user_id = ?",
        (str(user_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    access_token, refresh_token, expires_at = row
    now = int(time.time())
    if access_token and int(expires_at or 0) - 60 > now:
        return access_token
    if refresh_token:
        refreshed = spotify_refresh_token(refresh_token)
        if refreshed and refreshed.get("access_token"):
            save_spotify_user_token(user_id, refreshed)
            return refreshed.get("access_token")
    return None


def spotify_user_request(user_id: str, url: str) -> dict | None:
    token = get_spotify_user_token(user_id)
    if not token:
        return None
    return http_json_request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        timeout=15,
        max_retries=0,
    )


def get_spotify_request_token(user_id: str | None = None) -> str | None:
    if user_id:
        token = get_spotify_user_token(user_id)
        if token:
            return token
    return get_spotify_token()


def build_spotify_artist_catalog(query: str, token: str | None = None) -> tuple[dict | None, list]:
    artist = search_spotify_artist(query, token=token)
    if artist:
        albums = build_artist_catalog_from_artist(artist.get("id"), artist.get("name"), token=token)
        return artist, albums
    albums = build_artist_catalog_from_search(query, limit=SPOTIFY_ARTIST_TRACK_LIMIT, token=token)
    if albums:
        fallback_artist = {
            "id": None,
            "name": query,
            "image": None,
            "followers": None,
            "genres": [],
            "url": "",
        }
        return fallback_artist, albums
    return None, []


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
                cover = data.get("thumbnail") or data.get("artwork_url")
                if not cover:
                    thumbs = data.get("thumbnails") or []
                    if thumbs and isinstance(thumbs, list):
                        cover = thumbs[-1].get("url")
            album_title = data.get("album") or data.get("album_name")
            if isinstance(album_title, dict):
                album_title = album_title.get("name") or album_title.get("title")
            url = data.get("webpage_url") or data.get("original_url") or data.get("url")
            artist = data.get("uploader") or data.get("channel") or data.get("creator") or data.get("artist")
            results.append(
                {
                    "id": data.get("id"),
                    "title": data.get("title", "Без названия"),
                    "artist": artist,
                    "duration": data.get("duration_string", "?:??"),
                    "duration_sec": duration_sec,
                    "url": url,
                    "cover_url": cover,
                    "album_name": album_title,
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


def search_soundcloud_user_yt(query: str) -> dict | None:
    if not query:
        return None
    if "soundcloud.com/" in query:
        username = query.rstrip("/").split("/")[-1]
        return {"username": username, "permalink": username, "permalink_url": query}
    return {"username": query.strip(), "permalink": query.strip(), "permalink_url": f"https://soundcloud.com/{slugify_sc_user(query)}"}


def is_soundcloud_set_url(url: str | None) -> bool:
    if not url:
        return False
    return "/sets/" in url


def normalize_soundcloud_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"https://soundcloud.com{url}"
    return f"https://soundcloud.com/{url}"


def extract_soundcloud_entries_from_flat(stdout: str) -> tuple[list[str], list[dict]]:
    track_urls: list[str] = []
    set_items: list[dict] = []
    for line in stdout.strip().split("\n"):
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        url = (
            data.get("webpage_url")
            or data.get("original_url")
            or data.get("url")
            or data.get("webpage_url_basename")
        )
        norm = normalize_soundcloud_url(url)
        if not norm:
            continue
        if is_soundcloud_set_url(norm):
            set_items.append(
                {
                    "url": norm,
                    "title": data.get("title"),
                    "cover_url": data.get("thumbnail") or data.get("artwork_url"),
                }
            )
        else:
            track_urls.append(norm)

    def dedupe_urls(urls: list[str]) -> list[str]:
        seen = set()
        out = []
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
        return out

    track_urls = dedupe_urls(track_urls)
    # Dedupe sets by URL
    set_map: dict[str, dict] = {}
    for item in set_items:
        if not item.get("url"):
            continue
        if item["url"] not in set_map:
            set_map[item["url"]] = item
    return track_urls, list(set_map.values())


def extract_soundcloud_track_urls(stdout: str) -> list[str]:
    track_urls, _ = extract_soundcloud_entries_from_flat(stdout)
    return track_urls


def expand_soundcloud_sets(set_items: list[dict]) -> tuple[list[str], dict]:
    if not set_items:
        return [], {}
    urls: list[str] = []
    meta_by_url: dict[str, dict] = {}
    for item in set_items[:SC_SET_MAX_PER_PAGE]:
        set_url = item.get("url")
        if not set_url:
            continue
        set_cover = normalize_sc_cover(item.get("cover_url"))
        set_title = item.get("title") or "Без альбома"
        cmd = [
            "yt-dlp",
            *build_common_yt_dlp_args(),
            "--flat-playlist",
            "--dump-json",
            "--playlist-end",
            str(max(1, SC_SET_TRACK_LIMIT)),
            "--skip-download",
            set_url,
        ]
        result = run_yt_dlp(cmd, timeout=max(SC_SEARCH_TIMEOUT_SEC, 10 + SC_SET_TRACK_LIMIT // 3))
        if result.returncode != 0:
            continue
        set_urls = extract_soundcloud_track_urls(result.stdout)
        for u in set_urls:
            if u not in meta_by_url:
                meta_by_url[u] = {"album_name": set_title, "cover_url": set_cover}
            urls.append(u)

    # Dedupe preserving order
    seen = set()
    out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out, meta_by_url


def dedupe_soundcloud_tracks(tracks: list[dict]) -> list[dict]:
    def score(item: dict) -> int:
        s = 0
        if item.get("album_name"):
            s += 2
        if item.get("cover_url"):
            s += 1
        dur = item.get("duration")
        if dur and dur != "?:??":
            s += 1
        if item.get("artist"):
            s += 1
        return s

    ordered_keys: list[str] = []
    best: dict[str, dict] = {}
    for t in tracks:
        key = str(t.get("id") or t.get("url") or f"{t.get('title')}|{t.get('artist')}")
        if key not in best:
            best[key] = t
            ordered_keys.append(key)
            continue
        if score(t) > score(best[key]):
            best[key] = t
    return [best[k] for k in ordered_keys]


def get_soundcloud_user_tracks_yt(user_url: str, limit: int, offset: int = 0) -> tuple[list, int]:
    safe_limit = max(1, min(limit, SC_ARTIST_TRACK_LIMIT))
    safe_offset = max(0, int(offset or 0))
    start = safe_offset + 1
    end = safe_offset + safe_limit
    timeout_sec = max(SC_SEARCH_TIMEOUT_SEC, 20 + safe_limit // 3)
    flat_cmd = [
        "yt-dlp",
        *build_common_yt_dlp_args(),
        "--flat-playlist",
        "--dump-json",
        "--playlist-start",
        str(start),
        "--playlist-end",
        str(end),
        "--skip-download",
        user_url,
    ]
    flat_result = run_yt_dlp(flat_cmd, timeout=timeout_sec)
    if flat_result.returncode != 0:
        return [], 0
    track_urls, set_items = extract_soundcloud_entries_from_flat(flat_result.stdout)
    set_urls, set_meta_by_url = expand_soundcloud_sets(set_items)
    urls = track_urls + set_urls
    if not urls:
        return [], 0

    info_timeout = max(SC_SEARCH_TIMEOUT_SEC, 12 + len(urls) // 2)
    info_cmd = [
        "yt-dlp",
        *build_common_yt_dlp_args(),
        "--dump-json",
        "--no-playlist",
        "--skip-download",
        *urls,
    ]
    info_result = run_yt_dlp(info_cmd, timeout=info_timeout)
    if info_result.returncode != 0:
        return [], len(track_urls) + len(set_items)
    tracks = parse_search_results(info_result.stdout, include_covers=True)
    tracks = [t for t in tracks if not is_soundcloud_set_url(t.get("url"))]
    for t in tracks:
        meta = set_meta_by_url.get(t.get("url") or "")
        if not meta:
            continue
        if not t.get("album_name"):
            t["album_name"] = meta.get("album_name")
        if not t.get("cover_url"):
            t["cover_url"] = meta.get("cover_url")
    return tracks, len(track_urls) + len(set_items)


def is_soundcloud_api_configured() -> bool:
    return bool(SC_CLIENT_ID)


def normalize_sc_cover(url: str | None) -> str | None:
    if not url:
        return None
    # Upgrade artwork size when possible.
    return url.replace("-large", "-t500x500")


def slugify_sc_user(name: str) -> str:
    cleaned = []
    prev_dash = False
    for ch in (name or "").lower().strip():
        if ch.isalnum():
            cleaned.append(ch)
            prev_dash = False
        elif ch in (" ", "-", "_"):
            if not prev_dash:
                cleaned.append("-")
                prev_dash = True
    slug = "".join(cleaned).strip("-")
    return slug


def resolve_soundcloud_url(url: str) -> dict | None:
    if not is_soundcloud_api_configured() or not url:
        return None
    encoded = urllib.parse.quote(url, safe="")
    api = f"https://api-v2.soundcloud.com/resolve?url={encoded}&client_id={SC_CLIENT_ID}"
    return http_json_request(api, headers={"User-Agent": "Mozilla/5.0"}, timeout=SC_API_TIMEOUT_SEC)


def search_soundcloud_user(query: str) -> dict | None:
    if not is_soundcloud_api_configured() or not query:
        return None
    if "soundcloud.com/" in query:
        resolved = resolve_soundcloud_url(query)
        if resolved:
            if resolved.get("kind") == "user":
                return resolved
            if resolved.get("kind") == "track":
                return resolved.get("user") or None
    encoded_q = urllib.parse.quote(query)
    url = (
        "https://api-v2.soundcloud.com/search/users"
        f"?q={encoded_q}&limit=10&client_id={SC_CLIENT_ID}"
    )
    payload = http_json_request(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=SC_API_TIMEOUT_SEC)
    users = (payload or {}).get("collection") or []
    qn = normalize_artist_name(query)
    best = None
    best_followers = -1
    for user in users:
        name = user.get("username") or user.get("permalink") or ""
        nn = normalize_artist_name(name)
        if qn and nn == qn:
            return user
        if qn and qn in nn:
            return user
        followers = int(user.get("followers_count") or 0)
        if followers > best_followers:
            best_followers = followers
            best = user
    if best:
        return best

    slug = slugify_sc_user(query)
    if slug:
        resolved = resolve_soundcloud_url(f"https://soundcloud.com/{slug}")
        if resolved:
            if resolved.get("kind") == "user":
                return resolved
            if resolved.get("kind") == "track":
                return resolved.get("user") or None

    # Fallback: use track search to infer the user.
    track_payload = http_json_request(
        f"https://api-v2.soundcloud.com/search/tracks?q={encoded_q}&limit=20&client_id={SC_CLIENT_ID}",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=SC_API_TIMEOUT_SEC,
    )
    if track_payload:
        items = track_payload.get("collection") or []
        candidate = None
        candidate_followers = -1
        for it in items:
            user = (it.get("user") or {})
            name = user.get("username") or user.get("permalink") or ""
            if not name:
                continue
            nn = normalize_artist_name(name)
            if qn and nn == qn:
                return user
            if qn and qn in nn:
                return user
            followers = int(user.get("followers_count") or 0)
            if followers > candidate_followers:
                candidate_followers = followers
                candidate = user
        if candidate:
            return candidate
    return None


def get_soundcloud_user_tracks(user_id: int, limit: int, offset: int = 0) -> list:
    if not is_soundcloud_api_configured() or not user_id:
        return []
    safe_limit = max(1, min(limit, SC_ARTIST_TRACK_LIMIT))
    page_size = max(50, min(SC_ARTIST_PAGE_SIZE, 200))
    offset = max(0, int(offset or 0))
    tracks = []
    next_url = (
        f"https://api-v2.soundcloud.com/users/{user_id}/tracks"
        f"?limit={page_size}&offset={offset}&linked_partitioning=1&client_id={SC_CLIENT_ID}"
    )
    while next_url and len(tracks) < safe_limit:
        payload = http_json_request(next_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=SC_API_TIMEOUT_SEC)
        if not payload:
            break
        collection = payload.get("collection") or []
        if not collection:
            break
        for it in collection:
            tracks.append(it)
            if len(tracks) >= safe_limit:
                break
        next_url = payload.get("next_href")
        if next_url and "client_id=" not in next_url:
            next_url = f"{next_url}&client_id={SC_CLIENT_ID}"
    return tracks


def map_soundcloud_track(track: dict, default_artist: str | None = None, default_cover: str | None = None) -> dict:
    artist = (track.get("user") or {}).get("username") or default_artist
    duration_ms = int(track.get("duration") or 0)
    duration_sec = duration_ms / 1000 if duration_ms else 0
    if MIN_TRACK_DURATION_SEC and duration_sec and duration_sec < MIN_TRACK_DURATION_SEC:
        return {}
    if MAX_TRACK_DURATION_SEC and duration_sec and duration_sec > MAX_TRACK_DURATION_SEC:
        return {}
    mins = duration_ms // 60000
    secs = (duration_ms % 60000) // 1000
    cover = normalize_sc_cover(track.get("artwork_url")) or normalize_sc_cover(default_cover)
    album_title = None
    publisher = track.get("publisher_metadata") or {}
    if publisher:
        album_title = publisher.get("album_title")
    album_title = album_title or track.get("album_title")
    return {
        "id": track.get("id"),
        "title": track.get("title", "Без названия"),
        "artist": artist,
        "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
        "duration_sec": duration_sec,
        "url": track.get("permalink_url") or track.get("uri"),
        "cover_url": cover,
        "album_name": album_title,
        "source": "soundcloud",
    }


def group_soundcloud_tracks_by_album(tracks: list) -> list:
    albums_map: dict[str, dict] = {}
    for track in tracks:
        album_name = track.get("album_name") or "Без альбома"
        album_id = album_name
        album = albums_map.get(album_id)
        if not album:
            album = {
                "id": None,
                "name": album_name,
                "release_date": None,
                "cover_url": track.get("cover_url"),
                "tracks": [],
            }
            albums_map[album_id] = album
        album["tracks"].append(
            {
                "id": track.get("id"),
                "title": track.get("title"),
                "artist": track.get("artist"),
                "duration": track.get("duration"),
                "url": track.get("url"),
                "source": "soundcloud",
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
    return albums


def build_soundcloud_artist_catalog(query: str, offset: int = 0, limit: int | None = None) -> tuple[dict | None, list, dict]:
    user = None
    tracks = []
    avatar = None
    user_url = None
    safe_offset = max(0, int(offset or 0))
    safe_limit = max(1, min(int(limit or SC_ARTIST_TRACK_LIMIT), SC_ARTIST_TRACK_LIMIT))

    page_count = 0
    if SC_USER_TRACK_SOURCE == "yt-dlp":
        user = search_soundcloud_user_yt(query)
        if user:
            user_url = user.get("permalink_url")
            raw_tracks, page_count = get_soundcloud_user_tracks_yt(user_url, safe_limit, safe_offset)
            for item in raw_tracks:
                if item:
                    tracks.append(item)
    else:
        user = search_soundcloud_user(query)
        if not user:
            return None, [], {"offset": safe_offset, "limit": safe_limit, "next_offset": None, "has_more": False}
        user_id = user.get("id")
        avatar = normalize_sc_cover(user.get("avatar_url"))
        raw_tracks = get_soundcloud_user_tracks(user_id, safe_limit, safe_offset)
        for item in raw_tracks:
            mapped = map_soundcloud_track(item, default_artist=user.get("username"), default_cover=avatar)
            if mapped:
                tracks.append(mapped)
        page_count = len(tracks)

    if tracks:
        fallback_artist = (user.get("username") if user else None) or query
        for item in tracks:
            if not item.get("artist"):
                item["artist"] = fallback_artist
    tracks = dedupe_soundcloud_tracks(tracks)

    if tracks and is_spotify_configured() and not is_spotify_rate_limited():
        limit = max(0, min(SPOTIFY_ARTIST_META_LIMIT, len(tracks)))
        for idx, item in enumerate(tracks):
            if limit and idx >= limit:
                continue
            if is_spotify_rate_limited():
                break
            meta = spotify_lookup_track(item.get("title", ""), item.get("artist"))
            if meta:
                item.update(
                    {
                        "spotify_id": meta.get("spotify_id"),
                        "spotify_url": meta.get("spotify_url"),
                        "album_name": meta.get("album_name") or item.get("album_name"),
                        "album_id": meta.get("album_id"),
                        "release_date": meta.get("release_date"),
                        "popularity": meta.get("popularity"),
                        "explicit": meta.get("explicit"),
                        "preview_url": meta.get("preview_url"),
                        "isrc": meta.get("isrc"),
                        "cover_url": meta.get("cover_url") or item.get("cover_url"),
                    }
                )
    albums = group_soundcloud_tracks_by_album(tracks)
    artist = {
        "id": user.get("id") if user else None,
        "name": (user.get("username") or user.get("permalink")) if user else query,
        "image": avatar,
        "followers": user.get("followers_count") if user else None,
        "genres": [],
        "url": (user.get("permalink_url") or user_url or "") if user else (user_url or ""),
    }
    next_offset = safe_offset + page_count if page_count >= safe_limit else None
    page_info = {"offset": safe_offset, "limit": safe_limit, "next_offset": next_offset, "has_more": bool(next_offset)}
    return artist, albums, page_info


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
    user_id: str | None = None,
) -> tuple[list, str | None]:
    try:
        target_limit = max(1, min(limit or MAX_SEARCH_RESULTS, 200))
        if artist_mode:
            target_limit = max(target_limit, ARTIST_SEARCH_RESULTS)
        source = (source or "spotify").strip().lower()

        if "soundcloud.com/" in query and source != "spotify":
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

        cache_key = make_search_cache_key(
            query, target_limit, artist_mode, include_covers, source, offset, user_id
        )
        cached = get_search_cache(cache_key)
        if cached is not None:
            return cached, None

        if source == "spotify":
            token = get_spotify_user_token(user_id) if user_id else None
            if user_id and not token:
                return [], "Подключите Spotify в приложении"
            token = token or get_spotify_token()
            if not token:
                return [], "Spotify не настроен"
            spotify_query = f'artist:"{query}"' if artist_mode else query
            videos = search_spotify(
                spotify_query,
                target_limit,
                include_meta=True,
                offset=offset,
                token=token,
            )
            if videos:
                set_search_cache(cache_key, videos)
                return videos, None
            hint = spotify_error_hint()
            return [], hint or "Spotify не вернул результаты. Попробуйте другой запрос."

        if artist_mode and is_soundcloud_api_configured():
            artist = search_soundcloud_user(query)
            if artist:
                needed = min(SC_ARTIST_TRACK_LIMIT, target_limit + max(0, offset))
                raw_tracks = get_soundcloud_user_tracks(artist.get("id"), needed)
                mapped = []
                avatar = normalize_sc_cover(artist.get("avatar_url"))
                for item in raw_tracks:
                    track = map_soundcloud_track(item, default_artist=artist.get("username"), default_cover=avatar)
                    if track:
                        mapped.append(track)
                sliced = mapped[offset : offset + target_limit]
                if include_meta and is_spotify_configured():
                    limit = max(0, min(SPOTIFY_META_LIMIT, len(sliced)))
                    for idx, item in enumerate(sliced):
                        if limit and idx >= limit:
                            continue
                        meta = spotify_lookup_track(item.get("title", ""), item.get("artist"))
                        if meta:
                            item.update(
                                {
                                    "spotify_id": meta.get("spotify_id"),
                                    "spotify_url": meta.get("spotify_url"),
                                    "album_name": meta.get("album_name") or item.get("album_name"),
                                    "album_id": meta.get("album_id"),
                                    "release_date": meta.get("release_date"),
                                    "popularity": meta.get("popularity"),
                                    "explicit": meta.get("explicit"),
                                    "preview_url": meta.get("preview_url"),
                                    "isrc": meta.get("isrc"),
                                    "cover_url": meta.get("cover_url") or item.get("cover_url"),
                                }
                            )
                set_search_cache(cache_key, sliced)
                return sliced, None

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
        "🎵 Отправь название трека или ссылку SoundCloud.\n"
        "Поиск: SoundCloud (обложки из Spotify при наличии)\n"
        "Скачивание MP3: через SoundCloud\n"
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

    source = "soundcloud"
    clean_query = query_text
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

    source = "soundcloud"
    clean_query = query

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
        source = str(payload.get("source") or "spotify").strip().lower()
        user_id = str(payload.get("userId") or payload.get("user_id") or "").strip() or None
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
            user_id,
        )
        return jsonify({"ok": len(videos) > 0, "error": error, "results": videos})

    @app.get("/api/artist")
    def api_artist():
        query = str(request.args.get("query") or "").strip()
        if not query:
            return jsonify({"ok": False, "error": "query is required"}), 400
        source = str(request.args.get("source") or "spotify").strip().lower()
        user_id = str(request.args.get("userId") or request.args.get("user_id") or "").strip() or None
        raw_limit = request.args.get("limit")
        raw_offset = request.args.get("offset")
        try:
            requested_limit = int(raw_limit) if raw_limit is not None else 50
        except (TypeError, ValueError):
            requested_limit = 50
        try:
            requested_offset = int(raw_offset) if raw_offset is not None else 0
        except (TypeError, ValueError):
            requested_offset = 0
        requested_limit = max(1, min(requested_limit, SC_ARTIST_TRACK_LIMIT))
        requested_offset = max(0, requested_offset)

        cached_payload = get_cached_artist_payload(query, requested_offset, requested_limit, source)
        if cached_payload is not None:
            return jsonify(cached_payload)
        if source == "spotify":
            token = get_spotify_user_token(user_id) if user_id else None
            if user_id and not token:
                return jsonify({"ok": False, "error": "Подключите Spotify"}), 401
            token = token or get_spotify_token()
            if not token:
                return jsonify({"ok": False, "error": "Spotify не настроен"}), 400
            artist, albums_with_tracks = build_spotify_artist_catalog(query, token=token)
            if not artist:
                return jsonify({"ok": False, "error": "artist not found"}), 404
            if not albums_with_tracks:
                return jsonify({"ok": False, "error": "artist catalog empty"}), 200
            payload = {
                "ok": True,
                "artist": artist,
                "albums": albums_with_tracks,
                "offset": 0,
                "limit": len(albums_with_tracks),
                "next_offset": None,
                "has_more": False,
            }
            set_cached_artist_payload(query, payload, requested_offset, requested_limit, source)
            return jsonify(payload)

        if SC_USER_TRACK_SOURCE != "yt-dlp" and not is_soundcloud_api_configured():
            return jsonify({"ok": False, "error": "SoundCloud не настроен"}), 400
        artist, albums_with_tracks, page_info = build_soundcloud_artist_catalog(
            query, offset=requested_offset, limit=requested_limit
        )
        if not artist:
            return jsonify({"ok": False, "error": "artist not found"}), 404
        if not albums_with_tracks:
            return jsonify({"ok": False, "error": "artist catalog empty"}), 200
        payload = {
            "ok": True,
            "artist": artist,
            "albums": albums_with_tracks,
            "offset": page_info.get("offset"),
            "limit": page_info.get("limit"),
            "next_offset": page_info.get("next_offset"),
            "has_more": page_info.get("has_more"),
        }
        set_cached_artist_payload(query, payload, requested_offset, requested_limit, source)
        return jsonify(payload)

    @app.get("/spotify/login")
    def spotify_login():
        if not (SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET and SPOTIFY_REDIRECT_URI):
            return "Spotify OAuth not configured", 400
        user_id = str(request.args.get("userId") or request.args.get("user_id") or "").strip()
        if not user_id:
            return "userId is required", 400
        return_url = str(request.args.get("return") or "").strip()
        state = uuid.uuid4().hex
        SPOTIFY_OAUTH_STATE[state] = {
            "user_id": user_id,
            "return_url": return_url,
            "expires_at": time.time() + 600,
        }
        return redirect(build_spotify_auth_url(state))

    @app.get("/spotify/callback")
    def spotify_callback():
        code = str(request.args.get("code") or "").strip()
        state = str(request.args.get("state") or "").strip()
        if not code or not state:
            return "Missing code/state", 400
        state_data = SPOTIFY_OAUTH_STATE.pop(state, None)
        if not state_data or state_data.get("expires_at", 0) < time.time():
            return "State expired", 400
        token_payload = spotify_exchange_code(code)
        if not token_payload or not token_payload.get("access_token"):
            return "Failed to authorize", 400
        user_id = state_data.get("user_id") or ""
        save_spotify_user_token(user_id, token_payload)
        return_url = state_data.get("return_url") or "/"
        if return_url.startswith("http://") or return_url.startswith("https://") or return_url.startswith("/"):
            return redirect(return_url)
        return redirect("/")

    @app.get("/api/spotify/me")
    def api_spotify_me():
        user_id = str(request.args.get("userId") or request.args.get("user_id") or "").strip()
        if not user_id:
            return jsonify({"ok": False, "error": "userId is required"}), 400
        me = spotify_user_request(user_id, "https://api.spotify.com/v1/me")
        if not me:
            return jsonify({"ok": False, "error": "not authorized"}), 401
        return jsonify({"ok": True, "me": me})

    @app.get("/api/spotify/library")
    def api_spotify_library():
        user_id = str(request.args.get("userId") or request.args.get("user_id") or "").strip()
        if not user_id:
            return jsonify({"ok": False, "error": "userId is required"}), 400
        raw_limit = request.args.get("limit")
        raw_offset = request.args.get("offset")
        try:
            requested_limit = int(raw_limit) if raw_limit is not None else 20
        except (TypeError, ValueError):
            requested_limit = 20
        try:
            requested_offset = int(raw_offset) if raw_offset is not None else 0
        except (TypeError, ValueError):
            requested_offset = 0
        requested_limit = max(1, min(requested_limit, 50))
        requested_offset = max(0, requested_offset)
        url = (
            "https://api.spotify.com/v1/me/tracks"
            f"?limit={requested_limit}&offset={requested_offset}"
        )
        payload = spotify_user_request(user_id, url)
        if not payload:
            return jsonify({"ok": False, "error": "not authorized"}), 401
        items = payload.get("items") or []
        tracks = []
        for it in items:
            track = it.get("track") or {}
            artists = track.get("artists") or []
            artist_name = ", ".join([a.get("name", "") for a in artists if a.get("name")]).strip() or "Unknown Artist"
            images = (track.get("album") or {}).get("images") or []
            cover = images[0].get("url") if images else None
            duration_ms = int(track.get("duration_ms") or 0)
            mins = duration_ms // 60000
            secs = (duration_ms % 60000) // 1000
            tracks.append(
                {
                    "id": track.get("id"),
                    "title": track.get("name"),
                    "artist": artist_name,
                    "duration": f"{mins}:{secs:02d}" if duration_ms else "?:??",
                    "url": (track.get("external_urls") or {}).get("spotify") or "",
                    "cover_url": cover,
                    "source": "spotify",
                    "album_name": (track.get("album") or {}).get("name"),
                }
            )
        total = int(payload.get("total") or 0)
        next_offset = requested_offset + len(items) if requested_offset + len(items) < total else None
        return jsonify(
            {
                "ok": True,
                "tracks": tracks,
                "offset": requested_offset,
                "limit": requested_limit,
                "next_offset": next_offset,
                "has_more": bool(next_offset),
                "total": total,
            }
        )

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



