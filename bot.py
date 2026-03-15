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
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "").strip()
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "").strip()
SPOTIFY_PROXY = os.getenv("SPOTIFY_PROXY", "").strip()
SC_CLIENT_ID = os.getenv("SC_CLIENT_ID", "").strip()
SC_API_TIMEOUT_SEC = int(os.getenv("SC_API_TIMEOUT_SEC", "12"))
MAX_TRACK_DURATION_SEC = int(os.getenv("MAX_TRACK_DURATION_SEC", "0"))
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


def make_search_cache_key(query: str, limit: int, artist_mode: bool, include_covers: bool, source: str) -> str:
    return f"{source}|{query.lower().strip()}|{limit}|{int(artist_mode)}|{int(include_covers)}"


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
) -> dict | None:
    try:
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
    except Exception as e:
        logger.error("HTTP JSON request failed (%s): %s", url, e)
        return None


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
        return {"albums": [], "tracks": []}

    albums_raw = ((payload.get("albums") or {}).get("items")) or []
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

    for album in albums_raw[: min(10, len(albums_raw))]:
        album_id = album.get("id")
        if not album_id:
            continue
        track_payload = http_json_request(
            f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=1",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            proxy=SPOTIFY_PROXY or None,
        )
        if not track_payload:
            continue
        items = track_payload.get("items") or []
        if not items:
            continue
        it = items[0]
        artists = it.get("artists") or []
        artist = ", ".join([a.get("name", "") for a in artists if a.get("name")]).strip() or "Unknown Artist"
        duration_ms = int(it.get("duration_ms") or 0)
        mins = duration_ms // 60000
        secs = (duration_ms % 60000) // 1000
        images = album.get("images") or []
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


def search_spotify(query: str, limit: int = 20, include_meta: bool = False) -> list:
    token = get_spotify_token()
    if not token:
        return []

    safe_limit = max(1, min(limit, 50))
    encoded_q = urllib.parse.quote(query)
    url = f"https://api.spotify.com/v1/search?q={encoded_q}&type=track&limit={safe_limit}"
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


def search_music(
    query: str,
    limit: int | None = None,
    artist_mode: bool = False,
    include_covers: bool = True,
    source: str = "soundcloud",
    include_meta: bool = False,
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

        if source == "spotify":
            if not is_spotify_configured():
                return [], "Spotify не настроен. Добавьте SPOTIFY_CLIENT_ID и SPOTIFY_CLIENT_SECRET."
            spotify_results = search_spotify(query, target_limit, include_meta=include_meta)
            if spotify_results:
                return spotify_results, None
            return [], "Spotify не вернул результаты. Попробуйте другой запрос."

        cache_key = make_search_cache_key(query, target_limit, artist_mode, include_covers, source)
        cached = get_search_cache(cache_key)
        if cached is not None:
            return cached, None

        videos = search_soundcloud_api(query, target_limit, include_covers=include_covers)
        if not videos:
            videos = search_soundcloud(query, target_limit, include_covers=include_covers)
        if videos:
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
        try:
            requested_limit = int(raw_limit) if raw_limit is not None else MAX_SEARCH_RESULTS
        except (TypeError, ValueError):
            requested_limit = MAX_SEARCH_RESULTS

        artist_mode = bool(payload.get("artistMode")) or bool(payload.get("artist"))
        include_covers = bool(payload.get("includeCovers", False))
        include_meta = bool(payload.get("includeMeta", False))
        source = str(payload.get("source") or "soundcloud").strip().lower()
        if artist_mode:
            requested_limit = max(10, min(requested_limit, 200))
        else:
            requested_limit = max(5, min(requested_limit, 60))
        videos, error = search_music(query, requested_limit, artist_mode, include_covers, source, include_meta)
        return jsonify({"ok": len(videos) > 0, "error": error, "results": videos})

    @app.get("/api/new-releases")
    def api_new_releases():
        if not is_spotify_configured():
            return jsonify({"ok": False, "error": "Spotify не настроен", "albums": [], "tracks": []}), 400

        raw_limit = request.args.get("limit")
        raw_country = request.args.get("country")
        try:
            requested_limit = int(raw_limit) if raw_limit is not None else 12
        except (TypeError, ValueError):
            requested_limit = 12
        country = str(raw_country or "US").strip()[:2]
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



