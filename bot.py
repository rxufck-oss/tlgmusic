import asyncio
import json
import logging
import os
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
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
ARTIST_SEARCH_RESULTS = int(os.getenv("ARTIST_SEARCH_RESULTS", "100"))
DOWNLOAD_CACHE_TTL = int(os.getenv("DOWNLOAD_CACHE_TTL", "3600"))
SEARCH_COOLDOWN_SEC = int(os.getenv("SEARCH_COOLDOWN_SEC", "3"))
SC_SEARCH_TIMEOUT_SEC = int(os.getenv("SC_SEARCH_TIMEOUT_SEC", "20"))
WEBAPP_API_PORT = int(os.getenv("WEBAPP_API_PORT", "8080"))

os.makedirs(TEMP_DIR, exist_ok=True)
DOWNLOAD_CACHE: dict[str, dict] = {}
USER_LAST_SEARCH_TS: dict[int, float] = {}
DB_CONN = None
HTTP_API_THREAD = None


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


def put_download_item(url: str, title: str, artist: str | None = None, cover_url: str | None = None) -> str:
    prune_download_cache()
    key = uuid.uuid4().hex[:12]
    DOWNLOAD_CACHE[key] = {
        "url": url,
        "title": title,
        "artist": artist,
        "cover_url": cover_url,
        "source": "soundcloud",
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


def run_yt_dlp(cmd: list[str], timeout: int) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.error("YT-DLP Error (cmd=%s): %s", " ".join(cmd), result.stderr.strip())
        return result
    except subprocess.TimeoutExpired:
        logger.error("YT-DLP Timeout (cmd=%s)", " ".join(cmd))
        return subprocess.CompletedProcess(args=cmd, returncode=124, stdout="", stderr=f"timeout {timeout}s")


def parse_search_results(stdout: str) -> list:
    results = []
    for line in stdout.strip().split("\n"):
        if not line:
            continue
        try:
            data = json.loads(line)
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
                    "url": data.get("webpage_url"),
                    "cover_url": cover,
                    "source": "soundcloud",
                }
            )
        except json.JSONDecodeError:
            continue
    return [r for r in results if r.get("url")]


def search_soundcloud(query: str, limit: int) -> list:
    safe_limit = max(1, min(limit, 200))
    timeout_sec = max(SC_SEARCH_TIMEOUT_SEC, 20 + safe_limit // 4)
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
    return parse_search_results(result.stdout)


def search_music(query: str, limit: int | None = None, artist_mode: bool = False) -> tuple[list, str | None]:
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

        videos = search_soundcloud(query, target_limit)
        if videos:
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
            "bestaudio",
            "--extract-audio",
            "--audio-format",
            "mp3",
            "--audio-quality",
            "192K",
            "--no-playlist",
            "-o",
            output_template,
            source_url,
        ]
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

    audio_file = await asyncio.to_thread(download_audio, item["url"])
    if not audio_file:
        await status.edit_text("❌ Ошибка при скачивании MP3.")
        return

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
        "Поиск и скачивание: SoundCloud\n"
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

    videos, error = await asyncio.to_thread(search_music, query_text, BOT_RESULTS_LIMIT, False)
    username = context.bot.username

    results = []
    for i, video in enumerate(videos[:BOT_RESULTS_LIMIT]):
        dl_key = put_download_item(video["url"], video["title"], video.get("artist"), video.get("cover_url"))
        deep_link = f"https://t.me/{username}?start=dl_{dl_key}"
        artist = video.get("artist") or "Unknown Artist"
        body = f"{artist} | {video['duration']} | soundcloud"
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

    search_msg = await update.message.reply_text(f"🔍 Ищу: *{query}*...", parse_mode="Markdown")
    videos, error = await asyncio.to_thread(search_music, query, BOT_RESULTS_LIMIT, False)

    if not videos:
        await search_msg.edit_text(f"😔 Ошибка поиска: {error}")
        return

    text = "🎵 *Результаты:*\n"
    keyboard = []
    for i, video in enumerate(videos, 1):
        title = video["title"][:35]
        artist = (video.get("artist") or "Unknown Artist")[:25]
        text += f"{i}. {artist} - {title} ({video['duration']})\n"
        dl_key = put_download_item(video["url"], video["title"], video.get("artist"), video.get("cover_url"))
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
    api_bot = Bot(BOT_TOKEN)
    app_root = os.path.dirname(os.path.abspath(__file__))

    @app.get("/")
    def webapp_index():
        return send_from_directory(app_root, "index.html")

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
        videos, error = search_music(query, requested_limit, artist_mode)
        return jsonify({"ok": len(videos) > 0, "error": error, "results": videos})

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
            "source": "soundcloud",
        }
        track_key = make_track_key(item)

        try:
            cached = get_cached_file_id(track_key)
            if cached:
                asyncio.run(
                    api_bot.send_audio(
                        chat_id=chat_id,
                        audio=cached,
                        title=item.get("title"),
                        performer=item.get("artist"),
                    )
                )
                return jsonify({"ok": True, "cached": True})

            audio_file = download_audio(track_url)
            if not audio_file:
                return jsonify({"ok": False, "error": "download failed"}), 500

            thumb_path = download_thumbnail(item.get("cover_url"))
            try:
                with open(audio_file, "rb") as audio:
                    thumb_file = open(thumb_path, "rb") if thumb_path and os.path.exists(thumb_path) else None
                    try:
                        sent = asyncio.run(
                            api_bot.send_audio(
                                chat_id=chat_id,
                                audio=audio,
                                title=item.get("title"),
                                performer=item.get("artist"),
                                thumbnail=thumb_file,
                            )
                        )
                        if sent and sent.audio and sent.audio.file_id:
                            save_cached_file_id(track_key, sent.audio.file_id)
                    finally:
                        if thumb_file:
                            thumb_file.close()
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
    app.run_polling()


if __name__ == "__main__":
    main()
