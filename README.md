# 🎵 Telegram Music Bot

Простой бот для поиска и скачивания музыки с YouTube прямо в Telegram.

## ✨ Возможности

- 🔍 Поиск музыки на YouTube
- 📥 Скачивание в MP3 (192 kbps)
- ⚡ Быстрая работа
- 🎯 Простой интерфейс с кнопками

## 🚀 Быстрый старт

### Вариант 1: Docker (рекомендуется)

1. **Получите токен бота:**
   - Откройте Telegram → @BotFather
   - Отправьте `/newbot`
   - Сохраните токен

2. **Откройте `bot.py` и замените:**
   ```python
   BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"
   ```
   на ваш токен

3. **Запустите:**
   ```bash
   docker-compose up -d --build
   ```

4. **Готово!** Откройте бота в Telegram

### Вариант 2: Без Docker

1. **Установите зависимости:**
   ```bash
   # Windows
   pip install -r requirements.txt
   pip install yt-dlp
   
   # Установите ffmpeg с https://ffmpeg.org/download.html
   ```

2. **Отредактируйте `bot.py`:**
   ```python
   BOT_TOKEN = "ваш_токен_здесь"
   ```

3. **Запустите:**
   ```bash
   python bot.py
   ```

## 📋 Команды бота

- `/start` - Приветствие и инструкции
- Просто напишите название песни - и бот найдёт её!

## 🛠️ Управление (Docker)

```bash
# Просмотр логов
docker-compose logs -f

# Остановка
docker-compose stop

# Перезапуск
docker-compose restart

# Удаление
docker-compose down
```

## 🔧 Настройка

В файле `bot.py` можно изменить:

```python
# Качество MP3 (128K, 192K, 256K, 320K)
'--audio-quality', '192K'

# Количество результатов поиска (1-10)
f'ytsearch5:{query}'  # 5 результатов

# Папка для временных файлов
TEMP_DIR = "/tmp/music_bot"
```

## ❗ Требования

- Python 3.11+
- ffmpeg
- yt-dlp
- python-telegram-bot

Все автоматически установятся через Docker!

## 🐛 Проблемы

### Бот не отвечает
- Проверьте токен в `bot.py`
- Убедитесь что бот запущен: `docker-compose ps`

### Ошибки при скачивании
- Проверьте что ffmpeg установлен: `docker exec telegram-music-bot ffmpeg -version`
- Проверьте логи: `docker-compose logs`

### Долго скачивает
- Это нормально для больших файлов (1-2 минуты)
- YouTube может ограничивать скорость

## 📝 TODO

- [ ] База данных для статистики
- [ ] Система избранного
- [ ] Плейлисты
- [ ] Telegram Mini App интерфейс
- [ ] Поддержка других платформ (SoundCloud, Spotify)

## 📄 Лицензия

MIT License - используйте как хотите!

## 🙏 Благодарности

- [yt-dlp](https://github.com/yt-dlp/yt-dlp) - загрузка с YouTube
- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) - Telegram API
- [ffmpeg](https://ffmpeg.org/) - обработка аудио

---

**Сделано с ❤️ для музыкальных ценителей**
