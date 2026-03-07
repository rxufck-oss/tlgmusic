FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Создаем принудительный путь для программ
RUN ln -s /usr/bin/node /usr/local/bin/node || true

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -U yt-dlp

COPY . .
RUN mkdir -p /tmp/music_bot && chmod -R 777 /tmp/music_bot

CMD ["python", "bot.py"]