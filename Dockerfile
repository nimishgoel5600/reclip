FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg curl unzip && \
    rm -rf /var/lib/apt/lists/*

# Install Deno (required by yt-dlp for YouTube JS extraction)
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh
ENV DENO_DIR=/tmp/deno

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 10000
ENV HOST=0.0.0.0
ENV PORT=10000
CMD ["python", "app.py"]
