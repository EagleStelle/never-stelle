# syntax=docker/dockerfile:1.7

ARG APP_VERSION=1.0.0
ARG FFMPEG_VERSION=8.1.2
ARG NODE_VERSION=24
ARG PYTHON_VERSION=3.12

FROM --platform=$BUILDPLATFORM node:${NODE_VERSION}-alpine AS frontend-builder

WORKDIR /app/frontend
ENV CI=1

COPY --link frontend/package.json frontend/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --fund=false

COPY --link frontend/index.html frontend/tsconfig.json frontend/vite.config.ts ./
COPY --link frontend/src ./src
COPY --link frontend/public ./public
RUN npm run build

FROM python:${PYTHON_VERSION}-alpine AS python-wheels

WORKDIR /build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

COPY --link requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel --only-binary=:all: --wheel-dir /wheels -r requirements.txt

FROM python:${PYTHON_VERSION}-alpine AS ffmpeg-builder

ARG FFMPEG_VERSION

WORKDIR /build/ffmpeg

RUN --mount=type=cache,id=never-stelle-ffmpeg-source,target=/var/cache/ffmpeg \
    apk add --no-cache \
    build-base \
    curl \
    lame-dev \
    nasm \
    openssl-dev \
    opus-dev \
    pkgconf \
    x264-dev \
    xz \
    zlib-dev \
    && archive="/var/cache/ffmpeg/ffmpeg-${FFMPEG_VERSION}.tar.xz" \
    && if ! tar -tJf "$archive" >/dev/null 2>&1; then \
        rm -f "$archive" "$archive.tmp"; \
        curl --fail --location \
            --retry 8 \
            --retry-all-errors \
            --retry-delay 2 \
            --connect-timeout 20 \
            --speed-limit 1024 \
            --speed-time 30 \
            --output "$archive.tmp" \
            "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz"; \
        tar -tJf "$archive.tmp" >/dev/null; \
        mv "$archive.tmp" "$archive"; \
    fi \
    && tar -xJf "$archive" --strip-components=1 \
    && ./configure \
    --prefix=/opt/ffmpeg \
    --disable-autodetect \
    --disable-debug \
    --disable-doc \
    --disable-avdevice \
    --disable-programs \
    --enable-swscale \
    --disable-everything \
    --disable-static \
    --disable-stripping \
    --enable-gpl \
    --enable-version3 \
    --enable-ffmpeg \
    --enable-ffprobe \
    --enable-shared \
    --enable-small \
    --enable-openssl \
    --enable-swresample \
    --enable-avfilter \
    --enable-filter=aresample,aformat,anull,format,scale \
    --enable-protocol=concat,crypto,data,file,http,https,pipe,subfile,tcp,tls,udp \
    --enable-demuxer=aac,ass,concat,ffmetadata,flac,flv,gif,hls,image2,matroska,mov,mp3,mpegts,ogg,srt,wav,webvtt \
    --enable-muxer=adts,flac,image2,ipod,matroska,mp3,mp4,ogg,opus,wav,webm \
    --enable-parser=aac,aac_latm,av1,flac,h264,hevc,mpegaudio,opus,vorbis,vp8,vp9 \
    --enable-bsfs \
    --enable-decoder=aac,aac_fixed,aac_latm,alac,ass,av1,flac,gif,mjpeg,movtext,mp3,mp3float,opus,pcm_f32le,pcm_s16le,pcm_s24le,pcm_s32le,png,subrip,vorbis,webp,webvtt \
    --enable-encoder=aac,ass,flac,libmp3lame,libopus,mjpeg,movtext,pcm_s16le,png,subrip,webvtt \
    --enable-libmp3lame \
    --enable-libopus \
    --enable-libx264 \
    --enable-encoder=libx264 \
    --enable-zlib \
    --extra-cflags="-Os -ffunction-sections -fdata-sections" \
    --extra-ldflags="-Wl,--as-needed -Wl,--gc-sections" \
    && make -j"$(nproc)" \
    && make install \
    && find /opt/ffmpeg -type f \( -perm /111 -o -name '*.so*' \) -exec strip --strip-unneeded {} + \
    && rm -rf /opt/ffmpeg/include /opt/ffmpeg/lib/pkgconfig /opt/ffmpeg/share

FROM python:${PYTHON_VERSION}-alpine AS runtime

ARG APP_VERSION
ARG BUILD_DATE
ARG VCS_REF

LABEL org.opencontainers.image.title="Never Stelle" \
    org.opencontainers.image.description="Self-hosted web app that downloads videos, images, and audios" \
    org.opencontainers.image.version="${APP_VERSION}" \
    org.opencontainers.image.created="${BUILD_DATE}" \
    org.opencontainers.image.revision="${VCS_REF}" \
    org.opencontainers.image.source="https://github.com/EagleStelle/never-stelle" \
    org.opencontainers.image.url="https://hub.docker.com/r/eaglestelle/never-stelle" \
    org.opencontainers.image.documentation="https://github.com/EagleStelle/never-stelle#readme" \
    org.opencontainers.image.vendor="EagleStelle" \
    org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

ENV LD_LIBRARY_PATH=/opt/ffmpeg/lib \
    PATH="/opt/ffmpeg/bin:${PATH}" \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apk add --no-cache ca-certificates lame-libs opus nodejs x264-libs upx binutils \
    && upx --fast /usr/bin/node \
    && apk del upx binutils \
    && rm -rf /usr/lib/node_modules/npm /usr/bin/npm /usr/bin/npx /usr/share/man /usr/share/doc \
    && mkdir -p /data /media /scratch

COPY --link --from=ffmpeg-builder /opt/ffmpeg /opt/ffmpeg

RUN --mount=type=bind,from=python-wheels,source=/wheels,target=/wheels \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    pip install --root-user-action=ignore --no-index --find-links=/wheels --no-compile -r requirements.txt \
    && python -m pip uninstall --root-user-action=ignore -y pip setuptools wheel \
    && find /usr/local -type f -name '*.so*' -exec strip --strip-unneeded {} + 2>/dev/null || true \
    && find /usr/local -type d -name '__pycache__' -prune -exec rm -rf '{}' + \
    && rm -rf \
    /usr/local/lib/python*/ensurepip \
    /usr/local/lib/python*/idlelib \
    /usr/local/lib/python*/lib2to3 \
    /usr/local/lib/python*/tkinter \
    /usr/local/lib/python*/turtledemo \
    /usr/local/lib/python*/pydoc_data \
    /usr/local/lib/python*/unittest \
    /usr/local/lib/python*/test \
    /usr/local/lib/python*/sqlite3/test \
    /usr/local/bin/2to3* \
    /usr/local/bin/idle* \
    /usr/local/bin/pydoc*

COPY --link backend ./backend
COPY --link yt_dlp_plugins ./yt_dlp_plugins
COPY --link --from=frontend-builder /app/frontend/dist ./frontend/dist

EXPOSE 8840
STOPSIGNAL SIGTERM

CMD ["python", "-m", "backend.app.runtime.server"]
