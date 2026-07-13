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

RUN apk add --no-cache \
        build-base \
        lame-dev \
        nasm \
        openssl-dev \
        opus-dev \
        pkgconf \
        wget \
        xz \
        zlib-dev \
    && wget -O /tmp/ffmpeg.tar.xz "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" \
    && tar -xJf /tmp/ffmpeg.tar.xz --strip-components=1 \
    && ./configure \
        --prefix=/opt/ffmpeg \
        --disable-autodetect \
        --disable-debug \
        --disable-doc \
        --disable-avdevice \
        --disable-programs \
        --disable-swscale \
        --disable-everything \
        --disable-static \
        --disable-stripping \
        --enable-ffmpeg \
        --enable-ffprobe \
        --enable-shared \
        --enable-small \
        --enable-openssl \
        --enable-swresample \
        --enable-avfilter \
        --enable-filter=aresample,aformat,anull \
        --enable-protocol=concat,crypto,data,file,http,https,pipe,subfile,tcp,tls,udp \
        --enable-demuxer=aac,concat,flac,flv,hls,matroska,mov,mp3,mpegts,ogg,wav \
        --enable-muxer=adts,flac,ipod,matroska,mp3,mp4,ogg,opus,wav,webm \
        --enable-parser=aac,aac_latm,av1,flac,h264,hevc,mpegaudio,opus,vorbis,vp8,vp9 \
        --enable-bsfs \
        --enable-decoder=aac,aac_fixed,aac_latm,alac,flac,mp3,mp3float,opus,pcm_f32le,pcm_s16le,pcm_s24le,pcm_s32le,vorbis \
        --enable-encoder=aac,flac,libmp3lame,libopus,pcm_s16le \
        --enable-libmp3lame \
        --enable-libopus \
        --enable-zlib \
        --extra-cflags="-Os -ffunction-sections -fdata-sections" \
        --extra-ldflags="-Wl,--as-needed -Wl,--gc-sections" \
    && make -j"$(nproc)" \
    && make install \
    && find /opt/ffmpeg -type f \( -perm /111 -o -name '*.so*' \) -exec strip --strip-unneeded {} + \
    && rm -rf /opt/ffmpeg/include /opt/ffmpeg/lib/pkgconfig /opt/ffmpeg/share

FROM python:${PYTHON_VERSION}-alpine AS runtime

ARG APP_VERSION

LABEL org.opencontainers.image.title="Never Stelle" \
      org.opencontainers.image.description="Self-hosted media download manager" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

ENV LD_LIBRARY_PATH=/opt/ffmpeg/lib \
    PATH="/opt/ffmpeg/bin:${PATH}" \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apk add --no-cache ca-certificates lame-libs opus \
    && mkdir -p /data /media /scratch

COPY --link --from=ffmpeg-builder /opt/ffmpeg /opt/ffmpeg

COPY --link requirements.txt .
COPY --link --from=python-wheels /wheels /wheels
RUN pip install --root-user-action=ignore --no-index --find-links=/wheels --no-compile -r requirements.txt \
    && python -m pip uninstall --root-user-action=ignore -y pip setuptools wheel \
    && rm -rf /wheels \
    && find /usr/local -type d -name '__pycache__' -prune -exec rm -rf '{}' + \
    && rm -rf \
        /usr/local/lib/python*/ensurepip \
        /usr/local/lib/python*/idlelib \
        /usr/local/lib/python*/lib2to3 \
        /usr/local/lib/python*/tkinter \
        /usr/local/lib/python*/turtledemo \
        /usr/local/lib/python*/pydoc_data \
        /usr/local/bin/2to3* \
        /usr/local/bin/idle* \
        /usr/local/bin/pydoc*

COPY --link backend ./backend
COPY --link --from=frontend-builder /app/frontend/dist ./frontend/dist

EXPOSE 8840
STOPSIGNAL SIGTERM

CMD ["python", "-m", "backend.app.server"]
