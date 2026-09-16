# syntax=docker/dockerfile:1.7

ARG APP_VERSION=1.0.0
ARG CHROME_VERSION=153.0.8010.47
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
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

COPY --link requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel --only-binary=:all: --wheel-dir /wheels -r requirements.txt

FROM python:${PYTHON_VERSION}-alpine AS ffmpeg-builder

ARG FFMPEG_VERSION

WORKDIR /build/ffmpeg

RUN --mount=type=cache,id=never-stelle-ffmpeg-source,target=/var/cache/ffmpeg \
    apk add --no-cache \
    aom-dev \
    build-base \
    curl \
    dav1d-dev \
    lame-dev \
    libvpx-dev \
    nasm \
    openssl-dev \
    opus-dev \
    pkgconf \
    x264-dev \
    x265-dev \
    xz \
    zlib-dev \
    && archive="/var/cache/ffmpeg/ffmpeg-${FFMPEG_VERSION}.tar" \
    && if ! tar -tf "$archive" >/dev/null 2>&1; then \
        rm -f "$archive" "$archive.tmp"; \
        for url in \
            "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" \
            "https://github.com/FFmpeg/FFmpeg/archive/refs/tags/n${FFMPEG_VERSION}.tar.gz" \
            "https://git.ffmpeg.org/gitweb/ffmpeg.git/snapshot/n${FFMPEG_VERSION}.tar.gz" \
        ; do \
            if curl --fail --location --ipv4 \
                --retry 3 \
                --retry-all-errors \
                --retry-delay 2 \
                --connect-timeout 15 \
                --speed-limit 1024 \
                --speed-time 30 \
                --output "$archive.tmp" \
                "$url" \
                && tar -tf "$archive.tmp" >/dev/null 2>&1; then \
                mv "$archive.tmp" "$archive"; \
                break; \
            fi; \
            rm -f "$archive.tmp"; \
        done; \
        tar -tf "$archive" >/dev/null; \
    fi \
    && tar -xf "$archive" --strip-components=1 \
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
    --enable-bsf=aac_adtstoasc,av1_frame_merge,dump_extradata,extract_extradata,h264_mp4toannexb,hevc_mp4toannexb,mov2textsub,setts,text2movsub,vp9_superframe \
    --enable-decoder=aac,aac_latm,alac,ass,flac,gif,h264,hevc,libdav1d,mjpeg,movtext,mp3float,opus,pcm_f32le,pcm_s16le,pcm_s24le,pcm_s32le,png,subrip,vorbis,vp8,vp9,webp,webvtt \
    --enable-encoder=aac,ass,flac,libaom_av1,libmp3lame,libopus,libvpx_vp9,libx264,libx265,mjpeg,movtext,pcm_s16le,png,subrip,webvtt \
    --enable-libaom \
    --enable-libdav1d \
    --enable-libmp3lame \
    --enable-libopus \
    --enable-libvpx \
    --enable-libx264 \
    --enable-libx265 \
    --enable-zlib \
    --extra-cflags="-ffunction-sections -fdata-sections" \
    --extra-ldflags="-Wl,--as-needed -Wl,--gc-sections" \
    && make -j"$(nproc)" \
    && make install \
    && find /opt/ffmpeg -type f \( -perm /111 -o -name '*.so*' \) -exec strip --strip-unneeded {} + \
    && rm -rf /opt/ffmpeg/include /opt/ffmpeg/lib/pkgconfig /opt/ffmpeg/share

FROM debian:trixie-slim AS chrome-builder

ARG CHROME_VERSION
ARG TARGETARCH

WORKDIR /build

# Trackers scroll a page in a headless browser to reach items that only load as it grows.
# Chrome for Testing publishes linux64 alone, so other arches ship no archive and list static pages only.
RUN mkdir -p /chrome-dist/bundle /chrome-dist/lib64 \
    && if [ "$TARGETARCH" = "amd64" ]; then \
    apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip xz-utils fonts-liberation \
    && curl --fail --location --ipv4 --retry 3 --retry-all-errors --retry-delay 2 --connect-timeout 15 \
    --output shell.zip \
    "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-headless-shell-linux64.zip" \
    && unzip -q shell.zip \
    && mv chrome-headless-shell-linux64 /chrome \
    # Software rendering, Vulkan, hyphenation and every locale but one are dead weight
    # for a page we only read the DOM of; the locales alone are 48 MB of the download.
    && rm -rf /chrome/libEGL.so /chrome/libGLESv2.so /chrome/libvulkan.so.1 \
    /chrome/libvk_swiftshader.so /chrome/vk_swiftshader_icd.json \
    /chrome/LICENSE.headless_shell /chrome/ABOUT /chrome/*.deps /chrome/hyphen-data \
    && find /chrome/locales -type f ! -name 'en-US.pak' -delete \
    && mkdir -p /chrome/lib /chrome/fonts \
    # The libraries the binary links must be present before ldd can name them.
    && apt-get install -y --no-install-recommends \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 libdbus-1-3 libexpat1 libgbm1 \
    libglib2.0-0 libnspr4 libnss3 libx11-6 libxcb1 libxcomposite1 libxdamage1 libxext6 \
    libxfixes3 libxkbcommon0 libxrandr2 \
    && ! ldd /chrome/chrome-headless-shell | grep 'not found' \
    && ldd /chrome/chrome-headless-shell | awk '/=> \//{print $3}' | sort -u | xargs -I{} cp -L {} /chrome/lib/ \
    # NSS opens these at runtime rather than linking them, so ldd never names them.
    && cp -L /usr/lib/x86_64-linux-gnu/libsoftokn3.so /usr/lib/x86_64-linux-gnu/libfreebl*.so \
    /usr/lib/x86_64-linux-gnu/libnssckbi.so /chrome/lib/ \
    # The binary names this interpreter path, and musl leaves it free. It has to be the real
    # interpreter rather than a launcher argument: the browser re-execs /proc/self/exe to spawn
    # its renderer and GPU children, and a loader there is handed --type=renderer and refuses it.
    && cp -L /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 /chrome-dist/lib64/ \
    # Skia aborts without a font configuration, so one family and a one-directory config ship with it.
    && cp /usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf /chrome/fonts/ \
    && printf '%s\n' \
    '<?xml version="1.0"?>' \
    '<!DOCTYPE fontconfig SYSTEM "urn:fontconfig:fonts.dtd">' \
    '<fontconfig>' \
    '  <dir prefix="relative">fonts</dir>' \
    '  <cachedir>/tmp/fontconfig</cachedir>' \
    '</fontconfig>' > /chrome/fonts.conf \
    # The library path is exported for the browser alone: the app runs on musl, and glibc's
    # libraries on its own loader path break it.
    && printf '%s\n' \
    '#!/bin/sh' \
    'dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)' \
    '# The browser reads CDP on fd 3 and writes on fd 4; the app hands both in as stdin and stdout.' \
    'exec 3<&0 4>&1 0</dev/null 1>&2' \
    'export FONTCONFIG_FILE="$dir/fonts.conf"' \
    'export LD_LIBRARY_PATH="$dir/lib"' \
    'exec "$dir/chrome-headless-shell" "$@"' \
    > /chrome/chrome \
    && chmod 755 /chrome/chrome \
    && XZ_OPT='-9 -T0' tar -cJf /chrome-dist/bundle/chrome.tar.xz -C /chrome . \
    && printf '%s' "${CHROME_VERSION}" > /chrome-dist/bundle/version \
    && rm -rf /chrome /build/* /var/lib/apt/lists/*; \
    fi

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
    NEVER_STELLE_IMPERSONATE_PATH=/opt/impersonate \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apk add --no-cache ca-certificates lame-libs opus nodejs aom-libs libdav1d libvpx x264-libs x265-libs tini upx binutils \
    && upx --fast /usr/bin/node \
    && apk del upx binutils \
    && rm -rf /usr/lib/node_modules/npm /usr/bin/npm /usr/bin/npx /usr/share/man /usr/share/doc \
    && mkdir -p /data /media /scratch

COPY --link --from=ffmpeg-builder /opt/ffmpeg /opt/ffmpeg
COPY --link --from=chrome-builder /chrome-dist/bundle/ /opt/chrome/
COPY --link --from=chrome-builder /chrome-dist/lib64/ /lib64/

RUN --mount=type=bind,from=python-wheels,source=/wheels,target=/wheels \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    apk add --no-cache --virtual .strip-deps binutils upx \
    && pip install --root-user-action=ignore --no-index --find-links=/wheels --no-compile -r requirements.txt \
    && python -m pip uninstall --root-user-action=ignore -y pip setuptools wheel \
    && find /usr/local/lib/python*/site-packages -type f \( -name '*.so' -o -name '*.so.*' \) -exec strip --strip-unneeded {} + \
    # yt-dlp loads every installed request handler at startup, so curl_cffi lives off the
    # default path and is only added to PYTHONPATH for attempts that impersonate.
    && site="$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')" \
    && mkdir -p "$NEVER_STELLE_IMPERSONATE_PATH" \
    && mv "$site"/curl_cffi "$site"/curl_cffi-*.dist-info "$site"/_cffi_backend*.so "$NEVER_STELLE_IMPERSONATE_PATH"/ \
    && rm -rf "$site"/cffi "$site"/cffi-*.dist-info "$site"/pycparser "$site"/pycparser-*.dist-info \
    && upx --best "$NEVER_STELLE_IMPERSONATE_PATH"/curl_cffi/_wrapper*.so \
    && PYTHONPATH="$NEVER_STELLE_IMPERSONATE_PATH" yt-dlp --list-impersonate-targets | grep -v unavailable | grep -q curl_cffi \
    && ! python -c 'import curl_cffi' 2>/dev/null \
    && apk del .strip-deps \
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
    /usr/local/bin/2to3* \
    /usr/local/bin/idle* \
    /usr/local/bin/pydoc*

COPY --link backend ./backend
COPY --link --from=frontend-builder /app/frontend/dist ./frontend/dist

EXPOSE 8840
STOPSIGNAL SIGTERM

# An init reaps the browser's children once their parent is killed; the app as PID 1 would leave them zombies.
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["python", "-m", "backend.app.runtime.server"]
