FROM golang:1.25-bookworm AS iwaradl-builder

WORKDIR /src
RUN apt-get update && apt-get install -y git ca-certificates && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/Izumiko/iwaradl.git .
RUN CGO_ENABLED=0 go build -o /out/iwaradl .

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir flask requests PyYAML yt-dlp instaloader

COPY --from=iwaradl-builder /out/iwaradl /usr/local/bin/iwaradl
COPY app.py /app/
COPY templates /app/templates
COPY static /app/static

EXPOSE 8088
CMD ["python", "app.py"]
