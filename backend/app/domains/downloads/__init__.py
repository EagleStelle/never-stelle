"""Download domain package.

- links          : source URLs, redirects and learned route facts
- naming         : filename rules, title cleaning and template rendering
- metadata       : metadata values, creators and scraped tokens for the naming pipeline
- engines        : yt-dlp / gallery-dl backends and probing
- postprocessing : tagging, embedding and container repair of finished files
- library        : download history, folder scan, resolve and rename
- workers        : queue scheduling, execution and completion

Root modules hold what every layer shares: constants and the queue store, plus the
operations and serializers the API calls. Option vocabulary lives in `domains.options`
and learned URL formats in `domains.formats`.
"""
