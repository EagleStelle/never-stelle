from __future__ import annotations

# Each step owns a slice of 0-100.
PREPARE_END = 8.0
DOWNLOAD_END = 88.0
FINALIZE_END = 99.0

# Share of the room left that a restarted stream gets, so any stream count converges.
_SEGMENT_SHARE = 0.7
_RESTART_DROP = 5.0


def _clamp01(fraction: float) -> float:
    return max(0.0, min(1.0, float(fraction)))


class TaskProgress:
    """Maps each step's own percentage onto one monotonic 0-100 task bar."""

    def __init__(self) -> None:
        self._value = 0.0
        self._segment_start = PREPARE_END
        self._segment_end = DOWNLOAD_END
        self._last_raw = -1.0

    @property
    def value(self) -> float:
        return self._value

    def prepare(self, fraction: float) -> float:
        """Scraping, counting and command building, before any bytes move."""
        return self._advance(PREPARE_END * _clamp01(fraction))

    def download(self, raw_pct: float, *, partial: bool = False) -> float:
        """A downloader percentage. `partial` marks one stream of an unknown number."""
        raw = max(0.0, min(100.0, float(raw_pct)))
        if self._last_raw < 0 or raw < self._last_raw - _RESTART_DROP:
            self._open_segment(partial or self._last_raw >= 0)
        self._last_raw = raw
        span = self._segment_end - self._segment_start
        return self._advance(min(self._segment_start + span * raw / 100, DOWNLOAD_END))

    def finalize(self, fraction: float) -> float:
        """Renaming, remuxing and post-processing the finished outputs."""
        return self._advance(DOWNLOAD_END + (FINALIZE_END - DOWNLOAD_END) * _clamp01(fraction))

    def _open_segment(self, share_the_room: bool) -> None:
        # A restart picks the line up where it stands, then takes a share of what is
        # left so any number of further restarts still fits inside the slice.
        self._segment_start = max(self._value, PREPARE_END)
        room = DOWNLOAD_END - self._segment_start
        self._segment_end = self._segment_start + room * (_SEGMENT_SHARE if share_the_room else 1.0)

    def _advance(self, value: float) -> float:
        self._value = max(self._value, round(min(value, 100.0), 1))
        return self._value
