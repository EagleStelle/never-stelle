from __future__ import annotations

import asyncio
import os

from uvicorn import Config
from uvicorn.server import Server

DEFAULT_IDLE_TICK_SECONDS = 5.0
MIN_IDLE_TICK_SECONDS = 0.1
MAX_IDLE_TICK_SECONDS = 30.0


def idle_tick_seconds() -> float:
    raw_value = str(os.environ.get("NEVER_STELLE_IDLE_TICK_SECONDS") or "").strip()
    if not raw_value:
        return DEFAULT_IDLE_TICK_SECONDS
    try:
        value = float(raw_value)
    except ValueError:
        return DEFAULT_IDLE_TICK_SECONDS
    return min(MAX_IDLE_TICK_SECONDS, max(MIN_IDLE_TICK_SECONDS, value))


class LowIdleServer(Server):
    async def main_loop(self) -> None:
        counter = 0
        should_exit = await self.on_tick(counter)
        tick_seconds = idle_tick_seconds()
        while not should_exit:
            await asyncio.sleep(tick_seconds)
            # Uvicorn's on_tick does its once-per-second work when counter is divisible by 10.
            counter = (counter + 10) % 864000
            should_exit = await self.on_tick(counter)


def main() -> None:
    config = Config(
        "backend.app.main:app",
        host="0.0.0.0",
        port=8840,
        access_log=False,
        log_level="warning",
    )
    LowIdleServer(config).run()


if __name__ == "__main__":
    main()
