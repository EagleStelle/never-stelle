from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass, field
from typing import Any

from backend.app.domains.access.rotation import AccessIdentity, access_env
from backend.app.domains.downloads.engines.gallerydl import gallerydl_access_args
from backend.app.domains.downloads.engines.probe import probe_rotation
from backend.app.domains.downloads.engines.ytdlp import ytdlp_access_args
from backend.app.runtime.processes import (
    current_task_id,
    kill_process_tree,
    low_priority_command,
    raise_if_cancelled,
    register_process,
    unregister_process,
)

# A listing that prints nothing for this long is stuck, not slow.
_IDLE_TIMEOUT_SECONDS = 300


_LOG_TAIL = 20


_UNSUPPORTED_RE = re.compile(r"unsupported url", re.IGNORECASE)


# An engine's report of something it could not read, as gallery-dl's "[site][error]" or yt-dlp's "ERROR:".
_ERROR_LINE_RE = re.compile(r"^(?:\[[^\]]+\]\[error\]|ERROR:)\s*")


@dataclass
class _Run:
    returncode: int = -1
    messages: int = 0
    log: list[str] = field(default_factory=list)
    # The last error the engine reported; its JSON job exits 0 even when items failed.
    error: str = ""
    # Set when the listing stopped at a run of settled entries, before its end.
    caught_up: bool = False

    @property
    def answered(self) -> bool:
        # gallery-dl's JSON job exits 0 even when extraction failed, so silence is a failure too.
        return self.returncode == 0 and self.messages > 0

    @property
    def clean(self) -> bool:
        """Whether the listing reached its end, or caught up, without an item it could not read."""
        return self.caught_up or (self.answered and not self.error)

    @property
    def detail(self) -> str:
        return self.error or (self.log[-1].strip() if self.log else "")

    def note(self, line: str) -> None:
        """Keep a line the engine printed besides its JSON."""
        self.log = [*self.log[-_LOG_TAIL:], line]
        if _ERROR_LINE_RE.match(line):
            self.error = _ERROR_LINE_RE.sub("", line, count=1)


def _stream(cmd: list[str], access: AccessIdentity, run: _Run) -> Iterator[Any]:
    """Yield each JSON line the command prints; other lines are kept as its log.

    Closing the iterator early kills the process, so a caller can stop a long listing; so does cancelling
    the task it runs in.
    """
    cmd, kwargs = low_priority_command(cmd, {"start_new_session": True} if os.name != "nt" else {})
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=access_env(access),
        **kwargs,
    )
    task_id = current_task_id()
    register_process(task_id, process)
    last_output = [time.monotonic()]
    # Set while the caller handles a message, so slow probing is not taken for a silent listing.
    handling = threading.Event()
    done = threading.Event()

    def watchdog() -> None:
        while not done.wait(5):
            if not handling.is_set() and time.monotonic() - last_output[0] > _IDLE_TIMEOUT_SECONDS:
                run.log.append("Listing stopped responding.")
                kill_process_tree(process)
                return

    threading.Thread(target=watchdog, name="never-stelle-listing-watchdog", daemon=True).start()
    try:
        for line in process.stdout or ():
            last_output[0] = time.monotonic()
            text = line.strip()
            if text[:1] in "[{":
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    pass
                else:
                    run.messages += 1
                    handling.set()
                    try:
                        yield message
                    finally:
                        last_output[0] = time.monotonic()
                        handling.clear()
                    continue
            if text:
                run.note(text)
        run.returncode = process.wait()
        # A cancelled listing was killed, not finished.
        raise_if_cancelled()
    finally:
        done.set()
        unregister_process(task_id, process)
        kill_process_tree(process)
        process.wait()


def _gallerydl_command(access: AccessIdentity) -> list[str]:
    return ["gallery-dl", "-j", "-o", "output.jsonl=true", *gallerydl_access_args(access)]


def _ytdlp_command(access: AccessIdentity) -> list[str]:
    return [
        "yt-dlp",
        "--flat-playlist",
        "--lazy-playlist",
        "--dump-json",
        "--no-warnings",
        "--js-runtimes",
        "node",
        "--remote-components",
        "ejs:github",
        *ytdlp_access_args(access),
    ]


_Parser = Callable[[Iterator[Any], list[str]], Iterator[Any]]


def _engine_entries(
    url: str,
    source_key: str,
    command: Callable[[AccessIdentity], list[str]],
    parse: _Parser,
    sub_collections: list[str],
) -> tuple[Iterator[Any], _Run]:
    """Entries from one engine, walking the access rotation until an attempt finishes cleanly."""
    outcome = _Run()

    def entries() -> Iterator[Any]:
        with closing(probe_rotation(url, source_key)) as rotation:
            for access in rotation:
                run = _Run()
                yield from parse(_stream([*command(access), url], access, run), sub_collections)
                outcome.returncode, outcome.messages, outcome.log, outcome.error = (
                    run.returncode,
                    run.messages,
                    run.log,
                    run.error,
                )
                # Another identity cannot make an engine support a link.
                if run.clean or _UNSUPPORTED_RE.search(" ".join(run.log)):
                    return
                access.report("\n".join(run.log))

    return entries(), outcome
