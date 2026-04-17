"""Instagram download execution and task orchestration services."""

import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import instaloader

from app.config import INSTAGRAM_STAGING_DIR, PROGRESS_RE
from app.storage.settings_store import (
    create_instaloader_client,
    ensure_instagram_login,
    get_effective_template_settings,
    get_instagram_auth_status,
    prepare_runtime_cookies,
)
from app.storage.task_store import load_non_iwara_task, update_non_iwara_task
from app.utils.media import (
    capture_new_media_files,
    choose_best_media_file,
    create_zip_from_paths,
    list_media_files,
    unique_output_path,
)
from app.utils.platforms.instagram import (
    append_instagram_log,
    build_instagram_archive_name,
    build_instagram_final_filename,
    build_instagram_highlight_url_context,
    build_instagram_post_context,
    build_instagram_post_url,
    build_instagram_profile_pic_context,
    build_instagram_story_url_context,
    build_instagram_url_context,
    enrich_instagram_context_from_ytdlp_info,
    move_instagram_downloads,
    prepare_instagram_post_ytdlp_output,
    prepare_instagram_story_ytdlp_output,
    resolve_instagram_highlight_owner_username,
    summarize_instagram_paths,
)
from app.utils.process import ActivityWatchdog
from app.utils.url import canonicalize_source_url, parse_instagram_target, to_str
from app.workers import clear_task_cancelled, is_task_cancelled
from app.utils.ytdlp import (
    build_general_ytdlp_cmd,
    detect_ffmpeg_location,
    extract_downloaded_path_from_log_line,
    try_extract_ytdlp_info,
)


def _build_instagram_ytdlp_cmd(
    source_url: str, ffmpeg_location: str, output_template: str
) -> list[str]:
    cookies_file = prepare_runtime_cookies(source_url)
    return build_general_ytdlp_cmd(
        source_url,
        ffmpeg_location,
        output_template,
        cookies_file=cookies_file,
    )


def download_instagram_post_video_with_ytdlp(
    source_url: str,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    context: dict[str, Any],
    *,
    preferred_id: str = "",
    task_id: str | None = None,
) -> list[Path]:
    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for Instagram reel downloads.")

    info = try_extract_ytdlp_info(source_url, cookies_file=prepare_runtime_cookies(source_url))
    context = enrich_instagram_context_from_ytdlp_info(context, info, fallback_title="reel")
    target_dir, output_template, expected_name = prepare_instagram_post_ytdlp_output(
        output_root,
        folder_template,
        filename_template,
        context,
    )
    before = {str(path.resolve()) for path in list_media_files(target_dir)}
    cmd = _build_instagram_ytdlp_cmd(source_url, ffmpeg_location, output_template)

    process: subprocess.Popen[str] | None = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if task_id:
            update_non_iwara_task(
                task_id,
                command=" ".join(shlex.quote(part) for part in cmd),
                ffmpeg_location=ffmpeg_location,
            )

        watchdog = ActivityWatchdog(process)
        if process.stdout is not None:
            for raw_line in process.stdout:
                watchdog.ping()
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-30:]
                downloaded_path = extract_downloaded_path_from_log_line(line)
                if downloaded_path:
                    last_dest = downloaded_path
                if task_id:
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    match = PROGRESS_RE.search(line)
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    update_non_iwara_task(task_id, **updates)
        watchdog.cancel()

        rc = process.wait()
        if watchdog.timed_out:
            raise RuntimeError("Task timed out: no output for too long.")
        if rc != 0:
            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            raise RuntimeError(detail)

        candidates = [path for path in list_media_files(target_dir) if str(path.resolve()) not in before]
        if last_dest:
            last_path = Path(last_dest)
            if last_path.exists() and last_path.is_file():
                candidates.append(last_path)

        best = choose_best_media_file(
            candidates,
            preferred_stem=Path(expected_name).stem,
            preferred_id=to_str(preferred_id or context.get("video_id", "")),
        )
        if not best:
            best = choose_best_media_file(
                list_media_files(target_dir),
                preferred_stem=Path(expected_name).stem,
                preferred_id=to_str(preferred_id or context.get("video_id", "")),
            )
        if not best:
            raise RuntimeError("yt-dlp finished but no Instagram reel video file was produced.")

        actual_ext = best.suffix.lstrip(".").lower() or "mp4"
        final_name = build_instagram_final_filename(filename_template, context, actual_ext)
        final_path = best
        if best.name != final_name:
            renamed_target = unique_output_path(target_dir / final_name)
            shutil.move(str(best), str(renamed_target))
            final_path = renamed_target

        if task_id:
            update_non_iwara_task(
                task_id,
                resolved_full_path=str(final_path),
                resolved_folder=str(final_path.parent),
                resolved_filename=final_path.name,
            )
        return [final_path]
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def download_instagram_highlight_with_ytdlp(
    source_url: str,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    *,
    target: dict[str, str] | None = None,
    task_id: str | None = None,
) -> list[Path]:
    target = dict(target or parse_instagram_target(source_url))
    if target.get("mode") != "highlight":
        raise RuntimeError("yt-dlp highlight mode requires an Instagram highlight URL.")

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for Instagram highlight downloads.")

    owner_username = resolve_instagram_highlight_owner_username(source_url)
    if owner_username:
        target["username"] = owner_username

    context = build_instagram_highlight_url_context(target, source_url, fallback_title="highlight")
    info = try_extract_ytdlp_info(source_url, cookies_file=prepare_runtime_cookies(source_url))
    context = enrich_instagram_context_from_ytdlp_info(context, info, fallback_title="highlight")

    if owner_username:
        context["author"] = owner_username
        context["author_nickname"] = owner_username
        context["creator"] = owner_username

    title = to_str(context.get("title")).strip()
    video_id = to_str(context.get("video_id") or context.get("id")).strip()
    if not title or title.lower() in {"highlight", "story", "media", "instagram"} or title == video_id:
        context["title"] = "highlight"

    folder_rendered = (
        __import__("app.utils.templates", fromlist=["render_template_string"]).render_template_string(
            folder_template, context
        )
    )
    from app.utils.media import safe_component

    target_dir = output_root / safe_component(folder_rendered) if folder_rendered else output_root
    target_dir.mkdir(parents=True, exist_ok=True)
    before = {str(path.resolve()) for path in list_media_files(target_dir)}
    output_template = str(target_dir / "%(id)s.%(ext)s")
    cmd = _build_instagram_ytdlp_cmd(source_url, ffmpeg_location, output_template)

    process: subprocess.Popen[str] | None = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if task_id:
            update_non_iwara_task(
                task_id,
                command=" ".join(shlex.quote(part) for part in cmd),
                ffmpeg_location=ffmpeg_location,
            )

        watchdog = ActivityWatchdog(process)
        if process.stdout is not None:
            for raw_line in process.stdout:
                watchdog.ping()
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-30:]
                downloaded_path = extract_downloaded_path_from_log_line(line)
                if downloaded_path:
                    last_dest = downloaded_path
                if task_id:
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    match = PROGRESS_RE.search(line)
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    update_non_iwara_task(task_id, **updates)
        watchdog.cancel()

        rc = process.wait()
        if watchdog.timed_out:
            raise RuntimeError("Task timed out: no output for too long.")
        if rc != 0:
            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            raise RuntimeError(detail)

        candidates = [path for path in list_media_files(target_dir) if str(path.resolve()) not in before]
        if last_dest:
            last_path = Path(last_dest)
            if last_path.exists() and last_path.is_file():
                candidates.append(last_path)

        unique_candidates: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            resolved = str(candidate.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            unique_candidates.append(candidate)
        unique_candidates.sort(key=lambda p: p.name.lower())
        if not unique_candidates:
            unique_candidates = [path for path in list_media_files(target_dir) if path.is_file()]
            unique_candidates.sort(key=lambda p: p.name.lower())
        if not unique_candidates:
            raise RuntimeError(
                "yt-dlp finished but no Instagram highlight media files were produced."
            )

        total = len(unique_candidates)
        moved: list[Path] = []
        for index, src in enumerate(unique_candidates, start=1):
            ext = src.suffix.lstrip(".").lower() or "mp4"
            final_name = build_instagram_final_filename(
                filename_template,
                context,
                ext,
                index=index,
                total=total,
            )
            final_path = src
            if src.name != final_name:
                renamed_target = unique_output_path(target_dir / final_name)
                shutil.move(str(src), str(renamed_target))
                final_path = renamed_target
            moved.append(final_path)

        if task_id:
            final_path, final_folder, final_name = summarize_instagram_paths(moved, str(target_dir))
            payload: dict[str, Any] = {
                "resolved_full_path": final_path,
                "resolved_folder": final_folder,
                "resolved_filename": final_name,
                "downloaded_files": [
                    str(path) for path in moved if path.exists() and path.is_file()
                ],
            }
            if len(moved) > 1:
                payload["resolved_archive_name"] = build_instagram_archive_name(
                    filename_template,
                    context,
                    total=len(moved),
                )
            update_non_iwara_task(task_id, **payload)

        return moved
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def download_instagram_story_video_with_ytdlp(
    source_url: str,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    *,
    target: dict[str, str] | None = None,
    task_id: str | None = None,
) -> list[Path]:
    target = dict(target or parse_instagram_target(source_url))
    if target.get("mode") != "stories":
        raise RuntimeError("yt-dlp story mode requires an Instagram stories URL.")

    ffmpeg_location = detect_ffmpeg_location()
    if not ffmpeg_location:
        raise RuntimeError("ffmpeg was not found for Instagram story downloads.")

    context, target_dir, output_template, expected_name = prepare_instagram_story_ytdlp_output(
        output_root,
        folder_template,
        filename_template,
        target,
        fallback_title="story",
    )
    info = try_extract_ytdlp_info(source_url, cookies_file=prepare_runtime_cookies(source_url))
    context = enrich_instagram_context_from_ytdlp_info(context, info, fallback_title="story")
    expected_name = build_instagram_final_filename(filename_template, context, "mp4")

    before = {str(path.resolve()) for path in list_media_files(target_dir)}
    cmd = _build_instagram_ytdlp_cmd(source_url, ffmpeg_location, output_template)

    process: subprocess.Popen[str] | None = None
    last_dest = ""
    log_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if task_id:
            update_non_iwara_task(
                task_id,
                command=" ".join(shlex.quote(part) for part in cmd),
                ffmpeg_location=ffmpeg_location,
            )

        watchdog = ActivityWatchdog(process)
        if process.stdout is not None:
            for raw_line in process.stdout:
                watchdog.ping()
                line = raw_line.strip()
                if not line:
                    continue
                log_lines.append(line)
                log_lines = log_lines[-30:]
                downloaded_path = extract_downloaded_path_from_log_line(line)
                if downloaded_path:
                    last_dest = downloaded_path
                if task_id:
                    updates: dict[str, Any] = {"last_log_lines": log_lines}
                    match = PROGRESS_RE.search(line)
                    if match:
                        updates["progress_pct"] = float(match.group(1))
                    update_non_iwara_task(task_id, **updates)
        watchdog.cancel()

        rc = process.wait()
        if watchdog.timed_out:
            raise RuntimeError("Task timed out: no output for too long.")
        if rc != 0:
            tail = "\n".join(log_lines[-12:]).strip()
            detail = f"yt-dlp exited with code {rc}."
            if tail:
                detail = f"{detail}\n{tail}"
            raise RuntimeError(detail)

        candidates = [path for path in list_media_files(target_dir) if str(path.resolve()) not in before]
        if last_dest:
            last_path = Path(last_dest)
            if last_path.exists() and last_path.is_file():
                candidates.append(last_path)

        unique_candidates: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            resolved = str(candidate.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            unique_candidates.append(candidate)
        unique_candidates.sort(key=lambda p: p.name.lower())

        if not unique_candidates:
            best = choose_best_media_file(
                list_media_files(target_dir),
                preferred_stem=Path(expected_name).stem,
                preferred_id=to_str(target.get("story_id") or context.get("video_id", "")),
            )
            if best:
                unique_candidates = [best]

        if not unique_candidates:
            raise RuntimeError("yt-dlp finished but no Instagram story media files were produced.")

        total = len(unique_candidates)
        moved: list[Path] = []
        for index, src in enumerate(unique_candidates, start=1):
            ext = src.suffix.lstrip(".").lower() or "mp4"
            final_name = build_instagram_final_filename(
                filename_template,
                context,
                ext,
                index=index,
                total=total,
            )
            final_path = src
            if src.name != final_name:
                renamed_target = unique_output_path(target_dir / final_name)
                shutil.move(str(src), str(renamed_target))
                final_path = renamed_target
            moved.append(final_path)

        if task_id:
            final_path, final_folder, final_name = summarize_instagram_paths(moved, str(target_dir))
            payload: dict[str, Any] = {
                "resolved_full_path": final_path,
                "resolved_folder": final_folder,
                "resolved_filename": final_name,
                "downloaded_files": [
                    str(path) for path in moved if path.exists() and path.is_file()
                ],
            }
            if len(moved) > 1:
                payload["resolved_archive_name"] = build_instagram_archive_name(
                    filename_template,
                    context,
                    total=len(moved),
                )
            update_non_iwara_task(task_id, **payload)

        return moved
    finally:
        if process and process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def download_instagram_post_to_output(
    loader: instaloader.Instaloader,
    staging_root: Path,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    post: instaloader.Post,
    *,
    creator_hint: str = "",
) -> list[Path]:
    downloaded_files = capture_new_media_files(
        staging_root,
        lambda: loader.download_post(post, target="item"),
    )
    if not downloaded_files:
        raise RuntimeError(
            f"Instaloader did not produce media files for Instagram post {getattr(post, 'shortcode', 'unknown')}."
        )
    moved, _ = move_instagram_downloads(
        downloaded_files,
        output_root,
        folder_template,
        filename_template,
        build_instagram_post_context(post, creator_hint=creator_hint),
    )
    return moved


def download_instagram_profile_pic_to_output(
    loader: instaloader.Instaloader,
    staging_root: Path,
    output_root: Path,
    folder_template: str,
    filename_template: str,
    profile: instaloader.Profile,
) -> list[Path]:
    downloaded_files = capture_new_media_files(
        staging_root,
        lambda: loader.download_profilepic(profile),
    )
    if not downloaded_files:
        return []
    moved, _ = move_instagram_downloads(
        downloaded_files,
        output_root,
        folder_template,
        filename_template,
        build_instagram_profile_pic_context(profile.username),
    )
    return moved


def run_instagram_task(task_id: str, task: dict[str, Any]) -> None:
    source_url = canonicalize_source_url(task.get("source_url") or "")
    output_dir = str(task.get("output_dir") or task.get("resolved_folder") or "").strip()
    if not source_url or not output_dir:
        update_non_iwara_task(
            task_id,
            status="failed",
            error="Missing Instagram URL or output directory.",
        )
        return

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    template_settings = get_effective_template_settings()
    folder_template = template_settings["folder_template"]
    filename_template = template_settings["filename_template"]
    target = parse_instagram_target(source_url)
    mode = target.get("mode")

    staging_root = Path(tempfile.mkdtemp(prefix="neverstelle-ig-", dir=str(INSTAGRAM_STAGING_DIR)))
    loader = create_instaloader_client(staging_root)
    all_paths: list[Path] = []
    primary_folder = str(output_root)
    auth_status = get_instagram_auth_status()

    post_obj: instaloader.Post | None = None
    post_context: dict[str, Any] = {}
    archive_context_source: dict[str, Any] = {}

    def ensure_logged_in(required: bool) -> None:
        login_state = ensure_instagram_login(loader, require_login=required)
        if login_state.get("logged_in"):
            append_instagram_log(
                task_id,
                f"[instagram] Logged in as {login_state.get('username')} ({login_state.get('source')}).",
                progress_pct=2,
            )
        else:
            append_instagram_log(
                task_id,
                "[instagram] Using public Instagram access.",
                progress_pct=2,
            )

    try:
        update_non_iwara_task(
            task_id,
            status="running",
            progress_pct=1,
            error="",
            command=f"instagram:{mode or 'instagram'}",
            last_log_lines=[],
        )

        if mode == "post":
            ensure_logged_in(bool(auth_status.get("configured")))
            shortcode = target.get("shortcode", "")
            append_instagram_log(
                task_id,
                f"[instagram] Downloading post {shortcode}...",
                progress_pct=10,
            )
            post_obj = instaloader.Post.from_shortcode(loader.context, shortcode)
            moved = download_instagram_post_to_output(
                loader,
                staging_root,
                output_root,
                folder_template,
                filename_template,
                post_obj,
            )
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(
                task_id,
                f"[instagram] Downloaded {len(moved)} media file(s) from post {shortcode}.",
                progress_pct=95,
            )
            archive_context_source = build_instagram_post_context(post_obj)

        elif mode == "reel":
            shortcode = target.get("shortcode", "")
            append_instagram_log(
                task_id,
                f"[instagram] Downloading reel {shortcode} with yt-dlp...",
                progress_pct=8,
            )
            post_context = build_instagram_url_context(target, fallback_title="reel")
            if auth_status.get("configured"):
                try:
                    ensure_logged_in(True)
                    post_obj = instaloader.Post.from_shortcode(loader.context, shortcode)
                    post_context = build_instagram_post_context(post_obj)
                except Exception:
                    pass
            moved = download_instagram_post_video_with_ytdlp(
                build_instagram_post_url(shortcode, mode="reel"),
                output_root,
                folder_template,
                filename_template,
                post_context,
                preferred_id=shortcode,
                task_id=task_id,
            )
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(
                task_id,
                f"[instagram] yt-dlp downloaded reel {shortcode}.",
                progress_pct=95,
            )
            archive_context_source = post_context

        elif mode == "stories":
            append_instagram_log(
                task_id,
                "[instagram] Downloading stories with yt-dlp...",
                progress_pct=8,
            )
            moved = download_instagram_story_video_with_ytdlp(
                source_url,
                output_root,
                folder_template,
                filename_template,
                target=target,
                task_id=task_id,
            )
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(
                task_id,
                f"[instagram] yt-dlp downloaded {len(moved)} story media file(s).",
                progress_pct=95,
            )
            archive_context_source = build_instagram_story_url_context(
                target,
                fallback_title="story",
            )
            archive_context_source = enrich_instagram_context_from_ytdlp_info(
                archive_context_source,
                try_extract_ytdlp_info(source_url, cookies_file=prepare_runtime_cookies(source_url)),
                fallback_title="story",
            )

        elif mode == "highlight":
            append_instagram_log(
                task_id,
                "[instagram] Downloading highlight with yt-dlp...",
                progress_pct=8,
            )
            moved = download_instagram_highlight_with_ytdlp(
                source_url,
                output_root,
                folder_template,
                filename_template,
                target=target,
                task_id=task_id,
            )
            all_paths.extend(moved)
            if moved:
                primary_folder = str(moved[0].parent)
            append_instagram_log(
                task_id,
                f"[instagram] yt-dlp downloaded {len(moved)} highlight media file(s).",
                progress_pct=95,
            )
            archive_context_source = build_instagram_highlight_url_context(
                target,
                source_url,
                fallback_title="highlight",
            )
            archive_context_source = enrich_instagram_context_from_ytdlp_info(
                archive_context_source,
                try_extract_ytdlp_info(source_url, cookies_file=prepare_runtime_cookies(source_url)),
                fallback_title="highlight",
            )

        else:
            username = target.get("username", "")
            ensure_logged_in(bool(auth_status.get("configured")))
            profile = instaloader.Profile.from_username(loader.context, username)
            seen_shortcodes: set[str] = set()

            if mode == "profile":
                moved = download_instagram_profile_pic_to_output(
                    loader,
                    staging_root,
                    output_root,
                    folder_template,
                    filename_template,
                    profile,
                )
                all_paths.extend(moved)
                if moved:
                    primary_folder = str(moved[0].parent)
                append_instagram_log(
                    task_id,
                    f"[instagram] Downloading profile content for @{username}...",
                    progress_pct=8,
                )
                collections = [
                    ("posts", profile.get_posts()),
                    ("reels", profile.get_reels()),
                    ("igtv", profile.get_igtv_posts()),
                ]
            elif mode == "tagged":
                append_instagram_log(
                    task_id,
                    f"[instagram] Downloading tagged posts for @{username}...",
                    progress_pct=8,
                )
                collections = [("tagged", profile.get_tagged_posts())]
            elif mode == "profile_reels":
                append_instagram_log(
                    task_id,
                    f"[instagram] Downloading reels for @{username}...",
                    progress_pct=8,
                )
                collections = [("reels", profile.get_reels())]
            elif mode == "igtv":
                append_instagram_log(
                    task_id,
                    f"[instagram] Downloading IGTV for @{username}...",
                    progress_pct=8,
                )
                collections = [("igtv", profile.get_igtv_posts())]
            else:
                raise RuntimeError("Unsupported Instagram URL.")

            downloaded_count = 0
            total_media = max(1, getattr(profile, "mediacount", 0) or 1)
            for label, iterator in collections:
                for post in iterator:
                    shortcode = to_str(getattr(post, "shortcode", ""))
                    if shortcode and shortcode in seen_shortcodes:
                        continue
                    if shortcode:
                        seen_shortcodes.add(shortcode)

                    downloaded_count += 1
                    pct = min(95.0, 10.0 + 85.0 * downloaded_count / total_media)
                    append_instagram_log(
                        task_id,
                        f"[instagram] Downloading {label} item {downloaded_count}...",
                        progress_pct=pct,
                    )

                    if label == "reels":
                        reel_url = build_instagram_post_url(shortcode, mode="reel")
                        moved = download_instagram_post_video_with_ytdlp(
                            reel_url,
                            output_root,
                            folder_template,
                            filename_template,
                            build_instagram_post_context(post, creator_hint=username),
                            preferred_id=shortcode,
                            task_id=task_id,
                        )
                    else:
                        moved = download_instagram_post_to_output(
                            loader,
                            staging_root,
                            output_root,
                            folder_template,
                            filename_template,
                            post,
                            creator_hint=username,
                        )
                    all_paths.extend(moved)
                    if moved:
                        primary_folder = str(moved[0].parent)

            if not all_paths:
                raise RuntimeError(
                    "Instaloader finished but no Instagram media files were downloaded."
                )
            append_instagram_log(
                task_id,
                f"[instagram] Downloaded {len(all_paths)} media file(s) for @{username}.",
                progress_pct=95,
            )
            archive_context_source = {
                "title": target.get("username", "instagram"),
                "id": target.get("username", "instagram"),
                "video_id": target.get("username", "instagram"),
                "author": target.get("username", "instagram"),
                "author_nickname": target.get("username", "instagram"),
                "creator": target.get("username", "instagram"),
                "publish_time": None,
            }

        final_path, final_folder, final_name = summarize_instagram_paths(
            all_paths,
            primary_folder or str(output_root),
        )

        completed_payload: dict[str, Any] = {
            "status": "completed",
            "progress_pct": 100,
            "error": "",
            "resolved_full_path": final_path,
            "resolved_folder": final_folder or str(output_root),
            "resolved_filename": final_name,
            "output_template": "instaloader",
            "downloaded_files": [
                str(path) for path in all_paths if path.exists() and path.is_file()
            ],
        }

        if len(all_paths) > 1:
            existing_archive_name = str(
                load_non_iwara_task(task_id).get("resolved_archive_name") or ""
            ).strip()
            if existing_archive_name:
                completed_payload["resolved_archive_name"] = existing_archive_name
            else:
                completed_payload["resolved_archive_name"] = build_instagram_archive_name(
                    filename_template,
                    archive_context_source,
                    total=len(all_paths),
                )

        update_non_iwara_task(task_id, **completed_payload)

    except Exception as exc:
        if is_task_cancelled(task_id):
            clear_task_cancelled(task_id)
            update_non_iwara_task(task_id, status="failed", error="Cancelled by user.", output_template="instaloader")
        else:
            update_non_iwara_task(
                task_id,
                status="failed",
                error=str(exc),
                output_template="instaloader",
            )
    finally:
        try:
            loader.close()
        except Exception:
            pass
        shutil.rmtree(staging_root, ignore_errors=True)


def download_instagram_to_temp(source_url: str) -> tuple[Path, str]:
    temp_dir = tempfile.mkdtemp(prefix="neverstelle-instagram-device-")
    temp_root = Path(temp_dir)

    staging_root = temp_root / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    output_root = temp_root / "output"
    output_root.mkdir(parents=True, exist_ok=True)

    loader = create_instaloader_client(staging_root)
    try:
        target = parse_instagram_target(source_url)
        template_settings = get_effective_template_settings()
        folder_template = template_settings["folder_template"]
        filename_template = template_settings["filename_template"]
        downloaded: list[Path] = []
        auth_status = get_instagram_auth_status()

        if target.get("mode") == "post":
            ensure_instagram_login(loader, require_login=bool(auth_status.get("configured")))
            post = instaloader.Post.from_shortcode(loader.context, target.get("shortcode", ""))
            downloaded.extend(
                download_instagram_post_to_output(
                    loader,
                    staging_root,
                    output_root,
                    folder_template,
                    filename_template,
                    post,
                )
            )
            archive_context = build_instagram_post_context(post)

        elif target.get("mode") == "reel":
            shortcode = target.get("shortcode", "")
            post_context = build_instagram_url_context(target, fallback_title="reel")
            if auth_status.get("configured"):
                try:
                    ensure_instagram_login(loader, require_login=True)
                    post = instaloader.Post.from_shortcode(loader.context, shortcode)
                    post_context = build_instagram_post_context(post)
                except Exception:
                    pass
            downloaded.extend(
                download_instagram_post_video_with_ytdlp(
                    build_instagram_post_url(shortcode, mode="reel"),
                    output_root,
                    folder_template,
                    filename_template,
                    post_context,
                    preferred_id=shortcode,
                )
            )
            archive_context = post_context

        elif target.get("mode") == "stories":
            downloaded.extend(
                download_instagram_story_video_with_ytdlp(
                    source_url,
                    output_root,
                    folder_template,
                    filename_template,
                    target=target,
                )
            )
            archive_context = build_instagram_story_url_context(target, fallback_title="story")
            archive_context = enrich_instagram_context_from_ytdlp_info(
                archive_context,
                try_extract_ytdlp_info(source_url, cookies_file=prepare_runtime_cookies(source_url)),
                fallback_title="story",
            )

        elif target.get("mode") == "highlight":
            downloaded.extend(
                download_instagram_highlight_with_ytdlp(
                    source_url,
                    output_root,
                    folder_template,
                    filename_template,
                    target=target,
                )
            )
            archive_context = build_instagram_highlight_url_context(
                target,
                source_url,
                fallback_title="highlight",
            )
            archive_context = enrich_instagram_context_from_ytdlp_info(
                archive_context,
                try_extract_ytdlp_info(source_url, cookies_file=prepare_runtime_cookies(source_url)),
                fallback_title="highlight",
            )

        else:
            ensure_instagram_login(loader, require_login=bool(auth_status.get("configured")))
            username = target.get("username", "")
            profile = instaloader.Profile.from_username(loader.context, username)
            seen_shortcodes: set[str] = set()

            if target.get("mode") == "profile":
                downloaded.extend(
                    download_instagram_profile_pic_to_output(
                        loader,
                        staging_root,
                        output_root,
                        folder_template,
                        filename_template,
                        profile,
                    )
                )
                collections = [
                    ("posts", profile.get_posts()),
                    ("reels", profile.get_reels()),
                    ("igtv", profile.get_igtv_posts()),
                ]
            elif target.get("mode") == "tagged":
                collections = [("tagged", profile.get_tagged_posts())]
            elif target.get("mode") == "profile_reels":
                collections = [("reels", profile.get_reels())]
            elif target.get("mode") == "igtv":
                collections = [("igtv", profile.get_igtv_posts())]
            else:
                collections = []

            for label, iterator in collections:
                for post in iterator:
                    shortcode = to_str(getattr(post, "shortcode", ""))
                    if shortcode and shortcode in seen_shortcodes:
                        continue
                    if shortcode:
                        seen_shortcodes.add(shortcode)
                    if label == "reels":
                        downloaded.extend(
                            download_instagram_post_video_with_ytdlp(
                                build_instagram_post_url(shortcode, mode="reel"),
                                output_root,
                                folder_template,
                                filename_template,
                                build_instagram_post_context(post, creator_hint=username),
                                preferred_id=shortcode,
                            )
                        )
                    else:
                        downloaded.extend(
                            download_instagram_post_to_output(
                                loader,
                                staging_root,
                                output_root,
                                folder_template,
                                filename_template,
                                post,
                                creator_hint=username,
                            )
                        )

            archive_context = {
                "title": "instagram",
                "id": target.get("username", "instagram"),
                "video_id": target.get("username", "instagram"),
                "author": target.get("username", ""),
                "author_nickname": target.get("username", ""),
                "creator": target.get("username", ""),
                "publish_time": None,
            }

        if not downloaded:
            raise RuntimeError("This Instagram URL did not produce downloadable media.")

        if len(downloaded) == 1:
            return downloaded[0], temp_dir

        archive_name = build_instagram_archive_name(
            filename_template,
            archive_context,
            total=len(downloaded),
        )
        archive_path = create_zip_from_paths(downloaded, temp_root / archive_name)
        return archive_path, temp_dir

    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        try:
            loader.close()
        except Exception:
            pass
