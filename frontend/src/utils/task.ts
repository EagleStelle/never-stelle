import type { SourceProfile, TaskItem } from "@/types";
import { faviconUrlForHost, hostFromUrl, sourceLabelFromKey } from "@/utils/dashboard";

const IMAGE_EXTENSIONS = new Set([
  "jpg",
  "jpeg",
  "png",
  "webp",
  "gif",
  "bmp",
  "heic",
  "heif",
  "avif",
  "jfif",
]);

// Classify a task as image or video: by real file extension, else by the
// download engine (gallery-dl = images) for items not yet on disk.
export function mediaKindForTask(task: TaskItem): "image" | "video" {
  const name = String(task.resolved_filename || task.resolved_full_path || "");
  const dot = name.lastIndexOf(".");
  const ext = dot >= 0 ? name.slice(dot + 1).toLowerCase() : "";
  if (/^[a-z0-9]{1,5}$/.test(ext)) return IMAGE_EXTENSIONS.has(ext) ? "image" : "video";
  return task.task_type === "gallerydl" ? "image" : "video";
}

function clampProgress(value: number): number {
  return Math.max(0, Math.min(100, value));
}

// Clamp raw progress to 0-100, accepting either percent or 0-1 fraction payloads.
function progressPct(task: TaskItem): number {
  const pct = Number(task.progress_pct);
  if (Number.isFinite(pct) && pct > 0) return clampProgress(pct);

  const progress = Number(task.progress);
  if (Number.isFinite(progress) && progress > 0) {
    return clampProgress(progress <= 1 ? progress * 100 : progress);
  }

  return Number.isFinite(pct) ? clampProgress(pct) : 0;
}

// Resolve the profile matching the task's source key.
function sourceProfileFor(task: TaskItem, profiles: SourceProfile[] = []): SourceProfile | undefined {
  const key = task.source_key || "";
  return profiles.find((profile) => profile.key === key);
}

// Human label for the task's source.
function sourceLabel(task: TaskItem, profiles: SourceProfile[] = []): string {
  return String(sourceProfileFor(task, profiles)?.label || sourceLabelFromKey(task.source_key || "")).trim();
}

// Only linkify http(s) source urls.
export function sourceLink(task: TaskItem): string {
  const url = String(task.source_url || "").trim();
  return /^https?:\/\//i.test(url) ? url : "";
}

// Profile icon, else favicon of the source host.
export function sourceIconUrl(task: TaskItem, profiles: SourceProfile[] = []): string {
  const profile = sourceProfileFor(task, profiles);
  if (profile?.icon_url) return profile.icon_url;
  return faviconUrlForHost(hostFromUrl(String(task.source_url || ""))).trim();
}

// Display title: filename, else "<site> <status>".
export function taskTitle(task: TaskItem, profiles: SourceProfile[] = []): string {
  const filename = String(task.resolved_filename || "").trim();
  if (filename) return filename;
  const siteLabel = sourceLabel(task, profiles);
  const statusLabel = String(task.status_label || "Download").trim().toLowerCase();
  return siteLabel ? `${siteLabel} ${statusLabel}` : "Download";
}

// Secondary line under the title.
export function taskDetail(task: TaskItem): string {
  return String(task.source_url || task.vid || "").trim();
}

// A spent row is skipped by "Resolve Missing", so its own button is the only way back.
export function resolveHint(task: TaskItem): string {
  return task.resolve_failed
    ? "Source did not supply the missing details. Try again"
    : "Fetch info from source";
}

// Human-readable byte size, empty when unknown.
export function formatSize(bytes: number): string {
  const value = Number(bytes) || 0;
  if (value <= 0) return "";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size >= 10 || unit === 0 ? Math.round(size) : size.toFixed(1)} ${units[unit]}`;
}

export function taskProgressState(task: TaskItem): "failed" | "progress" | undefined {
  if (task.status === "failed") return "failed";
  const pct = progressPct(task);
  if (pct > 0 && (task.status === "running" || pct < 100)) return "progress";
  return undefined;
}

// Row/card surface fill amount. CSS owns the paint so rows and cards stay consistent.
export function taskProgressStyle(task: TaskItem): Record<string, string> {
  if (taskProgressState(task) !== "progress") return {};
  const pct = Number(progressPct(task).toFixed(2));
  return { "--task-progress": `${pct}%` };
}
