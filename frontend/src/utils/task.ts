import type { SourceProfile, TaskItem } from "../types";
import { faviconUrlForHost, hostFromUrl, sourceLabelFromKey } from "./dashboard";

// Clamp raw progress to 0-100.
function progressPct(task: TaskItem): number {
  return Math.max(0, Math.min(100, Number(task.progress_pct) || 0));
}

// Resolve the profile matching the task's source key.
function sourceProfileFor(task: TaskItem, profiles: SourceProfile[] = []): SourceProfile | undefined {
  const key = task.source_key || "others";
  return profiles.find((profile) => profile.key === key);
}

// Human label for the task's source.
function sourceLabel(task: TaskItem, profiles: SourceProfile[] = []): string {
  return String(sourceProfileFor(task, profiles)?.label || sourceLabelFromKey(task.source_key || "others")).trim();
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

// Row/card background: red tint on failure, accent progress bar otherwise.
export function taskBackgroundStyle(task: TaskItem) {
  if (task.status === "failed") {
    return { background: "color-mix(in srgb, var(--color-red-500) 20%, transparent)" };
  }
  const pct = progressPct(task);
  if (task.status === "running" || (pct > 0 && pct < 100)) {
    return {
      background: `linear-gradient(to right, color-mix(in srgb, var(--accent) 20%, transparent) ${pct}%, transparent ${pct}%)`,
    };
  }
  return {};
}
