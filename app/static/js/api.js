import { POLL_RUNNING_MS, POLL_PENDING_MS } from "./config.js";
import { state } from "./state.js";
import { toast } from "./utils.js";
import { renderTasks, updateCountsForMenu, countTasks } from "./render.js";
import { persistDeliveredDeviceDownloads } from "./settings.js";

export function syncPoll(counts) {
  const running = (counts.running ?? 0) > 0;
  const pending = (counts.queued ?? 0) > 0;
  const targetMs = running ? POLL_RUNNING_MS : pending ? POLL_PENDING_MS : 0;

  if (targetMs !== state.pollIntervalMs) {
    if (state.pollHandle) {
      clearInterval(state.pollHandle);
      state.pollHandle = null;
    }
    state.pollIntervalMs = targetMs;
  }

  if (targetMs > 0 && !state.pollHandle && !document.hidden) {
    state.pollHandle = setInterval(() => loadTasks(true), targetMs);
  }
}

export async function loadTasks(silent = false) {
  try {
    const response = await fetch("/api/tasks");
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Could not load tasks.");
    state.visibleTasks = data.tasks || [];
    processCompletedDeviceDownloads(state.visibleTasks);
    renderTasks(state.visibleTasks);
    updateCountsForMenu(state.visibleTasks);
    syncPoll(countTasks(state.visibleTasks));
  } catch (error) {
    if (!silent) toast(error.message || "Could not load tasks.", "error");
  }
}

export async function triggerTaskFileDownload(vid) {
  const response = await fetch(`/api/tasks/${encodeURIComponent(vid)}/file`, {
    credentials: "same-origin",
  });
  const contentType = (response.headers.get("content-type") || "").toLowerCase();
  if (!response.ok) {
    let message = "Could not download that file.";
    if (contentType.includes("application/json")) {
      try {
        const payload = await response.json();
        message = payload.error || message;
      } catch {}
    } else {
      try {
        const text = (await response.text()).trim();
        if (text) message = text;
      } catch {}
    }
    throw new Error(message);
  }
  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);
  const contentDisposition = response.headers.get("content-disposition") || "";
  const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
  const asciiMatch = contentDisposition.match(/filename=\"?([^\";]+)\"?/i);
  const filename = utf8Match
    ? decodeURIComponent(utf8Match[1])
    : asciiMatch
    ? asciiMatch[1]
    : "download";
  const link = document.createElement("a");
  link.href = objectUrl;
  link.rel = "noopener";
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
}

export function deliveredKey(vid) {
  return `${vid}:${state.tabId}`;
}

export async function acknowledgeDeliveredTask(vid) {
  try {
    await fetch(`/api/tasks/${encodeURIComponent(vid)}/delivered`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ client_tab_id: state.tabId }),
    });
  } catch {}
}

export async function downloadTaskFile(vid, options = {}) {
  const skipReload = options && options.skipReload === true;
  state.deliveredDeviceDownloads[deliveredKey(vid)] = true;
  persistDeliveredDeviceDownloads();
  try {
    await triggerTaskFileDownload(vid);
    await acknowledgeDeliveredTask(vid);
  } catch (err) {
    delete state.deliveredDeviceDownloads[deliveredKey(vid)];
    persistDeliveredDeviceDownloads();
    throw err;
  }
  if (!skipReload) await loadTasks(true);
}

export function processCompletedDeviceDownloads(tasks) {
  (tasks || []).forEach((task) => {
    if (task.save_mode !== "device" || task.status !== "completed" || !task.can_download) return;
    const allowedTabs = Array.isArray(task.device_request_tabs) ? task.device_request_tabs : [];
    if (!allowedTabs.includes(state.tabId)) return;
    const key = deliveredKey(task.vid);
    if (state.deliveredDeviceDownloads[key]) return;
    if (document.hidden && typeof Notification !== "undefined" && Notification.permission === "granted") {
      const label = task.resolved_filename || task.source_url || "Download ready";
      new Notification("never-stelle", { body: label, icon: "/static/favicon.png" });
    }
    downloadTaskFile(task.vid).catch((error) => {
      toast(error.message || "Could not download that file.", "error");
    });
  });
  persistDeliveredDeviceDownloads();
}
