import { state } from "./state.js";
import { toast } from "./utils.js";
import { loadTasks, downloadTaskFile } from "./api.js";
import { getSavedSettings, persistSettings } from "./settings.js";
import { renderTasks, updateCountsForMenu } from "./render.js";

export async function addTask(event) {
  event.preventDefault();
  const urlInput = document.getElementById("urlInput");
  const submitBtn = document.getElementById("submitButton");
  const url = urlInput.value.trim();
  if (!url) {
    toast("Paste a supported URL first.", "error");
    return;
  }
  if (submitBtn) submitBtn.disabled = true;
  const settings = getSavedSettings();
  try {
    const response = await fetch("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        url,
        site_locations: settings.site_locations,
        save_mode: settings.save_mode,
        client_tab_id: state.tabId,
      }),
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Failed to add task.");
    urlInput.value = "";
    const firstTask = Array.isArray(data.created) ? data.created[0] : null;
    if (firstTask && data.reused && settings.save_mode === "device" && firstTask.status === "completed") {
      await downloadTaskFile(firstTask.vid, { skipReload: true });
      toast("That file was already downloaded.");
    } else if (data.reused) {
      toast(
        firstTask && firstTask.status === "completed"
          ? "That file was already downloaded."
          : "That download is already in your list."
      );
    } else {
      toast(settings.save_mode === "device" ? "Device download queued." : "Download added.");
    }
    await loadTasks(true);
  } catch (error) {
    toast(error.message || "Failed to add task.", "error");
  } finally {
    if (submitBtn) submitBtn.disabled = false;
  }
}

export async function removeTask(vid) {
  try {
    const response = await fetch(`/api/tasks/${encodeURIComponent(vid)}`, { method: "DELETE" });
    if (response.status === 204) {
      toast("Task removed from the list.");
      await loadTasks(true);
      return;
    }
    const data = await response.json();
    throw new Error(data.error || "Could not remove task.");
  } catch (error) {
    toast(error.message || "Could not remove task.", "error");
  }
}

export async function hideTask(vid) {
  try {
    const response = await fetch(`/api/tasks/${encodeURIComponent(vid)}/hide`, { method: "POST" });
    if (response.status === 204) {
      toast("Task cleared.");
      await loadTasks(true);
      return;
    }
    const data = await response.json();
    throw new Error(data.error || "Could not hide task.");
  } catch (error) {
    toast(error.message || "Could not hide task.", "error");
  }
}

export async function retryTask(vid) {
  try {
    const response = await fetch(`/api/tasks/${encodeURIComponent(vid)}/retry`, { method: "POST" });
    if (response.status === 204) {
      toast("Task queued for retry.");
      await loadTasks(true);
      return;
    }
    const data = await response.json();
    throw new Error(data.error || "Could not retry task.");
  } catch (error) {
    toast(error.message || "Could not retry task.", "error");
  }
}

export async function cancelTask(vid) {
  try {
    const response = await fetch(`/api/tasks/${encodeURIComponent(vid)}/cancel`, { method: "POST" });
    if (response.status === 204) {
      toast("Task cancelled.");
      await loadTasks(true);
      return;
    }
    const data = await response.json();
    throw new Error(data.error || "Could not cancel task.");
  } catch (error) {
    toast(error.message || "Could not cancel task.", "error");
  }
}

export async function clearPending() {
  try {
    const response = await fetch("/api/tasks/clear-pending", { method: "POST" });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Could not clear queue.");
    toast(
      data.cleared === 0
        ? "No queued tasks to clear."
        : `Cleared ${data.cleared} queued task${data.cleared === 1 ? "" : "s"}.`
    );
    await loadTasks(true);
  } catch (error) {
    toast(error.message || "Could not clear queue.", "error");
  }
}

export async function clearCompleted() {
  try {
    const response = await fetch("/api/tasks/clear-completed", { method: "POST" });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Could not clear done.");
    if ((data.cleared || 0) === 0 && (data.skipped || 0) === 0) {
      toast("No done tasks to clear.");
    } else if ((data.skipped || 0) > 0) {
      toast(
        `Cleared ${data.cleared || 0} done task${(data.cleared || 0) === 1 ? "" : "s"}. ` +
        `${data.skipped} device download${data.skipped === 1 ? " is" : "s are"} still waiting to finish delivery.`
      );
    } else {
      toast(`Cleared ${data.cleared} done task${data.cleared === 1 ? "" : "s"}.`);
    }
    await loadTasks(true);
  } catch (error) {
    toast(error.message || "Could not clear done.", "error");
  }
}

export async function cleanNfo() {
  const btn = document.getElementById("cleanNfoButton");
  const originalText = btn.textContent;
  btn.textContent = "Deleting…";
  btn.disabled = true;
  try {
    const response = await fetch("/api/cleanup-nfo", { method: "POST" });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Could not delete .nfo files.");
    if (data.errors && data.errors.length) {
      toast(`Deleted ${data.deleted} .nfo file${data.deleted === 1 ? "" : "s"}, ${data.errors.length} could not be removed.`, "error");
    } else {
      toast(data.deleted === 0 ? "No .nfo files found." : `Deleted ${data.deleted} .nfo file${data.deleted === 1 ? "" : "s"}.`);
    }
  } catch (error) {
    toast(error.message || "Could not delete .nfo files.", "error");
  } finally {
    btn.textContent = originalText;
    btn.disabled = false;
  }
}

// Expose to inline onclick handlers
window.downloadTaskFile = downloadTaskFile;
window.removeTask = removeTask;
window.hideTask = hideTask;
window.retryTask = retryTask;
window.cancelTask = cancelTask;
