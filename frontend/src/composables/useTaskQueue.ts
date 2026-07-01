import { computed, onBeforeUnmount, reactive, ref, watch, type Ref } from "vue";
import { useMutation, useQuery } from "@tanstack/vue-query";
import { useEventListener, useIntervalFn } from "@vueuse/core";

import {
  addTask as createTask,
  clearPendingTasks,
  fetchTaskFile,
  getTasks,
  removeTask as removeTaskRequest,
} from "../api";
import { POLL_PENDING_MS, POLL_RUNNING_MS, SITE_LABELS, TASKS_QUERY_KEY } from "../ui";
import type { MenuKey, SavedSettings, TaskItem, TasksResponse, ToastType } from "../types";
import { countTasks, errorMessage, filenameFromContentDisposition, getTabId, readJsonRecord } from "../utils/dashboard";

interface UseTaskQueueOptions {
  getSavedSettings: () => SavedSettings;
  toast: (message: string, type?: ToastType) => void;
  url: Ref<string>;
}

export function useTaskQueue({ getSavedSettings, toast, url }: UseTaskQueueOptions) {
  const tabId = getTabId();
  const taskCache = new Map<string, Partial<TaskItem>>();
  const deliveredDeviceDownloads = reactive<Record<string, boolean>>(readJsonRecord("neverstelle.deviceDelivered"));
  const deviceDownloadsInFlight = new Set<string>();
  const pollingIntervalMs = ref(POLL_PENDING_MS);

  const tasksQuery = useQuery<TasksResponse>({
    queryKey: TASKS_QUERY_KEY,
    queryFn: getTasks,
    staleTime: 1000,
  });
  const addTaskMutation = useMutation({ mutationFn: createTask });

  const rawTasks = computed(() => tasksQuery.data.value?.tasks || []);
  const taskItems = computed(() => mergeTaskData(rawTasks.value));
  const tasksLoading = computed(() => tasksQuery.isPending.value);
  const tasksErrorMessage = computed(() => (tasksQuery.error.value ? errorMessage(tasksQuery.error.value, "Could not load tasks.") : ""));

  const { pause: pausePolling, resume: resumePolling, isActive: pollingActive } = useIntervalFn(
    () => void loadTasks(true),
    pollingIntervalMs,
    { immediate: false },
  );

  function mergeTaskData(tasks: TaskItem[]): TaskItem[] {
    return tasks.map((task) => {
      const cached = taskCache.get(task.vid) || {};
      const merged = {
        ...task,
        resolved_folder: task.resolved_folder || cached.resolved_folder || "",
        resolved_filename: task.resolved_filename || cached.resolved_filename || "",
        resolved_full_path: task.resolved_full_path || cached.resolved_full_path || "",
        site_category: task.site_category || cached.site_category || "others",
        site_label: task.site_label || SITE_LABELS[(task.site_category as MenuKey) || "others"] || "Others",
      };
      taskCache.set(task.vid, {
        resolved_folder: merged.resolved_folder,
        resolved_filename: merged.resolved_filename,
        resolved_full_path: merged.resolved_full_path,
        site_category: merged.site_category,
      });
      return merged;
    });
  }

  function syncPoll(tasks: TaskItem[]): void {
    const counts = countTasks(tasks);
    const targetMs = counts.running > 0 ? POLL_RUNNING_MS : counts.queued > 0 ? POLL_PENDING_MS : 0;
    if (!targetMs || document.hidden) {
      pausePolling();
      return;
    }
    const intervalChanged = pollingIntervalMs.value !== targetMs;
    pollingIntervalMs.value = targetMs;
    if (intervalChanged && pollingActive.value) pausePolling();
    if (!pollingActive.value) resumePolling();
  }

  async function loadTasks(silent = false): Promise<void> {
    const result = await tasksQuery.refetch();
    if (result.error && !silent) toast(errorMessage(result.error, "Could not load tasks."), "error");
  }

  async function addDownloadTask(): Promise<void> {
    const sourceUrl = url.value.trim();
    if (!sourceUrl) {
      toast("Paste a supported URL first.", "error");
      return;
    }
    const currentSettings = getSavedSettings();
    try {
      const data = await addTaskMutation.mutateAsync({
        url: sourceUrl,
        site_locations: currentSettings.site_locations,
        save_mode: currentSettings.save_mode,
        client_tab_id: tabId,
      });
      url.value = "";
      const firstTask = Array.isArray(data.created) ? data.created[0] : null;
      if (firstTask && data.reused && currentSettings.save_mode === "device" && firstTask.status === "completed") {
        await downloadTaskFile(firstTask.vid);
        toast("That file was already downloaded.");
      } else if (data.reused) {
        toast(firstTask && firstTask.status === "completed" ? "That file was already downloaded." : "That download is already in your list.");
      } else {
        toast(currentSettings.save_mode === "device" ? "Device download queued." : "Download added.");
      }
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Failed to add task."), "error");
    }
  }

  function deliveredKey(taskId: string): string {
    return `${taskId}:${tabId}`;
  }

  function persistDeliveredDeviceDownloads(): void {
    sessionStorage.setItem("neverstelle.deviceDelivered", JSON.stringify(deliveredDeviceDownloads));
  }

  async function triggerTaskFileDownload(taskId: string): Promise<void> {
    const response = await fetchTaskFile(taskId);
    const blob = await response.blob();
    const objectUrl = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = objectUrl;
    link.rel = "noopener";
    link.download = filenameFromContentDisposition(response.headers.get("content-disposition") || "");
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
  }

  async function downloadTaskFile(taskId: string, options: { skipReload?: boolean } = {}): Promise<void> {
    await triggerTaskFileDownload(taskId);
    // Client-side marker prevents a device-mode file from auto-downloading twice
    // in this browser. Completed downloads are never deleted, so there is no
    // server-side delivery acknowledgement to make.
    deliveredDeviceDownloads[deliveredKey(taskId)] = true;
    persistDeliveredDeviceDownloads();
    if (!options.skipReload) await loadTasks(true);
  }

  async function downloadTask(taskId: string): Promise<void> {
    try {
      await downloadTaskFile(taskId);
    } catch (error) {
      toast(errorMessage(error, "Could not download that file."), "error");
    }
  }

  function processCompletedDeviceDownloads(tasks: TaskItem[]): void {
    tasks.forEach((task) => {
      if (task.save_mode !== "device" || task.status !== "completed" || !task.resolved_full_path) return;
      const allowedTabs = Array.isArray(task.device_request_tabs) ? task.device_request_tabs : [];
      if (!allowedTabs.includes(tabId)) return;
      const key = deliveredKey(task.vid);
      if (deliveredDeviceDownloads[key] || deviceDownloadsInFlight.has(key)) return;
      deviceDownloadsInFlight.add(key);
      downloadTaskFile(task.vid, { skipReload: true })
        .catch((error) => toast(errorMessage(error, "Could not download that file."), "error"))
        .finally(() => {
          deviceDownloadsInFlight.delete(key);
          persistDeliveredDeviceDownloads();
        });
    });
  }

  async function removeTask(taskId: string): Promise<void> {
    try {
      await removeTaskRequest(taskId);
      toast("Task removed from the list.");
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Could not remove task."), "error");
    }
  }

  async function clearPending(): Promise<void> {
    try {
      const data = await clearPendingTasks();
      toast(data.cleared === 0 ? "No queued tasks to clear." : `Cleared ${data.cleared} queued task${data.cleared === 1 ? "" : "s"}.`);
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Could not clear queue."), "error");
    }
  }

  function handleVisibilityChange(): void {
    if (document.hidden) {
      pausePolling();
      return;
    }
    syncPoll(rawTasks.value);
    void loadTasks(true);
  }

  watch(
    rawTasks,
    (tasks) => {
      processCompletedDeviceDownloads(tasks);
      syncPoll(tasks);
    },
    { immediate: true },
  );

  useEventListener(document, "visibilitychange", handleVisibilityChange);
  onBeforeUnmount(() => pausePolling());

  return {
    addDownloadTask,
    clearPending,
    downloadTask,
    removeTask,
    taskItems,
    tasksErrorMessage,
    tasksLoading,
  };
}
