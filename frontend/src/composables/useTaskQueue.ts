import { computed, onBeforeUnmount, ref, watch, type Ref } from "vue";
import { useMutation, useQuery } from "@tanstack/vue-query";
import { useEventListener, useIntervalFn } from "@vueuse/core";

import {
  addTask as createTask,
  clearPendingTasks,
  fetchTaskFile,
  getTasks,
  removeTask as removeTaskRequest,
} from "../api";
import { POLL_PENDING_MS, POLL_RUNNING_MS, TASKS_QUERY_KEY } from "../ui";
import type { SavedSettings, TaskItem, TasksResponse, ToastType } from "../types";
import { countTasks, errorMessage, filenameFromContentDisposition } from "../utils/dashboard";

interface UseTaskQueueOptions {
  getSavedSettings: () => SavedSettings;
  toast: (message: string, type?: ToastType) => void;
  url: Ref<string>;
}

export function useTaskQueue({ getSavedSettings, toast, url }: UseTaskQueueOptions) {
  const taskCache = new Map<string, Partial<TaskItem>>();
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
        source_key: task.source_key || cached.source_key || "others",
      };
      taskCache.set(task.vid, {
        resolved_folder: merged.resolved_folder,
        resolved_filename: merged.resolved_filename,
        resolved_full_path: merged.resolved_full_path,
        source_key: merged.source_key,
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
      });
      url.value = "";
      const firstTask = Array.isArray(data.created) ? data.created[0] : null;
      if (data.reused) {
        toast(firstTask && firstTask.status === "completed" ? "That file was already downloaded." : "That download is already in your list.");
      } else {
        toast("Download added.");
      }
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Failed to add task."), "error");
    }
  }

  async function downloadTaskFile(taskId: string): Promise<void> {
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
    await loadTasks(true);
  }

  async function downloadTask(taskId: string): Promise<void> {
    try {
      await downloadTaskFile(taskId);
    } catch (error) {
      toast(errorMessage(error, "Could not download that file."), "error");
    }
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
    (tasks) => syncPoll(tasks),
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
