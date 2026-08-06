import { computed, onBeforeUnmount, ref, watch, type Ref } from "vue";
import { useMutation, useQuery } from "@tanstack/vue-query";
import { useEventListener, useIntervalFn } from "@vueuse/core";

import {
  addTask as createTask,
  cancelTask as cancelTaskRequest,
  clearPendingTasks,
  getTasks,
  probeUrl,
  removeTask as removeTaskRequest,
  retryTask as retryTaskRequest,
  scanMediaLibrary,
  setTaskSource as setTaskSourceRequest,
} from "@/api";
import { useAuth } from "@/composables/useAuth";
import {
  POLL_PENDING_MS,
  POLL_RUNNING_MS,
  QUEUE_FAILED_MESSAGE,
  REUSED_TASK_FALLBACK,
  REUSED_TASK_MESSAGES,
  TASKS_QUERY_KEY,
} from "@/ui";
import type { PlaylistEntry, QualitySelection, SavedSettings, TaskItem, TaskStatus, TasksResponse, ToastType } from "@/types";
import { countTasks, errorMessage, extractUrl, normalizeSourceKey } from "@/utils/dashboard";

interface UseTaskQueueOptions {
  getSavedSettings: () => SavedSettings;
  getQuality: () => QualitySelection;
  toast: (message: string, type?: ToastType) => void;
  url: Ref<string>;
}

// Playlists carry a `list` param; sets/albums live under known list routes.
function looksLikePlaylist(sourceUrl: string): boolean {
  try {
    const parsed = new URL(sourceUrl);
    if (parsed.searchParams.has("list")) return true;
    return /\/(playlist|playlists|sets|album|albums)(\/|$)/i.test(parsed.pathname);
  } catch {
    return false;
  }
}

function reusedMessage(status?: TaskStatus): string {
  return REUSED_TASK_MESSAGES[status || ""] || REUSED_TASK_FALLBACK;
}

export function useTaskQueue({ getSavedSettings, getQuality, toast, url }: UseTaskQueueOptions) {
  const auth = useAuth();
  const taskCache = new Map<string, Partial<TaskItem>>();
  const pollingIntervalMs = ref(POLL_PENDING_MS);

  const playlistOpen = ref(false);
  const playlistTitle = ref("");
  const playlistEntries = ref<PlaylistEntry[]>([]);

  const tasksQuery = useQuery<TasksResponse>({
    queryKey: TASKS_QUERY_KEY,
    queryFn: getTasks,
    enabled: auth.authenticated,
    staleTime: 1000,
  });
  const addTaskMutation = useMutation({ mutationFn: createTask });
  const probeMutation = useMutation({ mutationFn: probeUrl });
  const scanMediaMutation = useMutation({ mutationFn: scanMediaLibrary });
  const setSourceMutation = useMutation({
    mutationFn: (payload: { taskId: string; sourceKey: string }) =>
      setTaskSourceRequest(payload.taskId, payload.sourceKey),
  });

  const rawTasks = computed(() => tasksQuery.data.value?.tasks || []);
  const taskItems = computed(() => mergeTaskData(rawTasks.value));
  const countsByMenu = computed(() => tasksQuery.data.value?.counts_by_menu || {});
  const countsByMediaMenu = computed(() => tasksQuery.data.value?.counts_by_media_menu || {});
  const tasksLoading = computed(() => tasksQuery.isPending.value);
  const tasksErrorMessage = computed(() => (tasksQuery.error.value ? errorMessage(tasksQuery.error.value, "Could not load tasks.") : ""));
  const historyRefreshing = computed(() => scanMediaMutation.isPending.value);

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
        source_key: normalizeSourceKey(task.source_key || cached.source_key || ""),
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

  async function queueUrls(urls: string[]): Promise<void> {
    const currentSettings = getSavedSettings();
    const data = await addTaskMutation.mutateAsync({
      urls,
      site_locations: currentSettings.site_locations,
      template_settings: currentSettings.template_settings,
      source_profiles: currentSettings.source_profiles,
      source_templates: currentSettings.source_templates,
      quality: getQuality(),
    });
    const created = Array.isArray(data.created) ? data.created : [];
    if (urls.length > 1) {
      toast(created.length ? `Added ${created.length} download${created.length === 1 ? "" : "s"}.` : REUSED_TASK_FALLBACK);
    } else if (data.reused) {
      toast(reusedMessage(created[0]?.status));
    } else if (created[0]?.status === "failed") {
      toast(created[0].error || QUEUE_FAILED_MESSAGE, "error");
    } else {
      toast("Download added.");
    }
    await loadTasks(true);
  }

  async function addDownloadTask(): Promise<void> {
    const sourceUrl = extractUrl(url.value);
    if (!sourceUrl) {
      toast("Paste a supported URL first.", "error");
      return;
    }
    try {
      // Probe only playlist-shaped links: keeps single videos on the fast path
      // and lets the backend split playlists from endless radios/mixes.
      if (looksLikePlaylist(sourceUrl)) {
        const probe = await probeMutation.mutateAsync(sourceUrl);
        if (probe.kind === "playlist" && probe.entries.length > 0) {
          playlistTitle.value = probe.title || "Playlist";
          playlistEntries.value = probe.entries;
          playlistOpen.value = true;
          return;
        }
        await queueUrls([probe.url || sourceUrl]);
        url.value = "";
        return;
      }
      await queueUrls([sourceUrl]);
      url.value = "";
    } catch (error) {
      toast(errorMessage(error, "Failed to add task."), "error");
    }
  }

  async function confirmPlaylistSelection(urls: string[]): Promise<void> {
    playlistOpen.value = false;
    if (urls.length === 0) return;
    try {
      await queueUrls(urls);
      url.value = "";
    } catch (error) {
      toast(errorMessage(error, "Failed to add tasks."), "error");
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

  async function cancelTask(taskId: string): Promise<void> {
    try {
      await cancelTaskRequest(taskId);
      toast("Download cancelled.");
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Could not cancel download."), "error");
    }
  }

  async function retryTask(taskId: string): Promise<void> {
    try {
      await retryTaskRequest(taskId);
      toast("Retrying download.");
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Could not retry download."), "error");
    }
  }

  async function setTaskSource(payload: { taskId: string; sourceKey: string }): Promise<void> {
    const sourceKey = payload.sourceKey.trim();
    if (!sourceKey) return;
    try {
      await setSourceMutation.mutateAsync({ taskId: payload.taskId, sourceKey });
      toast("Source updated.");
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Could not set source."), "error");
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

  async function refreshHistory(): Promise<void> {
    try {
      const result = await scanMediaMutation.mutateAsync();
      await loadTasks(true);
      const plural = (count: number) => (count === 1 ? "" : "s");
      const parts: string[] = [];
      if (result.added > 0) parts.push(`added ${result.added} file${plural(result.added)}`);
      if (result.missing > 0) parts.push(`removed ${result.missing} missing item${plural(result.missing)}`);
      if (result.renamed > 0) parts.push(`renamed ${result.renamed} file${plural(result.renamed)}`);
      if (result.rename_failed > 0) parts.push(`could not rename ${result.rename_failed}`);
      if (parts.length === 0) {
        toast(`History refreshed. Checked ${result.checked} file${plural(result.checked)}.`);
        return;
      }
      const summary = parts.join(", ").replace(/, ([^,]*)$/, " and $1");
      toast(`History refreshed. ${summary.charAt(0).toUpperCase()}${summary.slice(1)}.`);
    } catch (error) {
      toast(errorMessage(error, "Could not refresh history."), "error");
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
    cancelTask,
    clearPending,
    confirmPlaylistSelection,
    historyRefreshing,
    retryTask,
    playlistEntries,
    playlistOpen,
    playlistTitle,
    refreshHistory,
    removeTask,
    setTaskSource,
    taskItems,
    countsByMenu,
    countsByMediaMenu,
    tasksErrorMessage,
    tasksLoading,
  };
}
