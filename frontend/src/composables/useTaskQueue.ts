import { computed, onBeforeUnmount, ref, watch, type Ref } from "vue";
import { useMutation, useQuery } from "@tanstack/vue-query";
import { useEventListener, useIntervalFn, useSessionStorage } from "@vueuse/core";

import {
  addTask as createTask,
  cancelTask as cancelTaskRequest,
  clearPendingTasks,
  getRenameCounts,
  getResolveScope,
  getTasks,
  probeUrl,
  removeTask as removeTaskRequest,
  renameHistory as renameHistoryRequest,
  resolveHistory as resolveHistoryRequest,
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
import type {
  NamingKind,
  PlaylistEntry,
  QualitySelection,
  PostProcessingSelection,
  RenameCounts,
  ResolvePassReport,
  ResolveScope,
  SavedSettings,
  TaskItem,
  TaskStatus,
  TasksResponse,
  ToastType,
} from "@/types";
import {
  countTasks,
  errorMessage,
  extractUrl,
  normalizeSourceKey,
} from "@/utils/dashboard";

interface UseTaskQueueOptions {
  getSavedSettings: () => SavedSettings;
  getQuality: () => QualitySelection;
  getPostProcessing: () => PostProcessingSelection;
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

// "1 item needs" vs "2 items need": the noun and the verb take the s in opposite cases.
const plural = (count: number) => (count === 1 ? "" : "s");
const pluralVerb = (count: number) => (count === 1 ? "s" : "");

// "Added 2 files and removed 3 missing files."
function sentence(parts: string[]): string {
  const summary = parts.join(", ").replace(/, ([^,]*)$/, " and $1");
  return `${summary.charAt(0).toUpperCase()}${summary.slice(1)}.`;
}

function resolvedMessage(report: ResolvePassReport): string {
  const parts: string[] = [];
  if (report.resolved > 0) parts.push(`resolved ${report.resolved} item${plural(report.resolved)}`);
  if (report.skipped > 0) parts.push(`skipped ${report.skipped} item${plural(report.skipped)}`);
  if (report.failed > 0) parts.push(`could not resolve ${report.failed} item${plural(report.failed)}`);
  return parts.length === 0 ? "Nothing changed." : sentence(parts);
}

// One source's unresolved change, as the Resolve Platform dialog confirms it. A template
// change is resolved per format, "" being links no format matches.
interface RenameTarget {
  key: string;
  label: string;
  kind: NamingKind;
  format: string;
}

export function useTaskQueue({
  getSavedSettings,
  getQuality,
  getPostProcessing,
  toast,
  url,
}: UseTaskQueueOptions) {
  const auth = useAuth();
  let taskCache = new Map<string, Partial<TaskItem>>();
  const pollingIntervalMs = ref(POLL_PENDING_MS);

  const playlistOpen = ref(false);
  const playlistTitle = ref("");
  const playlistEntries = ref<PlaylistEntry[]>([]);

  const resolveOpen = ref(false);
  const resolveFlagged = ref(0);
  const resolveTotal = ref(0);
  // Kept per tab, so a reload still reports the passes this tab started.
  const pendingResolvePasses = useSessionStorage<number[]>("neverstelle.resolvePasses", []);
  const renameCounts = ref<RenameCounts>({});
  const renameTarget = ref<RenameTarget | null>(null);
  // Covers the gap between the click and the first poll that sees the queued jobs.
  const renameStarting = ref<RenameTarget | null>(null);

  const tasksQuery = useQuery<TasksResponse>({
    queryKey: TASKS_QUERY_KEY,
    // Forwarding the signal aborts a poll the next one supersedes.
    queryFn: ({ signal }) => getTasks(signal),
    enabled: auth.authenticated,
    staleTime: 1000,
  });
  const addTaskMutation = useMutation({ mutationFn: createTask });
  const probeMutation = useMutation({ mutationFn: probeUrl });
  const scanMediaMutation = useMutation({ mutationFn: scanMediaLibrary });
  const resolveScopeMutation = useMutation({ mutationFn: getResolveScope });
  const resolveMutation = useMutation({ mutationFn: resolveHistoryRequest });
  const renameMutation = useMutation({ mutationFn: renameHistoryRequest });
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
  // Server-owned, so a reload rejoins a pass it did not start and the spinner stops when
  // the work is done rather than when this tab's request returned.
  const historyRefreshing = computed<boolean>(
    () => scanMediaMutation.isPending.value || Boolean(tasksQuery.data.value?.scanning),
  );
  const historyResolving = computed<boolean>(
    () =>
      resolveScopeMutation.isPending.value ||
      resolveMutation.isPending.value ||
      renameMutation.isPending.value ||
      Number(tasksQuery.data.value?.resolving || 0) > 0,
  );
  const libraryBusy = computed<boolean>(() => historyRefreshing.value || historyResolving.value);

  const { pause: pausePolling, resume: resumePolling, isActive: pollingActive } = useIntervalFn(
    () => void loadTasks(true),
    pollingIntervalMs,
    { immediate: false },
  );

  // Cache is rebuilt per payload: the feed is active rows only, so anything missing is done.
  function mergeTaskData(tasks: TaskItem[]): TaskItem[] {
    const nextCache = new Map<string, Partial<TaskItem>>();
    const merged = tasks.map((task) => {
      const cached = taskCache.get(task.vid) || {};
      const carried: Partial<TaskItem> = {
        resolved_folder: task.resolved_folder || cached.resolved_folder || "",
        resolved_filename: task.resolved_filename || cached.resolved_filename || "",
        resolved_full_path: task.resolved_full_path || cached.resolved_full_path || "",
        source_key: normalizeSourceKey(task.source_key || cached.source_key || ""),
      };
      nextCache.set(task.vid, carried);
      return { ...task, ...carried };
    });
    taskCache = nextCache;
    return merged;
  }

  function syncPoll(tasks: TaskItem[]): void {
    const counts = countTasks(tasks);
    // A scan or resolve keeps the poll awake on its own: both run with no task rows,
    // and stopping would strand the spinner until something else woke it.
    const targetMs =
      counts.running > 0 || libraryBusy.value
        ? POLL_RUNNING_MS
        : counts.queued > 0
          ? POLL_PENDING_MS
          : 0;
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
      source_locations: currentSettings.source_locations,
      template_settings: currentSettings.template_settings,
      source_profiles: currentSettings.source_profiles,
      source_templates: currentSettings.source_templates,
      quality: getQuality(),
      post_processing: getPostProcessing(),
    });
    const created = Array.isArray(data.created) ? data.created : [];
    if (urls.length > 1) {
      toast(created.length ? `Added ${created.length} download${plural(created.length)}.` : REUSED_TASK_FALLBACK);
    } else if (data.reused) {
      toast(reusedMessage(created[0]?.status));
    } else if (created[0]?.status === "failed") {
      toast(created[0].error || QUEUE_FAILED_MESSAGE, "error");
    } else {
      toast("Download added.");
    }
    await loadTasks(true);
  }

  // Returns as soon as the URL is accepted; the round trip runs detached so the field is
  // free for the next paste immediately.
  function addDownloadTask(): void {
    const sourceUrl = extractUrl(url.value);
    if (!sourceUrl) {
      toast("Enter a valid URL.", "error");
      return;
    }
    url.value = "";
    void submitDownload(sourceUrl);
  }

  async function submitDownload(sourceUrl: string): Promise<void> {
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
        return;
      }
      await queueUrls([sourceUrl]);
    } catch (error) {
      toast(errorMessage(error, "Failed to add task."), "error");
    }
  }

  async function confirmPlaylistSelection(urls: string[]): Promise<void> {
    playlistOpen.value = false;
    if (urls.length === 0) return;
    try {
      await queueUrls(urls);
    } catch (error) {
      toast(errorMessage(error, "Failed to add tasks."), "error");
    }
  }

  async function removeTask(taskId: string): Promise<void> {
    try {
      await removeTaskRequest(taskId);
      toast("Task removed.");
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
      toast(data.cleared === 0 ? "No queued tasks." : `Cleared ${data.cleared} queued task${plural(data.cleared)}.`);
      await loadTasks(true);
    } catch (error) {
      toast(errorMessage(error, "Could not clear queue."), "error");
    }
  }

  async function refreshHistory(): Promise<void> {
    try {
      const result = await scanMediaMutation.mutateAsync();
      await loadTasks(true);
      const parts: string[] = [];
      if (result.added > 0) parts.push(`added ${result.added} file${plural(result.added)}`);
      if (result.missing > 0) parts.push(`removed ${result.missing} missing file${plural(result.missing)}`);

      const pending =
        result.needs_resolve > 0
          ? `${result.needs_resolve} item${plural(result.needs_resolve)} need${pluralVerb(result.needs_resolve)} resolution.`
          : "";

      if (parts.length === 0) {
        toast(pending ? `History up to date. ${pending}` : "History up to date.");
        return;
      }

      const summary = sentence(parts);
      toast(pending ? `${summary} ${pending}` : summary);
    } catch (error) {
      toast(errorMessage(error, "Could not refresh history."), "error");
    }
  }

  async function openResolveDialog(): Promise<void> {
    try {
      const scope = await resolveScopeMutation.mutateAsync();
      resolveFlagged.value = scope.flagged;
      resolveTotal.value = scope.total;
      resolveOpen.value = true;
    } catch (error) {
      toast(errorMessage(error, "Could not read resolve scope."), "error");
    }
  }

  // Each click waits on its own pass, so a click landing mid-pass is never told the
  // running total of everything queued before it.
  function reportSettledResolves(): void {
    if (pendingResolvePasses.value.length === 0) return;
    const reports = tasksQuery.data.value?.resolve_passes || {};
    const idle = !historyResolving.value;
    const waiting: number[] = [];
    let settled = false;
    for (const passId of pendingResolvePasses.value) {
      const report = reports[String(passId)];
      // Gone with the process that ran it, so there is nothing left to report.
      if (!report) {
        if (!idle) waiting.push(passId);
        continue;
      }
      // Idle with rows still unaccounted for means their jobs left with a deleted row.
      if (report.resolved + report.skipped + report.failed < report.queued && !idle) {
        waiting.push(passId);
        continue;
      }
      toast(resolvedMessage(report));
      settled = true;
    }
    pendingResolvePasses.value = waiting;
    // Files that moved no longer count against their format.
    if (settled) void loadRenameCounts();
  }

  async function trackResolvePass(passId: number): Promise<void> {
    // The POST only queues, so the poll has to be started here or the spinner would
    // not appear until whatever tick happened to come next.
    await loadTasks(true);
    // Joined only once a poll can see it: an older payload would read as a pass the
    // server never heard of and be dropped unreported.
    pendingResolvePasses.value = [...pendingResolvePasses.value, passId];
    reportSettledResolves();
  }

  async function startResolve(payload: { scope?: ResolveScope; task_ids?: string[] }): Promise<void> {
    try {
      const result = await resolveMutation.mutateAsync(payload);
      if (result.queued === 0) {
        toast("Nothing to resolve.");
        return;
      }
      await trackResolvePass(result.pass_id);
    } catch (error) {
      toast(errorMessage(error, "Could not resolve history."), "error");
    }
  }

  // Per platform, the files its current templates or field order would file differently.
  async function loadRenameCounts(): Promise<void> {
    try {
      renameCounts.value = await getRenameCounts();
    } catch (error) {
      toast(errorMessage(error, "Could not read naming changes."), "error");
    }
  }

  function namingCount(counts: RenameCounts | undefined, key: string, kind: NamingKind, format: string): number {
    const entry = counts?.[key];
    return (kind === "templates" ? entry?.templates[format] : entry?.fields) || 0;
  }

  function renameCount(key: string, kind: NamingKind, format = ""): number {
    return namingCount(renameCounts.value, key, kind, format);
  }

  // Read from the task poll, so a reload keeps spinning until the queued files are filed.
  function renameRunning(key: string, kind: NamingKind, format = ""): boolean {
    const starting = renameStarting.value;
    if (starting?.key === key && starting.kind === kind && starting.format === format) return true;
    return namingCount(tasksQuery.data.value?.renaming, key, kind, format) > 0;
  }

  function openRename(key: string, label: string, kind: NamingKind, format = ""): void {
    renameTarget.value = { key, label, kind, format };
  }

  async function confirmRename(): Promise<void> {
    const target = renameTarget.value;
    if (!target) return;
    renameTarget.value = null;
    renameStarting.value = target;
    try {
      const result = await renameMutation.mutateAsync({
        source_key: target.key,
        kind: target.kind,
        format_template: target.format,
      });
      await loadRenameCounts();
      if (result.queued === 0) {
        toast("Nothing to resolve.");
        return;
      }
      await trackResolvePass(result.pass_id);
    } catch (error) {
      toast(errorMessage(error, "Could not resolve files."), "error");
    } finally {
      renameStarting.value = null;
    }
  }

  async function confirmResolve(scope: ResolveScope): Promise<void> {
    resolveOpen.value = false;
    await startResolve({ scope });
  }

  async function resolveTask(taskId: string): Promise<void> {
    await startResolve({ task_ids: [taskId] });
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
    [rawTasks, libraryBusy],
    () => syncPoll(rawTasks.value),
    { immediate: true },
  );

  // A finished pass has renamed rows, so the list they came from is stale.
  watch(libraryBusy, (busy, wasBusy) => {
    if (wasBusy && !busy) void loadTasks(true);
  });

  watch(tasksQuery.data, () => reportSettledResolves());

  useEventListener(document, "visibilitychange", handleVisibilityChange);
  onBeforeUnmount(() => pausePolling());

  return {
    addDownloadTask,
    cancelTask,
    clearPending,
    confirmPlaylistSelection,
    confirmResolve,
    historyRefreshing,
    historyResolving,
    libraryBusy,
    openResolveDialog,
    retryTask,
    playlistEntries,
    playlistOpen,
    playlistTitle,
    confirmRename,
    loadRenameCounts,
    openRename,
    refreshHistory,
    removeTask,
    renameCount,
    renameRunning,
    renameTarget,
    resolveFlagged,
    resolveOpen,
    resolveTask,
    resolveTotal,
    setTaskSource,
    taskItems,
    countsByMenu,
    countsByMediaMenu,
    tasksErrorMessage,
    tasksLoading,
  };
}
