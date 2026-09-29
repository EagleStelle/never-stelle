import { computed, ref, watch, type Ref } from "vue";
import { useQuery, useQueryClient } from "@tanstack/vue-query";

import {
  checkTrackers as checkTrackersRequest,
  createTracker,
  deleteTrackers as deleteTrackersRequest,
  getTrackers,
  setTrackersEnabled as setTrackersEnabledRequest,
  stopTrackerChecks as stopTrackerChecksRequest,
  updateTracker as updateTrackerRequest,
} from "@/api";
import { ACTION_ICONS, HISTORY_QUERY_KEY, POLL_RUNNING_MS, TASKS_QUERY_KEY, TRACKERS_QUERY_KEY } from "@/ui";
import type {
  ItemAction,
  TaskItem,
  ToastType,
  Tracker,
  TrackerPayload,
  TrackersResponse,
} from "@/types";
import { errorMessage, extractUrl, plural } from "@/utils/dashboard";

interface UseTrackersOptions {
  enabled: Ref<boolean>;
  // The task feed the downloads page polls; its rows carry their tracker.
  tasks: Ref<TaskItem[]>;
  toast: (message: string, type?: ToastType) => void;
  url: Ref<string>;
}

// Browsers fire a longer timer at once, so a wait past this would poll without pause.
const MAX_TIMER_MS = 2 ** 31 - 1;

type Schedule = Pick<Tracker, "checking" | "queued" | "enabled" | "next_check_at">;

// Checking now, or due and waiting for a free check slot.
export function hasCheck(tracker: Pick<Tracker, "checking" | "queued">): boolean {
  return tracker.checking || tracker.queued;
}

// Fast while a check runs, so its seen count moves live; otherwise wake when the next one falls due.
function pollDelay(trackers: Schedule[]): number | false {
  if (trackers.some(hasCheck)) return POLL_RUNNING_MS;
  const due = trackers
    .filter((tracker) => tracker.enabled && tracker.next_check_at)
    .map((tracker) => Date.parse(tracker.next_check_at))
    .filter(Number.isFinite);
  if (!due.length) return false;
  return Math.min(Math.max(Math.min(...due) - Date.now(), POLL_RUNNING_MS), MAX_TIMER_MS);
}

export function useTrackers({ enabled, tasks, toast, url }: UseTrackersOptions) {
  const queryClient = useQueryClient();
  // The tracker whose dialog is open; "" when none is.
  const openTrackerId = ref("");
  // A link whose dialog is open before it is saved; "" when none is.
  const newTrackerUrl = ref("");
  // Trackers waiting on the delete dialog.
  const deletingTrackers = ref<Tracker[]>([]);

  const trackersQuery = useQuery<TrackersResponse>({
    queryKey: TRACKERS_QUERY_KEY,
    queryFn: ({ signal }) => getTrackers(signal),
    enabled,
    staleTime: 1000,
    refetchInterval: (query) => pollDelay(query.state.data?.trackers || []),
  });

  // Queue counts are read off the task feed, so they move with the queue exactly as the downloads page does.
  const queueCounts = computed(() => {
    const counts = new Map<string, { queued: number; running: number; failed: number }>();
    for (const task of tasks.value) {
      if (!task.tracker_id) continue;
      const bucket = counts.get(task.tracker_id) ?? { queued: 0, running: 0, failed: 0 };
      if (task.status === "pending") bucket.queued += 1;
      else if (task.status === "running") bucket.running += 1;
      else if (task.status === "failed") bucket.failed += 1;
      counts.set(task.tracker_id, bucket);
    }
    return counts;
  });
  const trackers = computed<Tracker[]>(() =>
    (trackersQuery.data.value?.trackers || []).map((tracker) => ({
      ...tracker,
      counts: { queued: 0, running: 0, failed: 0, ...queueCounts.value.get(tracker.id), ...tracker.counts },
    })),
  );
  const trackersLoading = computed(() => trackersQuery.isPending.value);
  const trackersError = computed(() =>
    trackersQuery.error.value ? errorMessage(trackersQuery.error.value, "Could not load trackers.") : "",
  );
  const openTracker = computed(() => trackers.value.find((tracker) => tracker.id === openTrackerId.value));

  // A routed tracker that no longer exists closes its dialog.
  watch([trackersQuery.data, openTrackerId], ([data]) => {
    if (data && openTrackerId.value && !openTracker.value) openTrackerId.value = "";
  });

  // A tracker row leaving the queue finished or was removed: only then can done change.
  watch(
    () => new Set(tasks.value.filter((task) => task.tracker_id).map((task) => task.vid)),
    (next, previous) => {
      if (![...previous].some((id) => !next.has(id))) return;
      void queryClient.invalidateQueries({ queryKey: TRACKERS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: HISTORY_QUERY_KEY });
    },
  );

  // Manual checks waiting to report, with the seen count and check time from when they were asked for.
  const requestedChecks = new Map<string, { seen: number; lastCheckedAt: string }>();

  watch(trackers, (current) => {
    for (const [trackerId, before] of requestedChecks) {
      const tracker = current.find((item) => item.id === trackerId);
      if (!tracker) {
        requestedChecks.delete(trackerId);
        continue;
      }
      if (tracker.checking || tracker.last_checked_at === before.lastCheckedAt) continue;
      requestedChecks.delete(trackerId);
      if (tracker.last_error) {
        toast(`${tracker.name}: ${tracker.last_error}`, "error");
        continue;
      }
      const found = tracker.counts.seen - before.seen;
      toast(found > 0 ? `${tracker.name}: ${found} new item${plural(found)}.` : `${tracker.name}: no new media.`);
    }
  });

  // A running check queues rows as it lists, so the queue and history follow it.
  watch(trackersQuery.data, (next, previous) => {
    const busy = (next?.trackers || []).some((tracker) => tracker.checking);
    const wasBusy = (previous?.trackers || []).some((tracker) => tracker.checking);
    if (busy || wasBusy) {
      void queryClient.invalidateQueries({ queryKey: TASKS_QUERY_KEY });
      if (!busy) void queryClient.invalidateQueries({ queryKey: HISTORY_QUERY_KEY });
    }
  });

  async function refreshTrackers(): Promise<void> {
    await trackersQuery.refetch();
  }

  // Nothing is saved until the dialog is applied.
  function addTracker(): void {
    const sourceUrl = extractUrl(url.value);
    if (!sourceUrl) {
      toast("Enter a valid URL.", "error");
      return;
    }
    url.value = "";
    openTrackerId.value = "";
    newTrackerUrl.value = sourceUrl;
  }

  async function saveTracker(payload: Parameters<typeof createTracker>[0]): Promise<boolean> {
    try {
      await createTracker(payload);
      toast("Tracking started.");
      await refreshTrackers();
      return true;
    } catch (error) {
      toast(errorMessage(error, "Could not add tracker."), "error");
      return false;
    }
  }

  async function updateTracker(trackerId: string, payload: TrackerPayload, message: string): Promise<void> {
    try {
      await updateTrackerRequest(trackerId, payload);
      toast(message);
      await refreshTrackers();
    } catch (error) {
      toast(errorMessage(error, "Could not update tracker."), "error");
    }
  }

  function pick(ids: string[]): Tracker[] {
    const wanted = new Set(ids);
    return trackers.value.filter((tracker) => wanted.has(tracker.id));
  }

  // Each tracker reports once its own check is done, not when it is asked for.
  async function checkTrackers(ids: string[]): Promise<void> {
    try {
      const result = await checkTrackersRequest(ids);
      for (const tracker of pick(ids).filter((item) => item.enabled)) {
        requestedChecks.set(tracker.id, { seen: tracker.counts.seen, lastCheckedAt: tracker.last_checked_at });
      }
      if (result.errors.length > 0) toast(result.errors[0], "error");
      await refreshTrackers();
    } catch (error) {
      toast(errorMessage(error, "Could not check tracker."), "error");
    }
  }

  // A queued check never ran, so only the running ones are reported stopped.
  async function stopTrackerChecks(ids: string[]): Promise<void> {
    const running = pick(ids).filter((tracker) => tracker.checking).length;
    for (const id of ids) requestedChecks.delete(id);
    try {
      await stopTrackerChecksRequest(ids);
      if (running > 0) toast(`Stopped ${running} check${plural(running)}.`);
      await refreshTrackers();
    } catch (error) {
      toast(errorMessage(error, "Could not stop the check."), "error");
    }
  }

  async function setTrackersEnabled(ids: string[], enabled: boolean): Promise<void> {
    try {
      const result = await setTrackersEnabledRequest(ids, enabled);
      toast(`${enabled ? "Resumed" : "Paused"} ${result.count} tracker${plural(result.count)}.`);
      await refreshTrackers();
    } catch (error) {
      toast(errorMessage(error, "Could not update trackers."), "error");
    }
  }

  async function deleteTrackers(ids: string[], deleteFiles: boolean): Promise<void> {
    if (ids.includes(openTrackerId.value)) openTrackerId.value = "";
    try {
      const result = await deleteTrackersRequest(ids, deleteFiles);
      const noun = `tracker${plural(result.count)}`;
      toast(deleteFiles ? `Deleted ${result.count} ${noun} and their files.` : `Deleted ${result.count} ${noun}.`);
      if (result.errors.length > 0) toast(result.errors[0], "error");
      await refreshTrackers();
      void queryClient.invalidateQueries({ queryKey: TASKS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: HISTORY_QUERY_KEY });
    } catch (error) {
      toast(errorMessage(error, "Could not delete trackers."), "error");
    }
  }

  function checkAction(ids: string[]): ItemAction {
    return { key: "check", label: "Check", icon: ACTION_ICONS.check, variant: "primary", run: () => void checkTrackers(ids) };
  }

  function stopAction(ids: string[]): ItemAction {
    return { key: "stop", label: "Stop", icon: ACTION_ICONS.stop, variant: "ghost", run: () => void stopTrackerChecks(ids) };
  }

  function enabledAction(ids: string[], enabled: boolean): ItemAction {
    return enabled
      ? { key: "resume", label: "Resume", icon: ACTION_ICONS.resume, variant: "ghost", run: () => void setTrackersEnabled(ids, true) }
      : { key: "pause", label: "Pause", icon: ACTION_ICONS.pause, variant: "ghost", run: () => void setTrackersEnabled(ids, false) };
  }

  function deleteAction(trackers: Tracker[]): ItemAction {
    return {
      key: "delete",
      label: "Delete",
      icon: ACTION_ICONS.delete,
      variant: "destructive-ghost",
      run: () => (deletingTrackers.value = trackers),
    };
  }

  // The actions a selection bar offers, each on the selected trackers it applies to.
  function trackerBatchActions(selected: Tracker[]): ItemAction[] {
    const ids = (flag: (tracker: Tracker) => boolean) => selected.filter(flag).map((tracker) => tracker.id);
    const idle = ids((tracker) => tracker.enabled && !hasCheck(tracker));
    const busy = ids(hasCheck);
    const enabledOnes = ids((tracker) => tracker.enabled);
    const paused = ids((tracker) => !tracker.enabled);
    const entries: [number, ItemAction][] = [
      [idle.length, checkAction(idle)],
      [busy.length, stopAction(busy)],
      [enabledOnes.length, enabledAction(enabledOnes, false)],
      [paused.length, enabledAction(paused, true)],
      [selected.length, deleteAction(selected)],
    ];
    return entries.filter(([count]) => count > 0).map(([, action]) => action);
  }

  // One tracker's actions: its check first, which stops a check running or waiting.
  function trackerActions(tracker: Tracker): ItemAction[] {
    const check: ItemAction = hasCheck(tracker)
      ? { ...stopAction([tracker.id]), label: "Stop check", icon: ACTION_ICONS.check, variant: "primary", spinning: tracker.checking }
      : { ...checkAction([tracker.id]), label: "Check now", disabled: !tracker.enabled };
    return [check, enabledAction([tracker.id], !tracker.enabled), deleteAction([tracker])];
  }

  return {
    addTracker,
    deleteTrackers,
    deletingTrackers,
    newTrackerUrl,
    openTracker,
    openTrackerId,
    saveTracker,
    trackerActions,
    trackerBatchActions,
    trackers,
    trackersError,
    trackersLoading,
    updateTracker,
  };
}
