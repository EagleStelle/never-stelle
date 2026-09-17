import { computed, ref, watch, type Ref } from "vue";
import { useQuery, useQueryClient } from "@tanstack/vue-query";

import {
  checkTracker as checkTrackerRequest,
  createTracker,
  deleteTracker as deleteTrackerRequest,
  getTrackers,
  updateTracker as updateTrackerRequest,
} from "@/api";
import { HISTORY_QUERY_KEY, POLL_RUNNING_MS, TASKS_QUERY_KEY, TRACKERS_QUERY_KEY } from "@/ui";
import type {
  TaskItem,
  ToastType,
  Tracker,
  TrackerPayload,
  TrackersResponse,
} from "@/types";
import { errorMessage, extractUrl } from "@/utils/dashboard";

interface UseTrackersOptions {
  enabled: Ref<boolean>;
  // The task feed the downloads page polls; its rows carry their tracker.
  tasks: Ref<TaskItem[]>;
  toast: (message: string, type?: ToastType) => void;
  url: Ref<string>;
}

// Browsers fire a longer timer at once, so a wait past this would poll without pause.
const MAX_TIMER_MS = 2 ** 31 - 1;

// Checking now, or due and waiting for the server to pick it up.
type Schedule = Pick<Tracker, "checking" | "enabled" | "next_check_at">;

function isBusy(tracker: Schedule): boolean {
  return (
    tracker.checking ||
    (tracker.enabled && Boolean(tracker.next_check_at) && Date.parse(tracker.next_check_at) <= Date.now())
  );
}

// Fast while a check runs, so its seen count moves live; otherwise wake when the next one falls due.
function pollDelay(trackers: Schedule[]): number | false {
  if (trackers.some(isBusy)) return POLL_RUNNING_MS;
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

  // A tracker row leaving the queue finished or was removed: only then can downloaded change.
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
      toast(found > 0 ? `${tracker.name}: ${found} new item${found === 1 ? "" : "s"}.` : `${tracker.name}: no new media.`);
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

  // Reports once the check is done, not when it is asked for.
  async function checkTracker(trackerId: string): Promise<void> {
    const tracker = trackers.value.find((item) => item.id === trackerId);
    try {
      await checkTrackerRequest(trackerId);
      if (tracker) requestedChecks.set(trackerId, { seen: tracker.counts.seen, lastCheckedAt: tracker.last_checked_at });
      await refreshTrackers();
    } catch (error) {
      toast(errorMessage(error, "Could not check tracker."), "error");
    }
  }

  async function deleteTracker(trackerId: string, deleteFiles: boolean): Promise<void> {
    if (openTrackerId.value === trackerId) openTrackerId.value = "";
    try {
      await deleteTrackerRequest(trackerId, deleteFiles);
      toast(deleteFiles ? "Tracker and its files deleted." : "Tracker deleted.");
      await refreshTrackers();
      void queryClient.invalidateQueries({ queryKey: TASKS_QUERY_KEY });
      void queryClient.invalidateQueries({ queryKey: HISTORY_QUERY_KEY });
    } catch (error) {
      toast(errorMessage(error, "Could not delete tracker."), "error");
    }
  }

  return {
    addTracker,
    checkTracker,
    deleteTracker,
    newTrackerUrl,
    openTracker,
    openTrackerId,
    saveTracker,
    trackers,
    trackersError,
    trackersLoading,
    updateTracker,
  };
}
