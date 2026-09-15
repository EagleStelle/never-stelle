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
  PostProcessingSelection,
  QualitySelection,
  TaskItem,
  ToastType,
  Tracker,
  TrackerPayload,
  TrackersResponse,
} from "@/types";
import { errorMessage, extractUrl } from "@/utils/dashboard";

interface UseTrackersOptions {
  enabled: Ref<boolean>;
  getQuality: () => QualitySelection;
  getPostProcessing: () => PostProcessingSelection;
  // The task feed the downloads page polls; its rows carry their tracker.
  tasks: Ref<TaskItem[]>;
  toast: (message: string, type?: ToastType) => void;
  url: Ref<string>;
}

// Checking now, or due and waiting for the server to pick it up.
function isBusy(tracker: Pick<Tracker, "checking" | "enabled" | "next_check_at">): boolean {
  return (
    tracker.checking ||
    (tracker.enabled && Boolean(tracker.next_check_at) && Date.parse(tracker.next_check_at) <= Date.now())
  );
}

export function useTrackers({ enabled, getQuality, getPostProcessing, tasks, toast, url }: UseTrackersOptions) {
  const queryClient = useQueryClient();
  // The tracker whose dialog is open; "" when none is.
  const openTrackerId = ref("");

  const trackersQuery = useQuery<TrackersResponse>({
    queryKey: TRACKERS_QUERY_KEY,
    queryFn: ({ signal }) => getTrackers(signal),
    enabled,
    staleTime: 1000,
    refetchInterval: (query) => ((query.state.data?.trackers || []).some(isBusy) ? POLL_RUNNING_MS : false),
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

  // The tracker is saved idle and listed at once; its dialog decides how it starts.
  async function addTracker(): Promise<void> {
    const sourceUrl = extractUrl(url.value);
    if (!sourceUrl) {
      toast("Enter a valid URL.", "error");
      return;
    }
    url.value = "";
    try {
      const tracker = await createTracker({
        url: sourceUrl,
        quality: getQuality(),
        post_processing: getPostProcessing(),
      });
      await refreshTrackers();
      openTrackerId.value = tracker.id;
    } catch (error) {
      toast(errorMessage(error, "Could not add tracker."), "error");
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
    openTracker,
    openTrackerId,
    trackers,
    trackersError,
    trackersLoading,
    updateTracker,
  };
}
