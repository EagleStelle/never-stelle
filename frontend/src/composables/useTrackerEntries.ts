import { computed, type Ref } from "vue";
import { useQuery, useQueryClient } from "@tanstack/vue-query";

import { changeTrackerEntries, getTrackerEntries } from "@/api";
import { ACTION_ICONS, TASKS_QUERY_KEY, TRACKER_ENTRIES_QUERY_KEY, TRACKERS_QUERY_KEY } from "@/ui";
import type { ItemAction, SeenDelete, TaskItem, ToastType, TrackerEntryAction } from "@/types";
import { errorMessage, plural, runBatch } from "@/utils/dashboard";

interface UseTrackerEntriesOptions {
  trackerId: Ref<string>;
  // The tracker's own source, whose icon the items show.
  sourceKey: Ref<string>;
  enabled: Ref<boolean>;
  toast: (message: string, type?: ToastType) => void;
  // Opens the delete confirm for Seen items.
  requestDelete: (seen: SeenDelete) => void;
}

// Each item action as its button shows it, and the word its toast reports it with.
const ENTRY_ACTIONS: Record<TrackerEntryAction, Omit<ItemAction, "key" | "run"> & { done: string }> = {
  queue: { label: "Queue", icon: ACTION_ICONS.queue, variant: "primary", done: "Queued" },
  dismiss: { label: "Dismiss", icon: ACTION_ICONS.dismiss, variant: "ghost", done: "Dismissed" },
  delete: { label: "Delete", icon: ACTION_ICONS.delete, variant: "destructive-ghost", done: "Deleted" },
};

// Named by its state, as a queued row is named Queued until its file is known.
const SEEN = "seen";

export function isTrackerEntry(task: TaskItem): boolean {
  return task.status === SEEN;
}

// The open tracker's items it only saw, as rows of its items list.
export function useTrackerEntries({ trackerId, sourceKey, enabled, toast, requestDelete }: UseTrackerEntriesOptions) {
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: [...TRACKER_ENTRIES_QUERY_KEY, trackerId],
    queryFn: ({ signal }) => getTrackerEntries(trackerId.value, signal),
    enabled,
    staleTime: 1000,
  });

  const trackerEntries = computed<TaskItem[]>(() =>
    (query.data.value?.urls || []).map((url) => ({
      vid: `tracker-entry:${url}`,
      status: SEEN,
      status_label: "Seen",
      progress: 0,
      progress_pct: 0,
      source_url: url,
      creator: "",
      file_size: 0,
      resolved_folder: "",
      resolved_filename: "",
      resolved_full_path: "",
      preview_warning: "",
      can_delete: false,
      can_retry: false,
      can_download: false,
      task_type: "",
      source_key: sourceKey.value,
      error: "",
      tracker_id: trackerId.value,
    })),
  );
  const entriesError = computed(() => (query.error.value ? errorMessage(query.error.value, "Could not load items.") : ""));

  async function change(action: TrackerEntryAction, urls: string[]): Promise<void> {
    const done = (count: number) => `${ENTRY_ACTIONS[action].done} ${count} item${plural(count)}.`;
    await runBatch(() => changeTrackerEntries(trackerId.value, action, urls), done, `Could not ${action}.`, toast);
    // Counts and item lists both sit under the trackers key.
    void queryClient.invalidateQueries({ queryKey: TRACKERS_QUERY_KEY });
    if (action === "queue") void queryClient.invalidateQueries({ queryKey: TASKS_QUERY_KEY });
  }

  function deleteOf(urls: string[]): SeenDelete {
    return { count: urls.length, remove: () => change("delete", urls) };
  }

  // A delete asks first, as a download's does.
  function entryAction(action: TrackerEntryAction, urls: string[]): ItemAction {
    const { done: _done, ...shown } = ENTRY_ACTIONS[action];
    const run = action === "delete" ? () => requestDelete(deleteOf(urls)) : () => void change(action, urls);
    return { key: action, ...shown, run };
  }

  function seenUrls(selected: TaskItem[]): string[] {
    return selected.filter(isTrackerEntry).map((task) => task.source_url);
  }

  // The item actions for the selected Seen rows; none for any other row.
  function entryBatchActions(selected: TaskItem[]): ItemAction[] {
    const urls = seenUrls(selected);
    if (!urls.length) return [];
    return (Object.keys(ENTRY_ACTIONS) as TrackerEntryAction[]).map((action) => entryAction(action, urls));
  }

  function seenDelete(selected: TaskItem[]): SeenDelete | undefined {
    const urls = seenUrls(selected);
    return urls.length ? deleteOf(urls) : undefined;
  }

  // Queue or clear every Seen item at once, whatever the list shows; none while nothing is seen.
  const seenActions = computed<ItemAction[]>(() => {
    const urls = query.data.value?.urls || [];
    if (!urls.length) return [];
    return [
      { ...entryAction("queue", urls), label: "Queue Seen", title: "Queues all seen items." },
      { ...entryAction("delete", urls), label: "Clear Seen", title: "Removes all seen items. Checks never queue them again." },
    ];
  });

  return { entriesError, entryBatchActions, seenActions, seenDelete, trackerEntries };
}
