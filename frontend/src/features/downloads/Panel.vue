<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import IconDelete from "~icons/material-symbols/delete";
import { Link as IconLink } from "@lucide/vue";
import IconMovie from "~icons/material-symbols/movie";
import IconMusic from "~icons/material-symbols/music-note";
import IconPause from "~icons/material-symbols/pause";
import IconResume from "~icons/material-symbols/play-arrow";
import IconSeen from "~icons/material-symbols/visibility";
import IconCheck from "~icons/material-symbols/sync";

import TaskCollection from "@/components/task/TaskCollection.vue";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import { DialogShell as Dialog } from "@/components/ui/dialog";
import { FieldGroup, FieldLabel, FieldLegend, FieldSeparator, FieldSet } from "@/components/ui/field";
import { IconImage } from "@/components/ui/icon-image";
import { SegmentedControl, SegmentedControlItem } from "@/components/ui/segmented-control";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import PostProcessingFields from "@/features/downloads/PostProcessingFields.vue";
import { qualityFieldsFor, type QualityField } from "@/features/downloads/qualityFields";
import { useDashboard } from "@/composables/useDashboard";
import { COUNT_ICONS, TRACKER_INTERVALS } from "@/ui";
import type { Component } from "vue";
import type { PostProcessingSelection, QualitySelection, Tracker } from "@/types";
import {
  createPostProcessingSelection,
  createQualitySelection,
  constrainPostProcessingSelection,
  postProcessingCapabilitiesForQuality,
  sourceIconUrl,
} from "@/utils/dashboard";

const {
  activeMenu,
  activePage,
  activeTasks,
  cancelTask,
  checkTracker,
  completedTasks,
  deleteTracker,
  historyError,
  historyFetchingMore,
  historyHasMore,
  historyLoading,
  loadMoreHistory,
  loadMoreTrackerHistory,
  mediaFilter,
  menuTrackers,
  openTracker,
  openTrackerId,
  qualityOptions,
  removeTask,
  resolveTask,
  retryTask,
  setTaskSource,
  sourceProfiles,
  tasksErrorMessage,
  tasksLoading,
  trackerHistoryError,
  trackerHistoryFetchingMore,
  trackerHistoryHasMore,
  trackerHistoryLoading,
  trackers,
  trackersError,
  trackersLoading,
  trackerTasks,
  updateTracker,
  viewMode,
} = useDashboard();

// Both pages render the same list; only the source of its rows differs.
const isHistory = computed(() => activePage.value === "history");
const isTrackers = computed(() => activePage.value === "trackers");
// Paging restarts from the top whenever the query behind the list changes.
const listKey = computed(
  () => `${activePage.value}|${openTrackerId.value}|${activeMenu.value}|${mediaFilter.value}`,
);

// A tracker saved from the URL field waits idle until its dialog is applied.
function started(tracker: Tracker): boolean {
  return tracker.enabled || Boolean(tracker.last_checked_at);
}

function trackerStatus(tracker: Tracker): string {
  if (tracker.checking) return "Checking";
  if (!started(tracker)) return "Not started";
  return tracker.enabled ? "" : "Paused";
}

const TRACKER_STATS: { label: string; icon: Component; count: (counts: Tracker["counts"]) => number }[] = [
  { label: "Seen", icon: IconSeen, count: (counts) => counts.seen },
  { label: "Downloaded", icon: COUNT_ICONS.completed, count: (counts) => counts.completed },
  { label: "Queued", icon: COUNT_ICONS.queued, count: (counts) => counts.queued + counts.running },
  { label: "Failed", icon: COUNT_ICONS.failed, count: (counts) => counts.failed },
];

function trackerStats(tracker: Tracker): { label: string; value: number; icon: Component }[] {
  return TRACKER_STATS.map((stat) => ({ label: stat.label, icon: stat.icon, value: stat.count(tracker.counts) }));
}

function toggleTracker(tracker: Tracker): void {
  void updateTracker(tracker.id, { enabled: !tracker.enabled }, tracker.enabled ? "Tracker paused." : "Tracker resumed.");
}

// The dialog edits a draft; nothing reaches the tracker until Apply.
const draftSelection = reactive<QualitySelection>(createQualitySelection());
const draftPostProcessing = reactive<PostProcessingSelection>(createPostProcessingSelection());
const draftInterval = ref("");
const draftBackfill = ref(true);
const draftQualityFields = computed(() => qualityFieldsFor(draftSelection, qualityOptions.value));
const draftCapabilities = computed(() => postProcessingCapabilitiesForQuality(draftSelection, qualityOptions.value));

watch(openTrackerId, () => {
  const tracker = openTracker.value;
  if (!tracker) return;
  Object.assign(draftSelection, createQualitySelection(tracker.quality, qualityOptions.value));
  Object.assign(draftPostProcessing, createPostProcessingSelection(tracker.post_processing));
  draftInterval.value = String(tracker.interval_seconds);
  draftBackfill.value = tracker.backfill;
});

function showTracker(trackerId: string): void {
  openTrackerId.value = trackerId;
}

function closeTracker(): void {
  openTrackerId.value = "";
}

function setDraftField(key: QualityField["key"], value: string | string[]): void {
  if (typeof value === "string") {
    Object.assign(draftSelection, createQualitySelection({ ...draftSelection, [key]: value }, qualityOptions.value));
  }
}

function setDraftMode(value: string | string[]): void {
  if (value === "video" || value === "audio") {
    Object.assign(draftSelection, createQualitySelection({ ...draftSelection, mode: value }, qualityOptions.value));
  }
}

function setDraftPostProcessing(next: PostProcessingSelection): void {
  Object.assign(draftPostProcessing, createPostProcessingSelection(next));
}

function setDraftInterval(value: string | string[]): void {
  if (typeof value === "string" && value) draftInterval.value = value;
}

function setDraftBackfill(value: boolean): void {
  draftBackfill.value = value;
}

async function applyTracker(): Promise<void> {
  const tracker = openTracker.value;
  if (!tracker) return;
  const starting = !started(tracker);
  closeTracker();
  await updateTracker(
    tracker.id,
    {
      quality: createQualitySelection(draftSelection, qualityOptions.value),
      post_processing: constrainPostProcessingSelection(draftPostProcessing, draftCapabilities.value),
      interval_seconds: Number(draftInterval.value),
      backfill: draftBackfill.value,
      ...(starting ? { enabled: true } : {}),
    },
    starting ? "Tracking started." : "Tracker updated.",
  );
}

const deleting = ref<Tracker | null>(null);
const deleteFiles = ref(false);

function openDelete(tracker: Tracker): void {
  deleteFiles.value = false;
  deleting.value = tracker;
}

async function confirmDelete(): Promise<void> {
  const tracker = deleting.value;
  if (!tracker) return;
  deleting.value = null;
  await deleteTracker(tracker.id, deleteFiles.value);
}
</script>

<template>
  <section v-if="isTrackers" aria-label="Trackers" class="flex flex-col gap-2">
    <div
      v-if="trackersLoading || trackersError || (trackers.length && menuTrackers.length === 0)"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 px-4 text-center text-white in-[.light-mode]:text-black"
    >
      <template v-if="trackersLoading">Loading trackers...</template>
      <template v-else-if="trackersError">{{ trackersError }}</template>
      <template v-else>No trackers for this platform.</template>
    </div>

    <Table v-else-if="viewMode === 'table' && menuTrackers.length">
      <TableHeader>
        <TableRow>
          <TableHead class="w-1/3 min-w-48 whitespace-nowrap">Name</TableHead>
          <TableHead class="w-2/3 min-w-72 whitespace-nowrap">Source</TableHead>
          <TableHead
            v-for="stat in TRACKER_STATS"
            :key="stat.label"
            class="w-px whitespace-nowrap text-right"
          >
            {{ stat.label }}
          </TableHead>
          <TableHead class="w-px"></TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <TableRow
          v-for="tracker in menuTrackers"
          :key="tracker.id"
          class="glass-rise cursor-pointer"
          @click="showTracker(tracker.id)"
        >
          <TableCell class="w-1/3 max-w-0 min-w-48">
            <div class="flex items-center gap-2 text-white in-[.light-mode]:text-black">
              <IconImage :src="sourceIconUrl(tracker.source_key)" class="h-4 w-4 shrink-0" />
              <span class="truncate" :title="tracker.name">{{ tracker.name }}</span>
              <span v-if="trackerStatus(tracker)" class="shrink-0 text-xs text-white/60 in-[.light-mode]:text-black/60">
                {{ trackerStatus(tracker) }}
              </span>
            </div>
            <div v-if="tracker.last_error" class="truncate text-xs text-destructive" :title="tracker.last_error">
              {{ tracker.last_error }}
            </div>
          </TableCell>
          <TableCell class="w-2/3 max-w-0 min-w-72">
            <a
              :href="tracker.source_url"
              target="_blank"
              rel="noopener noreferrer"
              class="truncate block min-w-0 font-mono text-accent underline decoration-dotted underline-offset-2 hover:decoration-solid"
              :title="tracker.source_url"
              @click.stop
            >
              {{ tracker.source_url }}
            </a>
          </TableCell>
          <TableCell
            v-for="stat in trackerStats(tracker)"
            :key="stat.label"
            class="w-px whitespace-nowrap text-right tabular-nums text-white in-[.light-mode]:text-black"
          >
            {{ stat.value }}
          </TableCell>
          <TableCell class="w-px" @click.stop>
            <div class="flex items-center justify-end gap-1.5">
              <Button
                v-if="started(tracker)"
                type="button"
                title="Check now"
                aria-label="Check now"
                :disabled="tracker.checking || !tracker.enabled"
                @click="checkTracker(tracker.id)"
              >
                <template #icon>
                  <IconCheck aria-hidden="true" :class="{ 'animate-spin': tracker.checking }" />
                </template>
              </Button>
              <Button
                v-if="started(tracker)"
                type="button"
                :title="tracker.enabled ? 'Pause' : 'Resume'"
                :aria-label="tracker.enabled ? 'Pause' : 'Resume'"
                @click="toggleTracker(tracker)"
              >
                <template #icon>
                  <IconPause v-if="tracker.enabled" aria-hidden="true" />
                  <IconResume v-else aria-hidden="true" />
                </template>
              </Button>
              <Button
                type="button"
                variant="destructive"
                title="Delete tracker"
                aria-label="Delete tracker"
                @click="openDelete(tracker)"
              >
                <template #icon>
                  <IconDelete aria-hidden="true" />
                </template>
              </Button>
            </div>
          </TableCell>
        </TableRow>
      </TableBody>
    </Table>

    <div v-else class="grid grid-cols-1 gap-2">
      <Card
        v-for="tracker in menuTrackers"
        :key="tracker.id"
        class="glass-rise glass-hoverable hover:-translate-y-0.5 cursor-pointer"
        role="button"
        tabindex="0"
        :aria-label="`Open ${tracker.name}`"
        @click="showTracker(tracker.id)"
        @keydown.enter="showTracker(tracker.id)"
      >
        <CardHeader>
          <CardTitle>
            <IconImage :src="sourceIconUrl(tracker.source_key)" class="mr-1.5 inline h-4 w-4 align-[-2px]" />
            {{ tracker.name }}
            <span v-if="trackerStatus(tracker)" class="ml-2 text-xs font-normal text-white/60 in-[.light-mode]:text-black/60">
              {{ trackerStatus(tracker) }}
            </span>
          </CardTitle>
          <CardDescription class="flex flex-col gap-1">
            <a
              :href="tracker.source_url"
              target="_blank"
              rel="noopener noreferrer"
              class="block font-mono text-accent underline decoration-dotted underline-offset-2 hover:decoration-solid"
              @click.stop
            >
              {{ tracker.source_url }}
            </a>
            <span class="flex flex-wrap items-center gap-x-4 gap-y-1">
              <span
                v-for="stat in trackerStats(tracker)"
                :key="stat.label"
                class="inline-flex items-center gap-1.5 tabular-nums"
                :title="stat.label"
                :aria-label="`${stat.label}: ${stat.value}`"
              >
                <component :is="stat.icon" class="h-4 w-4" aria-hidden="true" />
                {{ stat.value }}
              </span>
            </span>
          </CardDescription>
          <CardAction @click.stop>
            <Button
              v-if="started(tracker)"
              type="button"
              title="Check now"
              aria-label="Check now"
              :disabled="tracker.checking || !tracker.enabled"
              @click="checkTracker(tracker.id)"
            >
              <template #icon>
                <IconCheck aria-hidden="true" :class="{ 'animate-spin': tracker.checking }" />
              </template>
            </Button>
            <Button
              v-if="started(tracker)"
              type="button"
              :title="tracker.enabled ? 'Pause' : 'Resume'"
              :aria-label="tracker.enabled ? 'Pause' : 'Resume'"
              @click="toggleTracker(tracker)"
            >
              <template #icon>
                <IconPause v-if="tracker.enabled" aria-hidden="true" />
                <IconResume v-else aria-hidden="true" />
              </template>
            </Button>
            <Button
              type="button"
              variant="destructive"
              title="Delete tracker"
              aria-label="Delete tracker"
              @click="openDelete(tracker)"
            >
              <template #icon>
                <IconDelete aria-hidden="true" />
              </template>
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent v-if="tracker.last_error">
          <div class="wrap-break-word whitespace-pre-line text-destructive">{{ tracker.last_error }}</div>
        </CardContent>
      </Card>
    </div>

    <Dialog
      :open="Boolean(openTracker)"
      :title="openTracker?.name || 'Tracker'"
      hide-title
      content-class="fixed left-1/2 top-1/2 z-70 flex max-h-[90dvh] w-[min(900px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
      @update:open="(open) => !open && closeTracker()"
    >
      <template v-if="openTracker">
        <header class="glass-chrome flex shrink-0 flex-col gap-1.5 rounded-none border-0 border-b border-(--glass-border) shadow-none py-4 pl-5 pr-14 sm:pl-6">
          <div class="flex min-w-0 items-center gap-2 text-lg font-semibold">
            <IconImage :src="sourceIconUrl(openTracker.source_key)" class="h-5 w-5 shrink-0" />
            <span class="truncate">{{ openTracker.name }}</span>
            <Button
              as="a"
              variant="ghost"
              size="sm"
              :href="openTracker.source_url"
              target="_blank"
              rel="noopener noreferrer"
              :title="openTracker.source_url"
              aria-label="Open creator page"
            >
              <template #icon>
                <IconLink aria-hidden="true" />
              </template>
            </Button>
            <span v-if="trackerStatus(openTracker)" class="shrink-0 text-xs font-normal text-white/60 in-[.light-mode]:text-black/60">
              {{ trackerStatus(openTracker) }}
            </span>
          </div>
          <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
            <span
              v-for="stat in trackerStats(openTracker)"
              :key="stat.label"
              class="inline-flex items-center gap-1.5 tabular-nums"
              :title="stat.label"
              :aria-label="`${stat.label}: ${stat.value}`"
            >
              <component :is="stat.icon" class="h-4 w-4" aria-hidden="true" />
              <strong>{{ stat.value }}</strong>
            </span>
          </div>
          <div v-if="openTracker.last_error" class="wrap-break-word whitespace-pre-line text-sm text-destructive">
            {{ openTracker.last_error }}
          </div>
        </header>

        <div class="min-h-0 flex-1 overflow-y-auto p-5 sm:p-6 flex flex-col gap-6">
          <FieldSet>
            <FieldLegend>{{ draftSelection.mode === "audio" ? "Audio" : "Video" }}</FieldLegend>
            <SegmentedControl
              v-if="qualityOptions.video.length"
              :model-value="draftSelection.mode"
              aria-label="Mode"
              class="self-start"
              @update:model-value="setDraftMode"
            >
              <SegmentedControlItem value="video" aria-label="Video" title="Video">
                <IconMovie class="w-3.5 h-3.5" aria-hidden="true" />
                <span>Video</span>
              </SegmentedControlItem>
              <SegmentedControlItem value="audio" aria-label="Audio" title="Audio">
                <IconMusic class="w-3.5 h-3.5" aria-hidden="true" />
                <span>Audio</span>
              </SegmentedControlItem>
            </SegmentedControl>
            <FieldGroup>
              <Combobox
                v-for="field in draftQualityFields"
                :key="field.key"
                :model-value="draftSelection[field.key]"
                :items="field.items"
                :label="field.label"
                label-placement="start"
                :placeholder="field.placeholder"
                :empty-text="field.emptyText"
                @update:model-value="(value) => setDraftField(field.key, value)"
              />
            </FieldGroup>
          </FieldSet>

          <FieldSeparator />

          <PostProcessingFields
            :model-value="draftPostProcessing"
            :capabilities="draftCapabilities"
            @update:model-value="setDraftPostProcessing"
          />

          <FieldSeparator />

          <FieldSet>
            <FieldLegend>Items</FieldLegend>
            <TaskCollection
              :tasks="trackerTasks"
              :view-mode="viewMode"
              :list-key="listKey"
              :loading="trackerHistoryLoading"
              page-kind="trackers"
              :source-profiles="sourceProfiles"
              :error-message="trackerHistoryError"
              :has-more="trackerHistoryHasMore"
              :fetching-more="trackerHistoryFetchingMore"
              @cancel="cancelTask"
              @remove="removeTask"
              @resolve="resolveTask"
              @retry="retryTask"
              @set-source="setTaskSource"
              @load-more="loadMoreTrackerHistory"
            />
          </FieldSet>
        </div>

        <footer
          class="flex shrink-0 flex-wrap items-center justify-between gap-3 border-0 border-t border-(--glass-border) bg-primary/45 backdrop-blur-md px-5 py-4 sm:px-6"
        >
          <div class="flex flex-wrap items-center gap-3">
            <Combobox
              :model-value="draftInterval"
              :items="TRACKER_INTERVALS"
              aria-label="Check interval"
              placeholder="Select..."
              empty-text="No intervals."
              @update:model-value="setDraftInterval"
            />
            <FieldLabel
              class="cursor-pointer items-center gap-2 whitespace-nowrap"
              :title="openTracker.last_success_at ? 'Only applies before the first check.' : undefined"
            >
              <Checkbox
                :checked="draftBackfill"
                :disabled="Boolean(openTracker.last_success_at)"
                @update:checked="setDraftBackfill"
              />
              <span>Download existing media</span>
            </FieldLabel>
          </div>
          <div class="flex items-center gap-2">
            <Button variant="ghost" type="button" @click="closeTracker">Cancel</Button>
            <Button variant="primary" type="button" @click="applyTracker">Apply</Button>
          </div>
        </footer>
      </template>
    </Dialog>

    <Dialog
      :open="Boolean(deleting)"
      :title="deleting ? `Delete ${deleting.name}?` : 'Delete tracker?'"
      description="The tracker stops checking and its queued downloads are removed."
      content-class="fixed left-1/2 top-1/2 z-70 flex w-[min(480px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
      @update:open="(open) => !open && (deleting = null)"
    >
      <div class="px-5 py-4 sm:px-6">
        <FieldLabel class="cursor-pointer items-center gap-2">
          <Checkbox :checked="deleteFiles" @update:checked="(value: boolean) => (deleteFiles = value)" />
          <span>Also delete downloaded files</span>
        </FieldLabel>
      </div>
      <div class="flex shrink-0 items-center justify-end gap-2 border-0 border-t border-(--glass-border) bg-primary/45 backdrop-blur-md px-5 py-4 sm:px-6">
        <Button variant="ghost" type="button" @click="deleting = null">Cancel</Button>
        <Button variant="destructive" type="button" @click="confirmDelete">Delete</Button>
      </div>
    </Dialog>
  </section>

  <section v-else :aria-label="isHistory ? 'Download history' : 'Download queue'">
    <TaskCollection
      :tasks="isHistory ? completedTasks : activeTasks"
      :view-mode="viewMode"
      :list-key="listKey"
      :loading="isHistory ? historyLoading : tasksLoading"
      :page-kind="isHistory ? 'history' : 'downloads'"
      :source-profiles="sourceProfiles"
      :error-message="isHistory ? historyError : tasksErrorMessage"
      :has-more="isHistory ? historyHasMore : undefined"
      :fetching-more="isHistory ? historyFetchingMore : undefined"
      @cancel="cancelTask"
      @remove="removeTask"
      @resolve="resolveTask"
      @retry="retryTask"
      @set-source="setTaskSource"
      @load-more="loadMoreHistory"
    />
  </section>
</template>
