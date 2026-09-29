<script setup lang="ts">
import { computed, reactive, ref, useTemplateRef, watch } from "vue";
import { useElementSize } from "@vueuse/core";
import { Link as IconLink } from "@lucide/vue";
import IconSeen from "~icons/material-symbols/visibility";

import ItemActions from "@/components/task/ItemActions.vue";
import SelectionBar from "@/components/task/SelectionBar.vue";
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
import { DialogFooter, DialogShell as Dialog } from "@/components/ui/dialog";
import { FieldLabel, FieldLegend, FieldSet } from "@/components/ui/field";
import { IconImage } from "@/components/ui/icon-image";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import DownloadFields from "@/features/downloads/DownloadFields.vue";
import type { QualityField } from "@/features/downloads/qualityFields";
import HistoryToolbar from "@/features/history/Toolbar.vue";
import { useDashboard } from "@/composables/useDashboard";
import { provideScrollRoot } from "@/composables/useVirtualRows";
import { ACTION_ICONS, COUNT_ICONS, TILE_GRID, TRACKER_INTERVALS } from "@/ui";
import type { Component } from "vue";
import type { MediaMode, PostProcessingSelection, QualitySelection, Tracker } from "@/types";
import {
  createPostProcessingSelection,
  createQualitySelection,
  constrainPostProcessingSelection,
  postProcessingCapabilitiesForQuality,
  sourceIconUrl,
  sourceKeyFromUrl,
} from "@/utils/dashboard";

const {
  deleteTrackers,
  deletingTrackers,
  downloadPostProcessing,
  downloadSelection,
  itemBatch,
  itemSelection,
  loadMoreTrackerHistory,
  menuTrackers,
  newTrackerUrl,
  openTracker,
  openTrackerId,
  qualityOptions,
  saveTracker,
  settings,
  sourceProfiles,
  trackerActions,
  trackerHistoryError,
  trackerHistoryFetchingMore,
  trackerHistoryHasMore,
  trackerHistoryLoading,
  trackers,
  trackersError,
  trackersLoading,
  trackerSelection,
  trackerTasks,
  updateTracker,
  viewMode,
} = useDashboard();

const itemsScroll = useTemplateRef<HTMLElement>("itemsScroll");
provideScrollRoot(itemsScroll);
const { height: itemsViewHeight } = useElementSize(itemsScroll);

function trackerStatus(tracker: Tracker): string {
  if (tracker.checking) return "Checking";
  if (tracker.queued) return "Queued";
  return tracker.enabled ? "" : "Paused";
}

const TRACKER_STATS: { label: string; icon: Component; count: (counts: Tracker["counts"]) => number }[] = [
  { label: "Seen", icon: IconSeen, count: (counts) => counts.seen },
  { label: "Done", icon: COUNT_ICONS.completed, count: (counts) => counts.completed },
  { label: "Queued", icon: COUNT_ICONS.queued, count: (counts) => counts.queued + counts.running },
  { label: "Failed", icon: COUNT_ICONS.failed, count: (counts) => counts.failed },
];

function trackerStats(tracker: Tracker): { label: string; value: number; icon: Component }[] {
  return TRACKER_STATS.map((stat) => ({ label: stat.label, icon: stat.icon, value: stat.count(tracker.counts) }));
}

// The dialog edits a draft; nothing reaches the tracker until Apply.
const draftSelection = reactive<QualitySelection>(createQualitySelection());
const draftPostProcessing = reactive<PostProcessingSelection>(createPostProcessingSelection());
const draftInterval = ref("");
const draftCapabilities = computed(() => postProcessingCapabilitiesForQuality(draftSelection, qualityOptions.value));

// A new link starts from the toolbar and its source's tracker settings.
watch(
  () => openTracker.value?.id || newTrackerUrl.value,
  (key) => {
    if (!key) return;
    const tracker = openTracker.value;
    const source = settings.source_tracker_settings[sourceKeyFromUrl(newTrackerUrl.value, sourceProfiles.value)];
    Object.assign(draftSelection, createQualitySelection(tracker ? tracker.quality : downloadSelection, qualityOptions.value));
    Object.assign(draftPostProcessing, createPostProcessingSelection(tracker ? tracker.post_processing : downloadPostProcessing));
    draftInterval.value = String(
      tracker ? tracker.interval_seconds : (source?.interval_seconds ?? settings.tracker_settings.interval_seconds),
    );
  },
  { immediate: true },
);

function showTracker(trackerId: string): void {
  openTrackerId.value = trackerId;
}

function closeTracker(): void {
  openTrackerId.value = "";
  newTrackerUrl.value = "";
}

function setDraftField(key: QualityField["key"], value: string): void {
  Object.assign(draftSelection, createQualitySelection({ ...draftSelection, [key]: value }, qualityOptions.value));
}

// A mode starts from its own defaults; draft edits are not kept per mode.
function setDraftMode(mode: MediaMode): void {
  Object.assign(draftSelection, createQualitySelection(settings.default_quality[mode], qualityOptions.value));
}

function setDraftPostProcessing(next: PostProcessingSelection): void {
  Object.assign(draftPostProcessing, createPostProcessingSelection(next));
}

function setDraftInterval(value: string | string[]): void {
  if (typeof value === "string" && value) draftInterval.value = value;
}

const saving = ref(false);
const applyButton = useTemplateRef<InstanceType<typeof Button>>("applyButton");

function focusApply(event: Event): void {
  event.preventDefault();
  (applyButton.value?.$el as HTMLElement | undefined)?.focus();
}

// A new link stays open until it saves, so a failed save keeps the draft.
async function applyTracker(): Promise<void> {
  const choices = {
    quality: createQualitySelection(draftSelection, qualityOptions.value),
    post_processing: constrainPostProcessingSelection(draftPostProcessing, draftCapabilities.value),
    interval_seconds: Number(draftInterval.value),
  };
  const tracker = openTracker.value;
  if (tracker) {
    closeTracker();
    await updateTracker(tracker.id, choices, "Tracker updated.");
    return;
  }
  const url = newTrackerUrl.value;
  if (!url) return;
  saving.value = true;
  const saved = await saveTracker({ url, ...choices });
  saving.value = false;
  if (saved && newTrackerUrl.value === url) closeTracker();
}

const deleteFiles = ref(false);
const deleteTitle = computed(() =>
  deletingTrackers.value.length === 1
    ? `Delete ${deletingTrackers.value[0].name}?`
    : `Delete ${deletingTrackers.value.length} trackers?`,
);

watch(deletingTrackers, (next) => {
  if (next.length) deleteFiles.value = false;
});

async function confirmDelete(): Promise<void> {
  const ids = deletingTrackers.value.map((tracker) => tracker.id);
  deletingTrackers.value = [];
  if (ids.length) await deleteTrackers(ids, deleteFiles.value);
}
</script>

<template>
  <section aria-label="Trackers" class="flex flex-col gap-2">
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
          <TableHead class="w-px">
            <Checkbox
              :checked="trackerSelection.state"
              aria-label="Select all"
              title="Select all"
              @update:checked="trackerSelection.setAll"
            />
          </TableHead>
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
          v-bind="trackerSelection.itemProps(tracker, () => showTracker(tracker.id))"
          class="glass-rise cursor-pointer"
          :data-state="trackerSelection.isSelected(tracker) ? 'selected' : undefined"
        >
          <TableCell class="w-px" @click.stop>
            <Checkbox
              :checked="trackerSelection.isSelected(tracker)"
              :aria-label="`Select ${tracker.name}`"
              title="Select"
              @click="(event: MouseEvent) => trackerSelection.toggle(tracker, event.shiftKey)"
            />
          </TableCell>
          <TableCell class="w-1/3 max-w-0 min-w-48">
            <div class="flex items-center gap-2 text-white in-[.light-mode]:text-black">
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
            <div class="flex items-center gap-2">
              <IconImage :src="sourceIconUrl(tracker.source_key)" class="h-4 w-4 shrink-0" />
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
            </div>
          </TableCell>
          <TableCell
            v-for="stat in trackerStats(tracker)"
            :key="stat.label"
            class="w-px whitespace-nowrap text-right tabular-nums text-white in-[.light-mode]:text-black"
          >
            {{ stat.value }}
          </TableCell>
          <TableCell class="w-px" @click.stop>
            <ItemActions :actions="trackerActions(tracker)" :disabled="trackerSelection.count > 0" />
          </TableCell>
        </TableRow>
      </TableBody>
    </Table>

    <div v-else :class="TILE_GRID">
      <Card
        v-for="tracker in menuTrackers"
        :key="tracker.id"
        v-bind="trackerSelection.itemProps(tracker, () => showTracker(tracker.id))"
        class="glass-rise glass-hoverable hover:-translate-y-0.5 cursor-pointer gap-2 py-3"
        role="button"
        tabindex="0"
        :aria-label="`${trackerSelection.count ? 'Select' : 'Open'} ${tracker.name}`"
        :data-state="trackerSelection.isSelected(tracker) ? 'selected' : undefined"
      >
        <CardHeader class="items-center px-4">
          <div class="flex items-center gap-2.5">
            <Checkbox
              :checked="trackerSelection.isSelected(tracker)"
              :aria-label="`Select ${tracker.name}`"
              title="Select"
              @click.stop="(event: MouseEvent) => trackerSelection.toggle(tracker, event.shiftKey)"
            />
            <IconImage :src="sourceIconUrl(tracker.source_key)" class="h-4 w-4 shrink-0" />
          </div>
          <CardAction class="self-center" @click.stop>
            <ItemActions :actions="trackerActions(tracker)" :disabled="trackerSelection.count > 0" />
          </CardAction>
        </CardHeader>

        <CardContent class="gap-1 px-4">
          <CardTitle class="flex items-center gap-1.5">
            <span class="min-w-0 truncate" :title="tracker.name">{{ tracker.name }}</span>
            <Button
              as="a"
              variant="ghost"
              size="sm"
              class="-my-1.5"
              :href="tracker.source_url"
              target="_blank"
              rel="noopener noreferrer"
              :title="tracker.source_url"
              aria-label="Open creator page"
              @click.stop
            >
              <template #icon>
                <IconLink aria-hidden="true" />
              </template>
            </Button>
            <span v-if="trackerStatus(tracker)" class="shrink-0 text-xs font-normal text-white/60 in-[.light-mode]:text-black/60">
              {{ trackerStatus(tracker) }}
            </span>
          </CardTitle>
          <CardDescription class="flex flex-wrap items-center gap-x-4 gap-y-1">
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
          </CardDescription>
          <div v-if="tracker.last_error" class="mt-1 wrap-break-word whitespace-pre-line text-sm text-destructive">
            {{ tracker.last_error }}
          </div>
        </CardContent>
      </Card>
    </div>

    <Dialog
      :open="Boolean(openTracker || newTrackerUrl)"
      :title="openTracker?.name || newTrackerUrl || 'Tracker'"
      hide-title
      :content-class="`fixed left-1/2 top-1/2 z-70 flex ${openTracker ? 'h-[90dvh]' : 'max-h-[90dvh]'} w-[min(900px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none`"
      @update:open="(open) => !open && closeTracker()"
      @open-auto-focus="focusApply"
    >
      <template v-if="openTracker || newTrackerUrl">
        <header class="glass-chrome flex shrink-0 flex-col gap-1.5 rounded-none border-0 border-b border-(--glass-border) shadow-none py-4 pl-5 pr-14 sm:pl-6">
          <div class="flex min-w-0 items-center gap-2 text-lg font-semibold">
            <IconImage v-if="openTracker" :src="sourceIconUrl(openTracker.source_key)" class="h-5 w-5 shrink-0" />
            <span class="truncate">{{ openTracker?.name || newTrackerUrl }}</span>
            <Button
              as="a"
              variant="ghost"
              size="sm"
              :href="openTracker?.source_url || newTrackerUrl"
              target="_blank"
              rel="noopener noreferrer"
              :title="openTracker?.source_url || newTrackerUrl"
              aria-label="Open creator page"
            >
              <template #icon>
                <IconLink aria-hidden="true" />
              </template>
            </Button>
            <span v-if="openTracker && trackerStatus(openTracker)" class="shrink-0 text-xs font-normal text-white/60 in-[.light-mode]:text-black/60">
              {{ trackerStatus(openTracker) }}
            </span>
          </div>
          <div v-if="openTracker" class="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
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
          <div v-if="openTracker?.last_error" class="wrap-break-word whitespace-pre-line text-sm text-destructive">
            {{ openTracker.last_error }}
          </div>
        </header>

        <div ref="itemsScroll" class="min-h-0 flex-1 overflow-y-auto p-5 sm:p-6 flex flex-col gap-6">
          <DownloadFields
            :selection="draftSelection"
            :options="qualityOptions"
            :post-processing="draftPostProcessing"
            :capabilities="draftCapabilities"
            @update:mode="setDraftMode"
            @update:field="setDraftField"
            @update:post-processing="setDraftPostProcessing"
          />

          <FieldSet v-if="openTracker">
            <FieldLegend variant="divider">Items</FieldLegend>
            <HistoryToolbar hide-platform :selection="itemSelection" />
            <!-- At least one view tall, so a filter, search or view swap never scrolls the toolbar away. -->
            <TaskCollection
              :style="{ minHeight: `${itemsViewHeight}px` }"
              :tasks="trackerTasks"
              :view-mode="viewMode"
              :selection="itemSelection"
              :loading="trackerHistoryLoading"
              page-kind="trackers"
              :source-profiles="sourceProfiles"
              :error-message="trackerHistoryError"
              :has-more="trackerHistoryHasMore"
              :fetching-more="trackerHistoryFetchingMore"
              @load-more="loadMoreTrackerHistory"
            />
          </FieldSet>
        </div>

        <DialogFooter class="flex-row flex-wrap items-center justify-between gap-3 sm:justify-between">
          <div class="flex flex-wrap items-center gap-3">
            <SelectionBar v-if="itemSelection.count" :selection="itemSelection" :actions="itemBatch" />
            <Combobox
              v-else
              :model-value="draftInterval"
              :items="TRACKER_INTERVALS"
              aria-label="Check interval"
              placeholder="Select..."
              empty-text="No intervals."
              @update:model-value="setDraftInterval"
            />
          </div>
          <div v-if="!itemSelection.count" class="flex items-center gap-2">
            <Button variant="ghost" type="button" @click="closeTracker">Cancel</Button>
            <Button ref="applyButton" variant="primary" type="button" :disabled="saving" @click="applyTracker">Apply</Button>
          </div>
        </DialogFooter>
      </template>
    </Dialog>

    <Dialog
      :open="deletingTrackers.length > 0"
      :title="deleteTitle"
      description="Checks stop and queued downloads are removed."
      content-class="fixed left-1/2 top-1/2 z-70 flex w-[min(480px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
      @update:open="(open) => !open && (deletingTrackers = [])"
    >
      <div class="px-5 py-4 sm:px-6">
        <FieldLabel class="cursor-pointer items-center gap-2">
          <Checkbox :checked="deleteFiles" @update:checked="(value: boolean) => (deleteFiles = value)" />
          <span>Also delete downloaded files</span>
        </FieldLabel>
      </div>
      <DialogFooter>
        <Button variant="ghost" type="button" @click="deletingTrackers = []">Cancel</Button>
        <Button variant="destructive" type="button" @click="confirmDelete">
          <template #icon>
            <component :is="ACTION_ICONS.delete" aria-hidden="true" />
          </template>
          Delete
        </Button>
      </DialogFooter>
    </Dialog>
  </section>
</template>
