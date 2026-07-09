<script setup lang="ts">
import { computed, reactive, ref, watch, watchEffect } from "vue";
import { Dialog } from "../../components/ui/dialog";
import { Button } from "../../components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../../components/ui/table";
import type { PlaylistEntry } from "../../types";

const props = defineProps<{
  open: boolean;
  title: string;
  entries: PlaylistEntry[];
}>();

const emit = defineEmits<{
  "update:open": [open: boolean];
  confirm: [urls: string[]];
}>();

const openModel = computed({
  get: () => props.open,
  set: (value) => emit("update:open", value),
});

const selected = reactive(new Set<number>());
const headerCheckbox = ref<HTMLInputElement | null>(null);

const selectedCount = computed(() => selected.size);
const allSelected = computed(
  () => props.entries.length > 0 && selected.size === props.entries.length,
);

function selectAll(): void {
  selected.clear();
  for (const entry of props.entries) selected.add(entry.index);
}

// Default to the whole playlist checked; the user unchecks what they skip.
watch(
  () => props.entries,
  () => selectAll(),
  { immediate: true },
);
watch(
  () => props.open,
  (open) => {
    if (open) selectAll();
  },
);

// `indeterminate` is a DOM property, not an attribute, so set it imperatively.
watchEffect(() => {
  if (headerCheckbox.value) {
    headerCheckbox.value.indeterminate =
      selected.size > 0 && !allSelected.value;
  }
});

function toggle(index: number): void {
  if (selected.has(index)) selected.delete(index);
  else selected.add(index);
}

function toggleAll(): void {
  if (allSelected.value) selected.clear();
  else selectAll();
}

function confirm(): void {
  const urls = props.entries
    .filter((entry) => selected.has(entry.index))
    .map((entry) => entry.url);
  if (urls.length > 0) emit("confirm", urls);
}

function formatDuration(seconds: number | null): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) return "—";
  const total = Math.floor(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  const mm = hours > 0 ? String(minutes).padStart(2, "0") : String(minutes);
  return hours > 0
    ? `${hours}:${mm}:${String(secs).padStart(2, "0")}`
    : `${mm}:${String(secs).padStart(2, "0")}`;
}
</script>

<template>
  <Dialog
    v-model:open="openModel"
    :title="title"
    description="Choose which videos to download."
    content-class="fixed left-1/2 top-1/2 z-70 flex max-h-[85vh] w-[min(860px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
  >
    <div class="flex min-h-0 flex-1 flex-col">
      <div
        class="flex shrink-0 items-center justify-between gap-2 px-5 pb-3 pt-2 text-sm sm:px-6"
      >
        <label class="inline-flex cursor-pointer items-center gap-2">
          <input
            ref="headerCheckbox"
            type="checkbox"
            class="h-4 w-4 cursor-pointer accent-(--accent)"
            :checked="allSelected"
            @change="toggleAll"
          />
          <span>Select all</span>
        </label>
        <span class="text-white/60 in-[.light-mode]:text-black/60">
          {{ selectedCount }} of {{ entries.length }} selected
        </span>
      </div>

      <div class="min-h-0 flex-1 overflow-y-auto px-5 sm:px-6">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead class="w-10"></TableHead>
              <TableHead class="w-10 text-right">#</TableHead>
              <TableHead>Title</TableHead>
              <TableHead class="hidden sm:table-cell">Creator</TableHead>
              <TableHead class="w-16 text-right">Length</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow
              v-for="entry in entries"
              :key="entry.index"
              class="cursor-pointer"
              @click="toggle(entry.index)"
            >
              <TableCell>
                <input
                  type="checkbox"
                  class="h-4 w-4 cursor-pointer accent-(--accent)"
                  :checked="selected.has(entry.index)"
                  :aria-label="`Select ${entry.title}`"
                  @click.stop
                  @change="toggle(entry.index)"
                />
              </TableCell>
              <TableCell class="text-right tabular-nums text-white/60 in-[.light-mode]:text-black/60">
                {{ entry.index }}
              </TableCell>
              <TableCell class="max-w-0">
                <span class="block truncate">{{ entry.title }}</span>
              </TableCell>
              <TableCell class="hidden max-w-0 sm:table-cell">
                <span class="block truncate text-white/70 in-[.light-mode]:text-black/70">
                  {{ entry.creator || "—" }}
                </span>
              </TableCell>
              <TableCell class="text-right tabular-nums text-white/60 in-[.light-mode]:text-black/60">
                {{ formatDuration(entry.duration) }}
              </TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </div>

      <div
        class="flex shrink-0 items-center justify-end gap-2 border-t border-(--glass-border) px-5 py-4 sm:px-6"
      >
        <Button
          size="lg"
          type="button"
          class="bg-transparent! text-white! in-[.light-mode]:text-black! hover:bg-(--glass-hover)!"
          @click="openModel = false"
        >
          Cancel
        </Button>
        <Button
          variant="primary"
          size="lg"
          type="button"
          :disabled="selectedCount === 0"
          @click="confirm"
        >
          Download {{ selectedCount }}
        </Button>
      </div>
    </div>
  </Dialog>
</template>
