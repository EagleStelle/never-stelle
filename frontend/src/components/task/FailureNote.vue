<script setup lang="ts">
import { computed, ref, useTemplateRef } from "vue";
import IconCopy from "~icons/material-symbols/content-copy-rounded";
import IconOpen from "~icons/material-symbols/keyboard-arrow-right";

import { Button } from "@/components/ui/button";
import { DialogFooter, DialogShell as Dialog } from "@/components/ui/dialog";
import { useSonner } from "@/composables/useSonner";
import { COUNT_ICONS } from "@/ui";
import { copyText } from "@/utils/clipboard";

// Why an item failed: its telling line, opening the full engine output.
const props = defineProps<{ text: string; subject?: string; dense?: boolean }>();

// An engine's report of what failed, as gallery-dl's "[site][error]" or yt-dlp's "ERROR:".
const ENGINE_ERROR_RE = /^(?:\[[^\]]+\]\[error\]|ERROR:)\s*/;
const TAGS_RE = /^(?:\[[^\]]+\]\s*)+/;

const logText = computed(() => props.text.trim());
const reason = computed(() => {
  const lines = logText.value.split("\n").map((line) => line.trim()).filter(Boolean);
  const engineLine = lines.find((line) => ENGINE_ERROR_RE.test(line));
  return engineLine ? engineLine.replace(ENGINE_ERROR_RE, "").replace(TAGS_RE, "") : (lines[0] ?? "");
});

const open = ref(false);
const FailedIcon = COUNT_ICONS.failed;

const log = useTemplateRef<HTMLElement>("log");
const { toast } = useSonner();

async function copyLog(): Promise<void> {
  const host = log.value?.parentElement;
  if (host && (await copyText(logText.value, host))) {
    toast("Log copied.");
  } else {
    toast("Could not copy.", "error");
  }
}
</script>

<template>
  <div class="min-w-0" @click.stop>
    <button
      type="button"
      aria-haspopup="dialog"
      class="group flex max-w-full min-w-0 cursor-pointer items-start gap-1.5 rounded-sm text-left text-white/85 hover:text-white in-[.light-mode]:text-black/80 in-[.light-mode]:hover:text-black"
      :class="props.dense ? '-my-1 py-1 text-xs' : '-my-0.5 py-0.5 text-sm'"
      @click="open = true"
    >
      <FailedIcon
        class="shrink-0 text-destructive-ink"
        :class="props.dense ? 'mt-px size-3.5' : 'mt-0.5 size-4'"
        aria-hidden="true"
      />
      <span class="min-w-0 wrap-anywhere" :class="props.dense ? 'truncate' : 'line-clamp-2'">{{ reason }}</span>
      <IconOpen
        class="shrink-0 opacity-70 transition-transform duration-200 ease-glass group-hover:translate-x-0.5 motion-reduce:transition-none"
        :class="props.dense ? 'size-3.5' : 'mt-0.5 size-4'"
        aria-hidden="true"
      />
    </button>

    <Dialog
      :open="open"
      title="Error log"
      content-class="fixed left-1/2 top-1/2 z-70 flex max-h-[85dvh] w-[min(720px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
      @update:open="(value) => (open = value)"
    >
      <div class="flex min-h-0 flex-1 flex-col gap-3 px-5 pt-2 pb-5 sm:px-6">
        <p
          v-if="props.subject"
          class="shrink-0 truncate pr-8 text-sm text-white/60 in-[.light-mode]:text-black/60"
          :title="props.subject"
        >
          {{ props.subject }}
        </p>
        <pre
          ref="log"
          tabindex="0"
          aria-label="Full log"
          class="min-h-0 flex-1 cursor-text overflow-auto rounded-lg bg-black/30 px-4 py-3 font-mono text-xs leading-relaxed whitespace-pre-wrap wrap-anywhere text-white/85 select-text in-[.light-mode]:bg-black/5 in-[.light-mode]:text-black/80"
        >{{ logText }}</pre>
      </div>
      <DialogFooter>
        <Button type="button" @click="copyLog">
          <template #icon>
            <IconCopy aria-hidden="true" />
          </template>
          Copy log
        </Button>
      </DialogFooter>
    </Dialog>
  </div>
</template>
