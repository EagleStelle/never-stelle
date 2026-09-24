<script setup lang="ts">
import { reactive, ref } from "vue";
import IconAdd from "~icons/material-symbols/add";
import IconDrag from "~icons/material-symbols/drag-indicator";
import IconInfo from "~icons/material-symbols/info-outline";
import IconSpinner from "~icons/material-symbols/sync";
import IconTrash from "~icons/material-symbols/delete";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import type { LearnedFormat } from "@/types";
import { displayUrlTemplate, sourceIconUrl, templateParts } from "@/utils/dashboard";
import { useSettingsContext } from "@/features/settings/context";

const {
  learnedFormatsDraft,
  editableSourceProfiles,
  learnFormat,
  reorderFormatTemplates,
} = useSettingsContext();

const link = ref("");
const learning = ref(false);
// Open items in the multiple-select accordion; a just-learned source is expanded.
const open = ref<string[]>([]);

function learnedFormat(key: string): LearnedFormat | undefined {
  return learnedFormatsDraft?.[key];
}

function templatesFor(key: string): string[] {
  return learnedFormat(key)?.templates || [];
}

async function submit(): Promise<void> {
  const url = link.value.trim();
  if (!url || learning.value) return;
  learning.value = true;
  try {
    const key = await learnFormat(url);
    if (key) {
      link.value = "";
      if (!open.value.includes(key)) open.value.push(key);
    }
  } finally {
    learning.value = false;
  }
}

function deleteTemplate(key: string, index: number): void {
  const next = templatesFor(key).filter((_, i) => i !== index);
  void reorderFormatTemplates(key, next);
}

// Native drag-and-drop reordering, scoped to one source's template list at a time.
// Order is significant: reconstruct/scan try templates top-first.
const drag = reactive<{ key: string; from: number; over: number }>({
  key: "",
  from: -1,
  over: -1,
});

function onDragStart(key: string, index: number, event: DragEvent): void {
  drag.key = key;
  drag.from = index;
  drag.over = index;
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", String(index));
  }
}

function onDragOver(key: string, index: number): void {
  if (drag.key === key) drag.over = index;
}

function onDrop(key: string, index: number): void {
  if (drag.key === key && drag.from !== index) {
    const next = [...templatesFor(key)];
    const [moved] = next.splice(drag.from, 1);
    next.splice(index, 0, moved);
    void reorderFormatTemplates(key, next);
  }
  resetDrag();
}

function resetDrag(): void {
  drag.key = "";
  drag.from = -1;
  drag.over = -1;
}

function isDragging(key: string, index: number): boolean {
  return drag.key === key && drag.from === index;
}

function isDropTarget(key: string, index: number): boolean {
  return drag.key === key && drag.over === index && drag.from !== index;
}
</script>

<template>
  <div class="flex w-full items-center gap-2 pb-4">
    <Input
      id="formatLearnInput"
      v-model="link"
      class="flex-1"
      type="text"
      inputmode="url"
      placeholder="Paste a link"
      @keydown.enter.prevent="submit"
    />
    <Button
      variant="primary"
      size="icon"
      type="button"
      aria-label="Learn format"
      title="Learn format"
      :disabled="learning"
      :aria-busy="learning"
      @click="submit"
    >
      <template #icon>
        <IconSpinner
          v-if="learning"
          class="animate-spin"
          aria-hidden="true"
        />
        <IconAdd v-else aria-hidden="true" />
      </template>
    </Button>
  </div>

  <p
    v-if="editableSourceProfiles.length === 0"
    class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
  >
    <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
    No sources yet.
  </p>

  <Accordion v-else v-model="open" type="multiple" class="w-full">
    <AccordionItem
      v-for="site in editableSourceProfiles"
      :key="site.key"
      :value="site.key"
    >
      <AccordionTrigger :image="sourceIconUrl(site.key)">
        {{ site.label }}
      </AccordionTrigger>

      <AccordionContent>
        <p
          v-if="!templatesFor(site.key).length"
          class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
        >
          <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
          Download once from this source to learn its URL format.
        </p>

        <ul v-else class="flex flex-col">
          <li
            v-for="(template, index) in templatesFor(site.key)"
            :key="template"
            draggable="true"
            class="flex items-center gap-2 rounded-lg px-2 py-1.5 transition-colors duration-200 cursor-grab hover:bg-white/5 active:cursor-grabbing in-[.light-mode]:hover:bg-black/4"
            :class="[
              isDragging(site.key, index) ? 'opacity-40' : '',
              isDropTarget(site.key, index) ? 'bg-accent/15' : '',
            ]"
            @dragstart="onDragStart(site.key, index, $event)"
            @dragover.prevent="onDragOver(site.key, index)"
            @drop.prevent="onDrop(site.key, index)"
            @dragend="resetDrag"
          >
            <IconDrag class="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
            <span class="min-w-0 flex-1 font-mono text-[0.8125rem] leading-snug wrap-anywhere">
              <span
                v-for="(part, partIndex) in templateParts(displayUrlTemplate(template))"
                :key="partIndex"
                :class="part.token && 'text-accent-ink'"
                >{{ part.text }}</span
              >
            </span>
            <Button
              variant="destructive-ghost"
              size="icon-sm"
              type="button"
              title="Delete format"
              aria-label="Delete format"
              @click="deleteTemplate(site.key, index)"
            >
              <template #icon>
                <IconTrash aria-hidden="true" />
              </template>
            </Button>
          </li>
        </ul>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
