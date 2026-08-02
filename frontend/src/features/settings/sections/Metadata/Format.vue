<script setup lang="ts">
import { reactive, ref } from "vue";
import IconAdd from "~icons/material-symbols/add";
import IconDrag from "~icons/material-symbols/drag-indicator";
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
import { displayUrlTemplate } from "@/utils/dashboard";
import { useSettingsContext } from "@/features/settings/context";
import { Card } from "@/components/ui/card";

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
  <div class="py-2">
    <div class="flex w-full items-center gap-2">
      <Input
        id="formatLearnInput"
        v-model="link"
        type="text"
        inputmode="url"
        placeholder="Paste a link"
        @keydown.enter.prevent="submit"
      />
      <Button
        variant="primary"
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
            class="w-5 h-5 animate-spin"
            aria-hidden="true"
          />
          <IconAdd v-else class="w-5 h-5" aria-hidden="true" />
        </template>
      </Button>
    </div>
  </div>

  <Card v-if="editableSourceProfiles.length === 0" class="mt-3 px-6">
    <p class="text-[0.8125rem] text-muted-foreground">
      No sources yet.
    </p>
  </Card>

  <Accordion v-else v-model="open" type="multiple" class="w-full">
    <AccordionItem
      v-for="site in editableSourceProfiles"
      :key="site.key"
      :value="site.key"
    >
      <AccordionTrigger>
        {{ site.label }}
      </AccordionTrigger>

      <AccordionContent>
        <Card v-if="!templatesFor(site.key).length" class="px-6">
          <p class="text-[0.8125rem] text-muted-foreground">
            No format learned yet.
          </p>
        </Card>

        <ul v-else class="flex flex-col gap-1">
          <li
            v-for="(template, index) in templatesFor(site.key)"
            :key="template"
            draggable="true"
            class="flex items-center gap-1.5 rounded-md px-1 py-1 transition-colors cursor-grab active:cursor-grabbing"
            :class="[
              isDragging(site.key, index) ? 'opacity-40' : '',
              isDropTarget(site.key, index) ? 'bg-accent/15' : '',
            ]"
            @dragstart="onDragStart(site.key, index, $event)"
            @dragover.prevent="onDragOver(site.key, index)"
            @drop.prevent="onDrop(site.key, index)"
            @dragend="resetDrag"
          >
            <IconDrag class="w-4 h-4 shrink-0 opacity-50" aria-hidden="true" />
            <span
              class="font-mono text-[0.8125rem] flex-1 min-w-0 wrap-anywhere"
            >
              {{ displayUrlTemplate(template) }}
            </span>
            <Button
              variant="destructive"
              type="button"
              title="Delete format"
              aria-label="Delete format"
              @click="deleteTemplate(site.key, index)"
            >
              <template #icon>
                <IconTrash class="w-4 h-4" aria-hidden="true" />
              </template>
            </Button>
          </li>
        </ul>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
