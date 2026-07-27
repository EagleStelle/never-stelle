<script setup lang="ts">
import { reactive } from "vue";
import IconDrag from "~icons/material-symbols/drag-indicator";
import IconSearch from "~icons/material-symbols/search";
import IconSpinner from "~icons/material-symbols/sync";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  useFieldsSettings,
  type FieldRole,
} from "@/features/settings/composables/useFieldsSettings";
import { useSettingsContext } from "@/features/settings/context";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

const ROLES: { key: FieldRole; label: string }[] = [
  { key: "username", label: "Username" },
  { key: "nickname", label: "Nickname" },
  { key: "title", label: "Title" },
];

const {
  settings,
  settingsDraft,
  learnedFormatsDraft,
  editableSourceProfiles,
  probeFields,
} = useSettingsContext();
const {
  probes,
  fieldListItems,
  reorderField,
  resetRole,
  isConfigured,
  runProbe,
} = useFieldsSettings(
  settingsDraft,
  settings,
  learnedFormatsDraft,
  editableSourceProfiles,
  {
    probeFields,
  },
);

// Native drag-and-drop reordering, scoped to one (source, role) list at a time.
const drag = reactive<{
  key: string;
  role: FieldRole;
  from: number;
  over: number;
}>({
  key: "",
  role: "username",
  from: -1,
  over: -1,
});

function onDragStart(
  key: string,
  role: FieldRole,
  index: number,
  event: DragEvent,
): void {
  drag.key = key;
  drag.role = role;
  drag.from = index;
  drag.over = index;
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", String(index));
  }
}

function onDragOver(key: string, role: FieldRole, index: number): void {
  if (drag.key === key && drag.role === role) drag.over = index;
}

function onDrop(key: string, role: FieldRole, index: number): void {
  if (drag.key === key && drag.role === role) {
    reorderField(key, role, drag.from, index);
  }
  resetDrag();
}

function resetDrag(): void {
  drag.key = "";
  drag.from = -1;
  drag.over = -1;
}

function isDragging(key: string, role: FieldRole, index: number): boolean {
  return drag.key === key && drag.role === role && drag.from === index;
}

function isDropTarget(key: string, role: FieldRole, index: number): boolean {
  return (
    drag.key === key &&
    drag.role === role &&
    drag.over === index &&
    drag.from !== index
  );
}
</script>

<template>
  <Accordion type="multiple" class="w-full">
    <AccordionItem
      v-for="site in editableSourceProfiles"
      :key="site.key"
      :value="site.key"
    >
      <AccordionTrigger>
        {{ site.label }}
      </AccordionTrigger>
      <AccordionContent>
        <div class="flex flex-col gap-[0.85rem]">
          <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
            <Label class="sm:w-40 sm:shrink-0">Probe URL</Label>
            <div class="flex items-center gap-2 w-full sm:flex-auto">
              <Input
                :id="`${site.key}FieldsProbeInput`"
                v-model="probes[site.key].url"
                data-settings-system
                type="text"
                inputmode="url"
                aria-label="Probe URL"
                placeholder="Paste a link"
                class="flex-1"
                @keydown.enter.prevent="runProbe(site.key)"
              />
              <Button
                class="shrink-0"
                variant="primary"
                type="button"
                aria-label="Test"
                title="Test"
                :disabled="probes[site.key].loading"
                :aria-busy="probes[site.key].loading"
                @click="runProbe(site.key)"
              >
                <template #icon>
                  <IconSpinner
                    v-if="probes[site.key].loading"
                    class="w-4 h-4 animate-spin"
                    aria-hidden="true"
                  />
                  <IconSearch v-else class="w-4 h-4" aria-hidden="true" />
                </template>
              </Button>
            </div>
          </div>

          <Card v-if="probes[site.key].message" class="px-6">
            <p class="text-[0.8125rem] text-muted-foreground">
              {{ probes[site.key].message }}
            </p>
          </Card>

          <Table
            v-if="probes[site.key].fields.length"
            class="w-full table-fixed text-[0.8125rem]"
          >
            <TableHeader>
              <TableRow>
                <TableHead
                  class="w-36 sm:w-44 text-[0.68rem] uppercase tracking-wider text-white/45 in-[.light-mode]:text-black/45"
                >
                  Field
                </TableHead>
                <TableHead
                  class="text-[0.68rem] uppercase tracking-wider text-white/45 in-[.light-mode]:text-black/45"
                >
                  Sample
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <TableRow
                v-for="result in probes[site.key].fields"
                :key="result.field"
              >
                <TableCell
                  class="w-36 sm:w-44 max-w-[9rem] sm:max-w-[11rem] font-mono align-top"
                >
                  <span class="block truncate" :title="result.field">
                    {{ result.field }}
                  </span>
                </TableCell>
                <TableCell class="min-w-0 align-top">
                  <span
                    class="block min-w-0 break-words whitespace-pre-wrap leading-normal [word-break:break-word] max-h-32 overflow-y-auto"
                    :title="result.value"
                  >
                    {{ result.value }}
                  </span>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>

          <div class="grid grid-cols-1 sm:grid-cols-3 gap-x-4 gap-y-3 mt-3">
            <div
              v-for="role in ROLES"
              :key="role.key"
              class="flex flex-col gap-2"
            >
              <div class="flex items-center justify-between gap-2 min-h-6">
                <Label>{{ role.label }}</Label>
                <button
                  v-if="isConfigured(site.key, role.key)"
                  type="button"
                  class="text-xs opacity-70 hover:opacity-100 transition-opacity"
                  title="Restore the default order"
                  @click="resetRole(site.key, role.key)"
                >
                  Reset
                </button>
              </div>
              <ul class="flex flex-col gap-1">
                <li
                  v-for="(field, index) in fieldListItems(site.key, role.key)"
                  :key="field.key"
                  draggable="true"
                  class="flex items-center gap-1.5 rounded-md px-1 py-1 transition-colors cursor-grab active:cursor-grabbing"
                  :class="[
                    isDragging(site.key, role.key, index) ? 'opacity-40' : '',
                    isDropTarget(site.key, role.key, index)
                      ? 'bg-accent/15'
                      : '',
                  ]"
                  @dragstart="onDragStart(site.key, role.key, index, $event)"
                  @dragover.prevent="onDragOver(site.key, role.key, index)"
                  @drop.prevent="onDrop(site.key, role.key, index)"
                  @dragend="resetDrag"
                >
                  <IconDrag
                    class="w-4 h-4 shrink-0 opacity-50"
                    aria-hidden="true"
                  />
                  <span
                    class="font-mono text-[0.8125rem] flex-1 min-w-0 wrap-anywhere"
                  >
                    {{ field.label }}
                  </span>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
