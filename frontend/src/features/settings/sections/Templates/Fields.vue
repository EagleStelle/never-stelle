<script setup lang="ts">
import { reactive } from "vue";
import IconDrag from "~icons/material-symbols/drag-indicator";
import IconSearch from "~icons/material-symbols/search";
import IconSpinner from "~icons/material-symbols/sync";

import { Button } from "../../../../components/ui/button";
import { Checkbox } from "../../../../components/ui/checkbox";
import { Input } from "../../../../components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../../../../components/ui/table";
import {
  useFieldsSettings,
  type CreatorRole,
} from "../../composables/useFieldsSettings";
import { useSettingsContext } from "../../context";
import SettingsRow from "../../SettingsRow.vue";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "../../../../components/ui/accordion";

const ROLES: { key: CreatorRole; label: string }[] = [
  { key: "username", label: "Username" },
  { key: "nickname", label: "Nickname" },
];

const { settings, settingsDraft, editableSourceProfiles } =
  useSettingsContext();
const {
  probes,
  cleanupRules,
  titleLengthRule,
  fieldList,
  reorderField,
  resetRole,
  isConfigured,
  ruleEnabled,
  setRule,
  maxChars,
  setMaxChars,
  runProbe,
} = useFieldsSettings(settingsDraft, settings, editableSourceProfiles);

// Native drag-and-drop reordering, scoped to one (source, role) list at a time.
const drag = reactive<{
  key: string;
  role: CreatorRole;
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
  role: CreatorRole,
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

function onDragOver(key: string, role: CreatorRole, index: number): void {
  if (drag.key === key && drag.role === role) drag.over = index;
}

function onDrop(key: string, role: CreatorRole, index: number): void {
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

function isDragging(key: string, role: CreatorRole, index: number): boolean {
  return drag.key === key && drag.role === role && drag.from === index;
}

function isDropTarget(key: string, role: CreatorRole, index: number): boolean {
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
          <SettingsRow label="Probe URL">
            <div class="flex items-center gap-2 w-full">
              <Input
                :id="`${site.key}FieldsProbeInput`"
                v-model="probes[site.key].url"
                type="text"
                inputmode="url"
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
          </SettingsRow>

          <p
            v-if="probes[site.key].message"
            class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55"
          >
            {{ probes[site.key].message }}
          </p>

          <Table v-if="probes[site.key].fields.length" class="text-[0.8125rem]">
            <TableHeader>
              <TableRow>
                <TableHead
                  class="w-48 text-[0.68rem] uppercase tracking-wider text-white/45 in-[.light-mode]:text-black/45"
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
                <TableCell class="w-48 max-w-48 font-mono">
                  <span class="block truncate" :title="result.field">
                    {{ result.field }}
                  </span>
                </TableCell>
                <TableCell>
                  <span
                    class="block min-w-0 wrap-anywhere"
                    :title="result.value"
                  >
                    {{ result.value }}
                  </span>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-x-4 gap-y-3">
            <div
              v-for="role in ROLES"
              :key="role.key"
              class="flex flex-col gap-2"
            >
              <div class="flex items-center justify-between gap-2 min-h-6">
                <h3
                  class="text-xs font-bold text-white/50 in-[.light-mode]:text-black/50 uppercase tracking-wider"
                >
                  {{ role.label }}
                </h3>
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
                  v-for="(field, index) in fieldList(site.key, role.key)"
                  :key="field"
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
                    class="font-mono text-[0.8125rem] flex-1 min-w-0 truncate"
                  >
                    {{ field }}
                  </span>
                </li>
              </ul>
            </div>
          </div>

          <div class="flex flex-col gap-2">
            <h3
              class="text-xs font-bold text-white/50 in-[.light-mode]:text-black/50 uppercase tracking-wider"
            >
              Cleanup
            </h3>
            <label
              v-for="rule in cleanupRules"
              :key="rule.key"
              class="flex items-center gap-2 cursor-pointer select-none text-sm"
            >
              <Checkbox
                :checked="ruleEnabled(site.key, rule)"
                @update:checked="
                  (v: boolean) => setRule(site.key, rule, Boolean(v))
                "
              />
              <span>{{ rule.label }}</span>
            </label>
            <SettingsRow
              v-if="ruleEnabled(site.key, titleLengthRule)"
              label="Character limit"
            >
              <div class="flex items-center gap-2">
                <Input
                  type="number"
                  min="12"
                  :disabled="!ruleEnabled(site.key, titleLengthRule)"
                  :model-value="String(maxChars(site.key))"
                  class="w-24 shrink-0"
                  @update:model-value="
                    (v: string | number) => setMaxChars(site.key, Number(v))
                  "
                />
                <span class="text-white/55 in-[.light-mode]:text-black/55">
                  characters
                </span>
              </div>
            </SettingsRow>
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
