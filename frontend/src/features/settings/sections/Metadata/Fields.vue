<script setup lang="ts">
import { reactive } from "vue";
import IconDrag from "~icons/material-symbols/drag-indicator";
import IconInfo from "~icons/material-symbols/info-outline";
import IconResolve from "~icons/material-symbols/cloud-sync";
import IconSearch from "~icons/material-symbols/search";
import IconSpinner from "~icons/material-symbols/sync";

import { Button } from "@/components/ui/button";
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";
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
  FIELD_ROLE_DEFS,
  useFieldsSettings,
  type FieldRole,
} from "@/features/settings/composables/useFieldsSettings";
import { useSettingsContext } from "@/features/settings/context";
import { sourceIconUrl } from "@/utils/dashboard";
import { Label } from "@/components/ui/label";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

const {
  settings,
  settingsDraft,
  learnedFormatsDraft,
  editableSourceProfiles,
  probeFields,
  renameCount,
  renameRunning,
  openRename,
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

function filledRoles(key: string) {
  return FIELD_ROLE_DEFS.filter((role) => fieldListItems(key, role.key).length);
}
</script>

<template>
  <Accordion type="multiple" class="w-full">
    <AccordionItem
      v-for="site in editableSourceProfiles"
      :key="site.key"
      :value="site.key"
    >
      <AccordionTrigger :image="sourceIconUrl(site.key)">
        {{ site.label }}
      </AccordionTrigger>
      <AccordionContent>
        <div class="flex flex-col gap-4">
          <Field>
            <FieldLabel :for="`${site.key}FieldsProbeInput`">
              Probe URL
            </FieldLabel>
            <FieldContent class="flex-row items-center gap-2">
              <Input
                :id="`${site.key}FieldsProbeInput`"
                v-model="probes[site.key].url"
                data-settings-system
                type="text"
                inputmode="url"
                placeholder="Paste a link"
                class="flex-1"
                @keydown.enter.prevent="runProbe(site.key)"
              />
              <Button
                variant="primary"
                size="icon"
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
            </FieldContent>
          </Field>

          <p
            v-if="
              probes[site.key].message ||
              (!probes[site.key].fields.length && !filledRoles(site.key).length)
            "
            class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
          >
            <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
            {{
              probes[site.key].message ||
              "Test a link from this source to list the fields it carries."
            }}
          </p>

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
                  class="w-36 sm:w-44 max-w-36 sm:max-w-44 font-mono align-top"
                >
                  <span class="block truncate" :title="result.field">
                    {{ result.field }}
                  </span>
                </TableCell>
                <TableCell class="min-w-0 align-top">
                  <span
                    class="block min-w-0 wrap-break-word whitespace-pre-wrap leading-normal [word-break:break-word] max-h-32 overflow-y-auto"
                    :title="result.value"
                  >
                    {{ result.value }}
                  </span>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>

          <div v-if="filledRoles(site.key).length" class="flex flex-col gap-3 pt-2">
            <div
              class="grid min-w-0 grid-cols-1 sm:grid-cols-3 gap-x-4 gap-y-3"
            >
              <div
                v-for="role in filledRoles(site.key)"
                :key="role.key"
                class="flex flex-col gap-2"
              >
                <div class="flex items-center justify-between gap-2 min-h-9">
                  <Label>{{ role.label }}</Label>
                  <button
                    v-if="isConfigured(site.key, role.key)"
                    type="button"
                    class="rounded-md px-1.5 py-1 text-xs text-muted-foreground transition-colors duration-200 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
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
                    class="-mx-1 flex items-center gap-1.5 rounded-md px-1 py-1 transition-colors duration-200 cursor-grab hover:bg-white/5 active:cursor-grabbing in-[.light-mode]:hover:bg-black/4"
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
                      class="size-4 shrink-0 text-muted-foreground"
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
            <!-- Full width on phones, where the roles stack in one column. -->
            <div class="flex flex-col sm:flex-row sm:justify-end">
                <Button
                  variant="secondary"
                  size="sm"
                  type="button"
                  title="Resolve platform"
                  :disabled="!renameCount(site.key, 'fields') || renameRunning(site.key, 'fields')"
                  :aria-busy="renameRunning(site.key, 'fields')"
                  @click="openRename(site.key, site.label, 'fields')"
                >
                  <template #icon>
                    <IconResolve
                      aria-hidden="true"
                      :class="{ 'animate-spin': renameRunning(site.key, 'fields') }"
                    />
                  </template>
                  Resolve History
                </Button>
            </div>
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
