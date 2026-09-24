<script setup lang="ts">
import { reactive, ref } from "vue";
import IconDrag from "~icons/material-symbols/drag-indicator";
import IconInfo from "~icons/material-symbols/info-outline";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";

import { Button } from "@/components/ui/button";
import { Field, FieldContent } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import type { CookieFile, CookiePolicyField } from "@/types";
import { COOKIE_POLICY_FIELDS } from "@/features/settings/cookiePolicy";
import { useSettingsContext } from "@/features/settings/context";
import { sourceIconUrl } from "@/utils/dashboard";

const {
  settings,
  settingsDraft,
  editableSourceProfiles,
  cookieStatuses,
  connectCookies,
  removeCookies,
  reorderCookies,
} = useSettingsContext();

const policyEdits = reactive<Record<string, string>>({});

function editKey(key: string, field: CookiePolicyField): string {
  return `${key}:${field}`;
}

// A source with no override of its own follows the global default from the Defaults
// pane, which itself falls back to the built-in.
function policyInherited(field: CookiePolicyField): number {
  const configured = settingsDraft.default_cookie_policy[field];
  return Number(
    configured === undefined || configured === null
      ? settings.cookie_policy_defaults[field]
      : configured,
  );
}

function policyValue(key: string, field: CookiePolicyField): string {
  const editing = policyEdits[editKey(key, field)];
  if (editing !== undefined) return editing;
  const value = settingsDraft.source_cookie_policies[key]?.[field];
  return String(
    value === undefined || value === null ? policyInherited(field) : value,
  );
}

function setPolicyValue(key: string, field: CookiePolicyField, raw: string | number): void {
  if (!settingsDraft.source_cookie_policies[key]) {
    settingsDraft.source_cookie_policies[key] = {};
  }
  // Read back through the reactive proxy so the edit below is tracked.
  const entry = settingsDraft.source_cookie_policies[key];
  const text = String(raw ?? "").trim();
  policyEdits[editKey(key, field)] = text;
  const parsed = Number(text);
  if (!text || (Number.isFinite(parsed) && parsed === policyInherited(field))) {
    delete entry[field];
  } else {
    if (!Number.isFinite(parsed)) return;
    entry[field] = parsed;
  }
}

function endPolicyEdit(key: string, field: CookiePolicyField): void {
  delete policyEdits[editKey(key, field)];
}

// Open items in the multiple-select accordion.
const open = ref<string[]>([]);

function cookiesFor(key: string): CookieFile[] {
  return cookieStatuses.value[key]?.cookies || [];
}

function onCookieFile(site: string, event: Event): void {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0] || null;
  if (file) connectCookies(site, file);
  // Same file twice in a row must still fire change; the pool wants both jars.
  input.value = "";
}

function openPicker(site: string): void {
  document.getElementById(`${site}CookiesInput`)?.click();
}

function deleteCookie(key: string, cookieId: string): void {
  removeCookies(key, cookieId);
}

// Native drag-and-drop reordering, scoped to one source's jar list at a time.
// Order is significant: the rotation starts at the top and walks down.
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
    const next = [...cookiesFor(key)];
    const [moved] = next.splice(drag.from, 1);
    next.splice(index, 0, moved);
    void reorderCookies(
      key,
      next.map((cookie) => cookie.id),
    );
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
  <TooltipProvider>
    <Accordion v-model="open" type="multiple" class="w-full">
      <AccordionItem
        v-for="site in editableSourceProfiles"
        :key="site.key"
        :value="site.key"
      >
        <AccordionTrigger :image="sourceIconUrl(site.key)">
          {{ site.label }}
        </AccordionTrigger>

        <AccordionContent>
          <div class="flex flex-col gap-6">
            <div class="grid grid-cols-1 gap-x-8 gap-y-3 sm:grid-cols-2">
              <Field
                v-for="field in COOKIE_POLICY_FIELDS"
                :key="field.key"
              >
                <!-- The help button cannot sit inside a label, so the cell wraps both. -->
                <div data-slot="field-label" class="flex items-center gap-1.5">
                  <Label :for="`${site.key}Cookie${field.key}`">
                    {{ field.label }}
                  </Label>
                  <Tooltip>
                    <TooltipTrigger as-child>
                      <button
                        type="button"
                        class="-m-1 inline-flex size-6 items-center justify-center rounded-md text-muted-foreground transition-colors duration-200 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
                        :aria-label="`${field.label} help`"
                      >
                        <IconInfo class="size-4" aria-hidden="true" />
                      </button>
                    </TooltipTrigger>
                    <TooltipContent side="top">
                      {{ field.help }}
                    </TooltipContent>
                  </Tooltip>
                </div>
                <FieldContent>
                  <Input
                    :id="`${site.key}Cookie${field.key}`"
                    data-settings-system
                    type="number"
                    :min="field.min"
                    :placeholder="String(policyInherited(field.key))"
                    :model-value="policyValue(site.key, field.key)"
                    @blur="endPolicyEdit(site.key, field.key)"
                    @update:model-value="
                      (value: string | number) => setPolicyValue(site.key, field.key, value)
                    "
                  />
                </FieldContent>
              </Field>
            </div>

            <input
              :id="`${site.key}CookiesInput`"
              data-settings-system
              type="file"
              accept=".txt,.cookies,text/plain"
              class="hidden"
              @change="onCookieFile(site.key, $event)"
            />

            <div class="flex flex-col gap-2">
              <div class="flex min-h-8 items-center justify-between gap-3">
                <h3 class="text-sm font-semibold">Cookie Pool</h3>
                <Button
                  compact
                  variant="secondary"
                  size="sm"
                  type="button"
                  :title="`Upload ${site.label} cookie file`"
                  @click="openPicker(site.key)"
                >
                  <template #icon>
                    <IconUpload aria-hidden="true" />
                  </template>
                  Upload Cookies
                </Button>
              </div>
              <ul
                v-if="cookiesFor(site.key).length"
                class="flex flex-col"
              >
                <li
                  v-for="(cookie, index) in cookiesFor(site.key)"
                  :key="cookie.id"
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
                  <span class="min-w-0 flex-1">
                    <span class="block font-mono text-[0.8125rem] wrap-anywhere">
                      {{ cookie.filename }}
                    </span>
                    <span
                      v-if="cookie.browser"
                      class="block text-xs text-muted-foreground"
                      title="Browser this file acts as"
                    >
                      {{ cookie.browser }}
                    </span>
                  </span>
                  <Button
                    variant="destructive-ghost"
                    size="icon-sm"
                    type="button"
                    title="Delete cookies file"
                    aria-label="Delete cookies file"
                    @click="deleteCookie(site.key, cookie.id)"
                  >
                    <template #icon>
                      <IconTrash aria-hidden="true" />
                    </template>
                  </Button>
                </li>
              </ul>
              <p v-else class="text-[0.8125rem] text-muted-foreground">
                No cookie files yet.
              </p>
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </TooltipProvider>
</template>
