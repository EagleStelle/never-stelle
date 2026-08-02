<script setup lang="ts">
import { reactive, ref } from "vue";
import IconDrag from "~icons/material-symbols/drag-indicator";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";

import { Button } from "@/components/ui/button";
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

const {
  settings,
  settingsDraft,
  editableSourceProfiles,
  cookieStatuses,
  connectCookies,
  removeCookies,
  reorderCookies,
  markSettingsDraftDirty,
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
  markSettingsDraftDirty("cookies");
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
        <AccordionTrigger>
          {{ site.label }}
        </AccordionTrigger>

        <AccordionContent>
          <div class="flex flex-col gap-4">
            <div class="flex flex-col gap-3 w-full">
              <div
                v-for="field in COOKIE_POLICY_FIELDS"
                :key="field.key"
                class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3 w-full"
              >
                <div class="flex items-center gap-1.5 sm:w-40 sm:shrink-0">
                  <Label :for="`${site.key}Cookie${field.key}`">
                    {{ field.label }}
                  </Label>
                  <Tooltip>
                    <TooltipTrigger as-child>
                      <button
                        type="button"
                        class="inline-flex h-4 w-4 items-center justify-center rounded-full border border-(--glass-border) bg-black/20 text-[0.625rem] font-semibold leading-none text-muted-foreground transition-all duration-300 ease-glass hover:border-accent hover:text-white focus-visible:ring-2 focus-visible:ring-accent in-[.light-mode]:bg-white/40 in-[.light-mode]:hover:text-black"
                        :aria-label="`${field.label} help`"
                      >
                        i
                      </button>
                    </TooltipTrigger>
                    <TooltipContent side="top">
                      {{ field.help }}
                    </TooltipContent>
                  </Tooltip>
                </div>
                <div class="w-full sm:flex-auto">
                  <Input
                    :id="`${site.key}Cookie${field.key}`"
                    data-settings-system
                    type="number"
                    :min="field.min"
                    :placeholder="String(policyInherited(field.key))"
                    :model-value="policyValue(site.key, field.key)"
                    class="w-full"
                    @blur="endPolicyEdit(site.key, field.key)"
                    @update:model-value="
                      (value: string | number) => setPolicyValue(site.key, field.key, value)
                    "
                  />
                </div>
              </div>
            </div>

            <input
              :id="`${site.key}CookiesInput`"
              data-settings-system
              type="file"
              accept=".txt,.cookies,text/plain"
              class="hidden"
              @change="onCookieFile(site.key, $event)"
            />

            <div class="flex flex-col gap-1.5 mt-3">
              <Label>Cookie Pool</Label>
              <ul class="flex flex-col gap-1">
                <li
                  v-for="(cookie, index) in cookiesFor(site.key)"
                  :key="cookie.id"
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
                    {{ cookie.filename }}
                  </span>
                  <Button
                    variant="destructive"
                    type="button"
                    title="Delete cookies file"
                    aria-label="Delete cookies file"
                    @click="deleteCookie(site.key, cookie.id)"
                  >
                    <template #icon>
                      <IconTrash class="w-4 h-4" aria-hidden="true" />
                    </template>
                  </Button>
                </li>
                <li class="flex items-center gap-1.5 rounded-md px-1 py-1">
                  <IconUpload class="w-4 h-4 shrink-0 opacity-50" aria-hidden="true" />
                  <span class="font-mono text-[0.8125rem] flex-1 min-w-0 wrap-anywhere">
                    Upload cookie
                  </span>
                  <Button
                    variant="primary"
                    type="button"
                    :title="`Upload ${site.label} cookie`"
                    :aria-label="`Upload ${site.label} cookie`"
                    @click="openPicker(site.key)"
                  >
                    <template #icon>
                      <IconUpload class="w-4 h-4" aria-hidden="true" />
                    </template>
                  </Button>
                </li>
              </ul>
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </TooltipProvider>
</template>
