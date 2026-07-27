<script setup lang="ts">
import { reactive, ref } from "vue";
import IconDrag from "~icons/material-symbols/drag-indicator";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
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

// Left blank, a field keeps the built-in default shown as its placeholder.
const POLICY_FIELDS: Array<{
  key: CookiePolicyField;
  label: string;
  help: string;
  min: string;
}> = [
  {
    key: "limit",
    label: "Use limit",
    help: "How many times one cookies file can be used before it rests.",
    min: "1",
  },
  {
    key: "window",
    label: "Reset after",
    help: "Seconds before the use limit starts over.",
    min: "1",
  },
  {
    key: "delay",
    label: "Pause",
    help: "Seconds to wait before using the same cookies file again.",
    min: "0",
  },
  {
    key: "cooldown",
    label: "Blocked rest",
    help: "Seconds to rest a cookies file after the site blocks it.",
    min: "0",
  },
  {
    key: "wait",
    label: "Max wait",
    help: "How long a download waits for an available cookies file.",
    min: "0",
  },
];

function policyValue(key: string, field: CookiePolicyField): string {
  const value = settingsDraft.source_cookie_policies[key]?.[field];
  return value === undefined || value === null ? "" : String(value);
}

function policyPlaceholder(field: CookiePolicyField): string {
  return String(settings.cookie_policy_defaults[field]);
}

function setPolicyValue(key: string, field: CookiePolicyField, raw: string | number): void {
  if (!settingsDraft.source_cookie_policies[key]) {
    settingsDraft.source_cookie_policies[key] = {};
  }
  // Read back through the reactive proxy so the edit below is tracked.
  const entry = settingsDraft.source_cookie_policies[key];
  const text = String(raw ?? "").trim();
  if (!text) {
    delete entry[field];
  } else {
    const parsed = Number(text);
    if (!Number.isFinite(parsed)) return;
    entry[field] = parsed;
  }
  markSettingsDraftDirty("cookies");
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
            <div class="flex flex-wrap gap-3">
              <div
                v-for="field in POLICY_FIELDS"
                :key="field.key"
                class="flex flex-col gap-1.5"
              >
                <div class="flex items-center gap-1.5">
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
                    <TooltipContent side="top" align="start">
                      {{ field.help }}
                    </TooltipContent>
                  </Tooltip>
                </div>
                <Input
                  :id="`${site.key}Cookie${field.key}`"
                  data-settings-system
                  type="number"
                  :min="field.min"
                  :placeholder="policyPlaceholder(field.key)"
                  :model-value="policyValue(site.key, field.key)"
                  class="w-32"
                  @update:model-value="
                    (value: string | number) => setPolicyValue(site.key, field.key, value)
                  "
                />
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

            <Card v-if="!cookiesFor(site.key).length" class="px-6">
              <p class="text-[0.8125rem] text-muted-foreground">
                No cookies file yet.
              </p>
            </Card>

            <ul v-else class="flex flex-col gap-1">
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
                  variant="danger"
                  type="button"
                  class="shrink-0"
                  title="Delete cookies file"
                  aria-label="Delete cookies file"
                  @click="deleteCookie(site.key, cookie.id)"
                >
                  <template #icon>
                    <IconTrash class="w-4 h-4" aria-hidden="true" />
                  </template>
                </Button>
              </li>
            </ul>

            <Button
              variant="soft"
              type="button"
              class="self-start"
              :title="`Upload ${site.label} cookie`"
              :aria-label="`Upload ${site.label} cookie`"
              @click="openPicker(site.key)"
            >
              <template #icon>
                <IconUpload class="w-4 h-4" aria-hidden="true" />
              </template>
              Upload cookie
            </Button>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </TooltipProvider>
</template>
