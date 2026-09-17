<script setup lang="ts">
import { reactive, watch } from "vue";
import { toast } from "vue-sonner";
import IconSearch from "~icons/material-symbols/search";
import IconSpinner from "~icons/material-symbols/sync";

import { probeTabs } from "@/api";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useSettingsContext } from "@/features/settings/context";
import { errorMessage, normalizeSourceKey } from "@/utils/dashboard";

const { settingsDraft, editableSourceProfiles } = useSettingsContext();

const probes = reactive<Record<string, { url: string; loading: boolean }>>(
  {},
);

watch(
  editableSourceProfiles,
  (profiles) => {
    for (const profile of profiles) {
      if (!probes[profile.key]) {
        probes[profile.key] = { url: "", loading: false };
      }
    }
  },
  { immediate: true },
);

// The server joins the pages found to the rows they already are, so every row keeps its choice.
async function runProbe(key: string): Promise<void> {
  const state = probes[key];
  const url = state.url.trim();
  if (state.loading) return;
  if (!url) {
    toast.error("Paste a link to test.");
    return;
  }
  state.loading = true;
  try {
    const response = await probeTabs(
      url,
      key,
      settingsDraft.source_tracker_tabs,
    );
    const target = normalizeSourceKey(response.source_key) || key;
    settingsDraft.source_tracker_tabs[target] = response.tabs;
    if (!response.tabs.some((found) => found.tab)) {
      toast.error("No other pages found on that link.");
    }
  } catch (error) {
    toast.error(errorMessage(error, "Could not read that link."));
  } finally {
    state.loading = false;
  }
}

// By default a page is scrolled only when the engines can't list it.
function hasChoices(key: string): boolean {
  return (settingsDraft.source_tracker_tabs[key] || []).some(
    (row) => row.enabled === row.engine,
  );
}

function resetPages(key: string): void {
  for (const row of settingsDraft.source_tracker_tabs[key] || []) {
    row.enabled = !row.engine;
  }
}
</script>

<template>
  <TooltipProvider>
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
            <Field>
              <FieldLabel :for="`${site.key}ScrollingProbeInput`">
                Probe creator
              </FieldLabel>
              <FieldContent class="flex-row items-center gap-2">
                <Input
                  :id="`${site.key}ScrollingProbeInput`"
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
                  type="button"
                  aria-label="Find pages"
                  title="Find pages"
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

            <Card
              v-if="!settingsDraft.source_tracker_tabs[site.key]?.length"
              class="px-6"
            >
              <p class="text-[0.8125rem] text-muted-foreground">
                Test a creator link from this source to choose which pages its
                trackers scroll.
              </p>
            </Card>

            <div
              v-if="settingsDraft.source_tracker_tabs[site.key]?.length"
              class="flex flex-col gap-2"
            >
              <div class="flex items-center justify-between gap-2 min-h-6">
                <div class="flex items-center gap-1.5">
                  <Label>Pages to scroll</Label>
                  <Tooltip>
                    <TooltipTrigger as-child>
                      <button
                        type="button"
                        class="inline-flex h-4 w-4 items-center justify-center rounded-full border border-(--glass-border) bg-black/20 text-[0.625rem] font-semibold leading-none text-muted-foreground transition-all duration-300 ease-glass hover:border-accent hover:text-white focus-visible:ring-2 focus-visible:ring-accent in-[.light-mode]:bg-white/40 in-[.light-mode]:hover:text-black"
                        aria-label="Pages to scroll help"
                      >
                        i
                      </button>
                    </TooltipTrigger>
                    <TooltipContent side="top">
                      Ticked pages are scrolled once the engines are done.
                      Unticked pages are left to the engines when they can list
                      them, else skipped. Each tracker finds its own version of a
                      page.
                    </TooltipContent>
                  </Tooltip>
                </div>
                <button
                  v-if="hasChoices(site.key)"
                  type="button"
                  class="text-xs opacity-70 hover:opacity-100 transition-opacity"
                  title="Scroll only the pages the engines can't list"
                  @click="resetPages(site.key)"
                >
                  Reset
                </button>
              </div>
              <FieldLabel
                v-for="row in settingsDraft.source_tracker_tabs[site.key]"
                :key="row.tab"
                class="cursor-pointer items-center gap-2"
              >
                <Checkbox
                  :checked="row.enabled"
                  @update:checked="(value: boolean) => (row.enabled = value)"
                />
                <span>{{ row.label || row.tab || "This link" }}</span>
              </FieldLabel>
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </TooltipProvider>
</template>
