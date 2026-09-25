<script setup lang="ts">
import { reactive, watch } from "vue";
import { toast } from "vue-sonner";
import IconInfo from "~icons/material-symbols/info-outline";
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
import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import {
  Field,
  FieldContent,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSet,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { TrackerSettings } from "@/types";
import { TRACKER_INTERVALS } from "@/ui";
import { useInheritedFields } from "@/features/settings/composables/useInheritedFields";
import { useSettingsContext } from "@/features/settings/context";
import { TRACKER_COUNT_FIELDS } from "@/features/settings/trackerFields";
import { errorMessage, normalizeSourceKey, sourceIconUrl } from "@/utils/dashboard";

const { settingsDraft, editableSourceProfiles } = useSettingsContext();

// A source with no override of its own follows the Defaults pane.
function inherited(field: keyof TrackerSettings): number {
  return settingsDraft.tracker_settings[field];
}

const { fieldValue, setField, endEdit } = useInheritedFields<keyof TrackerSettings>({
  entries: () => settingsDraft.source_tracker_settings,
  inherited,
});

// A pick is final, so it needs no edit text kept.
function setCheckInterval(key: string, value: string | string[]): void {
  if (typeof value !== "string" || !value) return;
  setField(key, "interval_seconds", value);
  endEdit(key, "interval_seconds");
}

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
        <AccordionTrigger :image="sourceIconUrl(site.key)">
          {{ site.label }}
        </AccordionTrigger>
        <AccordionContent>
          <div class="flex flex-col gap-6">
            <FieldSet>
              <FieldLegend variant="divider">Fetching</FieldLegend>
              <FieldGroup>
                <Combobox
                  :id="`${site.key}Trackerinterval_seconds`"
                  :model-value="fieldValue(site.key, 'interval_seconds')"
                  :items="TRACKER_INTERVALS"
                  label="Check interval"
                  label-placement="start"
                  placeholder="Select..."
                  empty-text="No intervals."
                  @update:model-value="(value: string | string[]) => setCheckInterval(site.key, value)"
                />
                <Field v-for="field in TRACKER_COUNT_FIELDS" :key="field.key">
                  <FieldLabel :for="`${site.key}Tracker${field.key}`" class="items-center gap-1.5">
                    <span>{{ field.label }}</span>
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
                  </FieldLabel>
                  <FieldContent>
                    <Input
                      :id="`${site.key}Tracker${field.key}`"
                      type="number"
                      min="1"
                      max="500"
                      :placeholder="String(inherited(field.key))"
                      :model-value="fieldValue(site.key, field.key)"
                      @blur="endEdit(site.key, field.key)"
                      @update:model-value="
                        (value: string | number) => setField(site.key, field.key, value)
                      "
                    />
                  </FieldContent>
                </Field>
              </FieldGroup>
            </FieldSet>

            <FieldSet>
              <FieldLegend variant="divider">Scrolling</FieldLegend>
              <FieldGroup>
                <Field>
                  <FieldLabel :for="`${site.key}TrackerProbeInput`">
                    Probe creator
                  </FieldLabel>
                  <FieldContent class="flex-row items-center gap-2">
                    <Input
                      :id="`${site.key}TrackerProbeInput`"
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

                <p
                  v-if="!settingsDraft.source_tracker_tabs[site.key]?.length"
                  class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
                >
                  <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
                  Test a creator link from this source to choose which pages its
                  trackers scroll.
                </p>

                <div
                  v-if="settingsDraft.source_tracker_tabs[site.key]?.length"
                  class="flex flex-col gap-2"
                >
                  <div class="flex items-center justify-between gap-2 min-h-8">
                    <div class="flex items-center gap-1.5">
                      <Label>Pages to scroll</Label>
                      <Tooltip>
                        <TooltipTrigger as-child>
                          <button
                            type="button"
                            class="-m-1 inline-flex size-6 items-center justify-center rounded-md text-muted-foreground transition-colors duration-200 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
                            aria-label="Pages to scroll help"
                          >
                            <IconInfo class="size-4" aria-hidden="true" />
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
                      class="rounded-md px-1.5 py-1 text-xs text-muted-foreground transition-colors duration-200 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
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
              </FieldGroup>
            </FieldSet>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </TooltipProvider>
</template>
