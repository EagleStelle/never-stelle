<script setup lang="ts">
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Checkbox } from "@/components/ui/checkbox";
import {
  FieldGroup,
  FieldLegend,
  FieldSet,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import type { NamingChoice } from "@/types";
import { useNamingSettings } from "@/features/settings/composables/useNamingSettings";
import { useSettingsContext } from "@/features/settings/context";

const { settings, settingsDraft, editableSourceProfiles } =
  useSettingsContext();
const {
  cleanupRules,
  titleLengthRule,
  namingChoices,
  ruleEnabled,
  setRule,
  maxChars,
  setMaxChars,
  stemMaxChars,
  setStemMaxChars,
  choiceValue,
  setChoice,
} = useNamingSettings(settingsDraft, settings);

// A segmented control clears its value when the active item is clicked again; keep the
// current choice instead of writing an empty one.
function onChoice(
  siteKey: string,
  choice: NamingChoice,
  value: string | string[],
): void {
  const next = Array.isArray(value) ? value[0] : value;
  if (next) setChoice(siteKey, choice, next);
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
        <div class="flex flex-col gap-6 pt-2">
          <FieldSet>
            <FieldLegend class="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">
              Title
            </FieldLegend>
            <FieldGroup class="gap-4">
              <div class="flex flex-col gap-2">
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
              </div>
              <div
                v-if="ruleEnabled(site.key, titleLengthRule)"
                class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3"
              >
                <Label class="sm:w-40 sm:shrink-0">Maximum length</Label>
                <div class="w-full sm:flex-auto">
                  <div class="flex items-center gap-2">
                    <Input
                      type="number"
                      min="12"
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
                </div>
              </div>
            </FieldGroup>
          </FieldSet>

          <FieldSet>
            <FieldLegend class="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">
              Filename
            </FieldLegend>
            <FieldGroup class="gap-4">
              <div
                v-for="choice in namingChoices"
                :key="choice.key"
                class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3"
              >
                <Label class="sm:w-40 sm:shrink-0">{{ choice.label }}</Label>
                <div class="w-full sm:flex-auto">
                  <SegmentedControl
                    class="max-w-full overflow-x-auto"
                    :model-value="choiceValue(site.key, choice)"
                    @update:model-value="
                      (v: string | string[]) => onChoice(site.key, choice, v)
                    "
                  >
                    <SegmentedControlItem
                      v-for="option in choice.options"
                      :key="option.value"
                      :value="option.value"
                    >
                      {{ option.label }}
                    </SegmentedControlItem>
                  </SegmentedControl>
                </div>
              </div>
              <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                <Label class="sm:w-40 sm:shrink-0">Maximum length</Label>
                <div class="w-full sm:flex-auto">
                  <div class="flex items-center gap-2">
                    <Input
                      type="number"
                      min="0"
                      placeholder="Off"
                      :model-value="
                        stemMaxChars(site.key) ? String(stemMaxChars(site.key)) : ''
                      "
                      class="w-24 shrink-0"
                      @update:model-value="
                        (v: string | number) =>
                          setStemMaxChars(site.key, Number(v || 0))
                      "
                    />
                    <span class="text-white/55 in-[.light-mode]:text-black/55">
                      characters
                    </span>
                  </div>
                </div>
              </div>
            </FieldGroup>
          </FieldSet>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
