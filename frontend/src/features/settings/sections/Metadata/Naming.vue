<script setup lang="ts">
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "../../../../components/ui/accordion";
import { Checkbox } from "../../../../components/ui/checkbox";
import { Input } from "../../../../components/ui/input";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "../../../../components/ui/segmented-control";
import type { NamingChoice } from "../../../../types";
import { useNamingSettings } from "../../composables/useNamingSettings";
import { useSettingsContext } from "../../context";
import SettingsGroup from "../../SettingsGroup.vue";
import SettingsRow from "../../SettingsRow.vue";
import SettingsSection from "../../SettingsSection.vue";

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
        <SettingsSection>
          <SettingsGroup label="Title">
            <SettingsGroup dense>
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
            </SettingsGroup>
            <SettingsRow
              v-if="ruleEnabled(site.key, titleLengthRule)"
              label="Maximum length"
            >
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
            </SettingsRow>
          </SettingsGroup>

          <SettingsGroup label="Filename">
            <SettingsRow
              v-for="choice in namingChoices"
              :key="choice.key"
              :label="choice.label"
            >
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
            </SettingsRow>
            <SettingsRow label="Maximum length">
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
            </SettingsRow>
          </SettingsGroup>
        </SettingsSection>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
