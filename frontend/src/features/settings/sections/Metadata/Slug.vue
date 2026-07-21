<script setup lang="ts">
import { Input } from "../../../../components/ui/input";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "../../../../components/ui/segmented-control";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "../../../../components/ui/accordion";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "../../../../components/ui/card";
import type { LearnedSegment, TokenRole } from "../../../../types";
import { useSlugTokens } from "../../composables/useSlugTokens";
import { useSettingsContext } from "../../context";
import SettingsLabel from "../../SettingsLabel.vue";

const ROLE_ITEMS: { key: TokenRole; label: string }[] = [
  { key: "ignore", label: "None" },
  { key: "creator", label: "Creator" },
  { key: "title", label: "Title" },
];

const { settings, settingsDraft, editableSourceProfiles } = useSettingsContext();
const {
  learnedFormat,
  isSelected,
  tokenForPart,
  setSelected,
  setTokenName,
  suggestedToken,
  segmentLabel,
  displayTemplate,
  tokenRole,
  isTitleRoleDisabled,
  setTokenRole,
} = useSlugTokens(settingsDraft, settings, editableSourceProfiles);

function selectableSegments(key: string): LearnedSegment[] {
  return (learnedFormat(key)?.segments || []).filter((segment) => !segment.reserved);
}

function updateRole(key: string, token: string, value: unknown): void {
  const next = String(value || "ignore") as TokenRole;
  const role = ROLE_ITEMS.some((item) => item.key === next) ? next : "ignore";
  setTokenRole(key, token, role);
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
        <div
          v-if="!learnedFormat(site.key)"
          class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55"
        >
          Download once from this source to learn its URL format, then choose which
          parts become tokens.
        </div>

        <div v-else class="flex flex-col gap-[0.85rem]">
          <SettingsLabel
            v-for="template in learnedFormat(site.key)?.templates || []"
            :key="template"
            class="wrap-anywhere"
          >
            {{ displayTemplate(site.key, template) }}
          </SettingsLabel>

          <Card
            v-if="!selectableSegments(site.key).length"
            class="px-6"
          >
            <p class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55">
              This platform has no configurable parts yet.
            </p>
          </Card>

          <Card
            v-for="segment in selectableSegments(site.key)"
            :key="segment.part"
          >
            <CardHeader>
              <CardTitle class="font-mono text-sm leading-snug">
                {{ segmentLabel(site.key, segment) }}
              </CardTitle>
              <CardDescription class="font-mono text-xs">
                {{ segment.part }}
              </CardDescription>
            </CardHeader>

            <CardContent class="grid grid-cols-1 gap-3 lg:grid-cols-2">
              <label class="flex flex-col gap-1.5">
                <SettingsLabel>Token</SettingsLabel>
                <Input
                  :model-value="tokenForPart(site.key, segment.part, segment)"
                  aria-label="Token name"
                  input-class="font-mono"
                  @update:model-value="(v) => setTokenName(site.key, segment.part, String(v), segment)"
                />
              </label>

              <div class="flex flex-col gap-1.5">
                <SettingsLabel>Role</SettingsLabel>
                <SegmentedControl
                  :model-value="tokenRole(site.key, tokenForPart(site.key, segment.part, segment) || suggestedToken(site.key, segment))"
                  class="flex-wrap h-auto min-h-9"
                  @update:model-value="(value) => updateRole(site.key, tokenForPart(site.key, segment.part, segment) || suggestedToken(site.key, segment), value)"
                >
                  <SegmentedControlItem
                    v-for="role in ROLE_ITEMS"
                    :key="role.key"
                    :value="role.key"
                    :disabled="
                      role.key === 'title' &&
                      isTitleRoleDisabled(site.key, tokenForPart(site.key, segment.part, segment) || suggestedToken(site.key, segment))
                    "
                  >
                    {{ role.label }}
                  </SegmentedControlItem>
                </SegmentedControl>
              </div>
            </CardContent>
          </Card>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
