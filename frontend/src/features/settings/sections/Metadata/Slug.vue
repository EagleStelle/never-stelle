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
import SettingsEmptyCard from "../../SettingsEmptyCard.vue";
import SettingsLabel from "../../SettingsLabel.vue";

const ROLE_ITEMS: { key: TokenRole; label: string }[] = [
  { key: "ignore", label: "None" },
  { key: "creator", label: "Creator" },
  { key: "title", label: "Title" },
];

const { settings, settingsDraft, learnedFormatsDraft, editableSourceProfiles } = useSettingsContext();
const {
  learnedFormat,
  tokenForPart,
  setTokenName,
  segmentLabel,
  displayTemplate,
  tokenRole,
  isTitleRoleDisabled,
  setSegmentRole,
} = useSlugTokens(settingsDraft, settings, learnedFormatsDraft, editableSourceProfiles);

function selectableSegments(key: string): LearnedSegment[] {
  return (learnedFormat(key)?.segments || []).filter((segment) => !segment.reserved);
}

function updateRole(key: string, segment: LearnedSegment, value: unknown): void {
  const next = String(value || "ignore") as TokenRole;
  const role = ROLE_ITEMS.some((item) => item.key === next) ? next : "ignore";
  setSegmentRole(key, segment, role);
}

function segmentToken(key: string, segment: LearnedSegment): string {
  return tokenForPart(key, segment.part, segment);
}

function roleValue(key: string, segment: LearnedSegment): TokenRole {
  const token = segmentToken(key, segment);
  return token ? tokenRole(key, token) : "ignore";
}

function roleDisabled(key: string, segment: LearnedSegment, role: TokenRole): boolean {
  const token = segmentToken(key, segment);
  if (!token) return role !== "ignore";
  return role === "title" && isTitleRoleDisabled(key, token);
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
        <SettingsEmptyCard
          v-if="!learnedFormat(site.key)"
        >
          Download once from this source to learn its URL format, then choose which
          parts become tokens.
        </SettingsEmptyCard>

        <div v-else class="flex flex-col gap-[0.85rem]">
          <SettingsLabel
            v-for="template in learnedFormat(site.key)?.templates || []"
            :key="template"
            class="wrap-anywhere"
          >
            {{ displayTemplate(site.key, template) }}
          </SettingsLabel>

          <SettingsEmptyCard
            v-if="!selectableSegments(site.key).length"
          >
            This platform has no configurable parts yet.
          </SettingsEmptyCard>

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
                  :model-value="roleValue(site.key, segment)"
                  class="flex-wrap h-auto min-h-9"
                  @update:model-value="(value) => updateRole(site.key, segment, value)"
                >
                  <SegmentedControlItem
                    v-for="role in ROLE_ITEMS"
                    :key="role.key"
                    :value="role.key"
                    :disabled="roleDisabled(site.key, segment, role.key)"
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
