<script setup lang="ts">
import { Input } from "@/components/ui/input";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import IconInfo from "~icons/material-symbols/info-outline";
import type { LearnedSegment, TokenRole } from "@/types";
import { sourceIconUrl, templateParts } from "@/utils/dashboard";
import { useSlugTokens } from "@/features/settings/composables/useSlugTokens";
import { useSettingsContext } from "@/features/settings/context";
import { Field, FieldContent, FieldLabel, FieldLegend } from "@/components/ui/field";

const ROLE_ITEMS: { key: TokenRole; label: string }[] = [
  { key: "ignore", label: "None" },
  { key: "creator", label: "Creator" },
  { key: "title", label: "Title" },
];

const { settings, settingsDraft, learnedFormatsDraft, editableSourceProfiles } =
  useSettingsContext();
const {
  learnedFormat,
  tokenForPart,
  setTokenName,
  displayTemplate,
  tokenRole,
  isRoleDisabled,
  setSegmentRole,
} = useSlugTokens(
  settingsDraft,
  settings,
  learnedFormatsDraft,
  editableSourceProfiles,
);

function selectableSegments(key: string): LearnedSegment[] {
  return (learnedFormat(key)?.segments || []).filter(
    (segment) => !segment.reserved,
  );
}

function updateRole(
  key: string,
  segment: LearnedSegment,
  value: unknown,
): void {
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

function roleDisabled(
  key: string,
  segment: LearnedSegment,
  role: TokenRole,
): boolean {
  const token = segmentToken(key, segment);
  if (!token) return role !== "ignore";
  return isRoleDisabled(key, token, role);
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
        <!-- No learned format yet, or its links have no parts that can become tokens. -->
        <p
          v-if="!selectableSegments(site.key).length"
          class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
        >
          <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
          Download once from this source to see which parts of its links, if any,
          can become tokens.
        </p>

        <!-- Tokens belong to the source, so every URL format sits above one set of rows. -->
        <section v-else class="flex flex-col gap-4" :aria-label="`${site.label} tokens`">
          <div class="flex flex-col gap-2">
            <FieldLegend
              v-for="template in learnedFormat(site.key)?.templates || []"
              :key="template"
              as="p"
              variant="divider"
              class="mb-0"
            >
              <span class="min-w-0 wrap-anywhere">
                <span
                  v-for="(part, index) in templateParts(displayTemplate(site.key, template))"
                  :key="index"
                  :class="part.token && 'text-accent-ink'"
                  >{{ part.text }}</span
                >
              </span>
            </FieldLegend>
          </div>

          <!-- Two columns like Scraper; on phones the right one wraps only when out of room. -->
          <div
            v-for="segment in selectableSegments(site.key)"
            :key="segment.part"
            class="flex flex-wrap gap-x-6 gap-y-3 sm:grid sm:grid-cols-2 sm:gap-x-8"
          >
            <div class="min-w-36 flex-1">
              <Field label-width="sm">
                <FieldLabel :for="`${site.key}SlugToken${segment.part}`">
                  Token
                </FieldLabel>
                <FieldContent>
                  <Input
                    :id="`${site.key}SlugToken${segment.part}`"
                    :model-value="tokenForPart(site.key, segment.part, segment)"
                    @update:model-value="
                      (v) =>
                        setTokenName(site.key, segment.part, String(v), segment)
                    "
                  />
                </FieldContent>
              </Field>
            </div>
            <div>
              <SegmentedControl
                label="Role"
                label-placement="start"
                label-width="sm"
                :model-value="roleValue(site.key, segment)"
                @update:model-value="
                  (value) => updateRole(site.key, segment, value)
                "
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
          </div>
        </section>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
