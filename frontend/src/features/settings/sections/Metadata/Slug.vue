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
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import type { LearnedSegment, TokenRole } from "@/types";
import { useSlugTokens } from "@/features/settings/composables/useSlugTokens";
import { useSettingsContext } from "@/features/settings/context";
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";

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
  segmentLabel,
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
      <AccordionTrigger>
        {{ site.label }}
      </AccordionTrigger>

      <AccordionContent>
        <Card v-if="!learnedFormat(site.key)" class="px-6">
          <p class="text-[0.8125rem] text-muted-foreground">
            Download once from this source to learn its URL format, then choose
            which parts become tokens.
          </p>
        </Card>

        <div v-else class="flex flex-col gap-[0.85rem]">
          <Card
            v-for="template in learnedFormat(site.key)?.templates || []"
            :key="template"
          >
            <CardHeader>
              <CardTitle class="font-mono text-sm leading-snug">
                {{ displayTemplate(site.key, template) }}
              </CardTitle>
            </CardHeader>

            <CardContent class="flex flex-col gap-4">
              <p
                v-if="!selectableSegments(site.key).length"
                class="text-[0.8125rem] text-muted-foreground"
              >
                This platform has no configurable parts yet.
              </p>

              <template
                v-for="(segment, i) in selectableSegments(site.key)"
                :key="segment.part"
              >
                <Separator v-if="i > 0" class="my-2" />

                <div class="flex flex-col gap-3">
                  <span class="min-w-0 font-mono text-sm font-semibold wrap-anywhere">
                    {{ segmentLabel(site.key, segment) }}
                  </span>

                  <div class="flex flex-col gap-3">
                    <Field label-width="xs">
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

                    <SegmentedControl
                      label="Role"
                      label-placement="start"
                      label-width="xs"
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
              </template>
            </CardContent>
          </Card>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
