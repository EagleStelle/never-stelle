<script setup lang="ts">
import { Checkbox } from "../../../../components/ui/checkbox";
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
import type { LearnedSegment, TokenRole } from "../../../../types";
import { useSlugTokens } from "../../composables/useSlugTokens";
import { useSettingsContext } from "../../context";

const ROLE_ITEMS: { key: TokenRole; label: string }[] = [
  { key: "ignore", label: "None" },
  { key: "username", label: "Username" },
  { key: "nickname", label: "Nickname" },
  { key: "title", label: "Title" },
];

const { settings, settingsDraft, editableSourceProfiles } = useSettingsContext();
const {
  learnedFormat,
  isSelected,
  tokenForPart,
  setSelected,
  setTokenName,
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
          <p
            class="wrap-anywhere font-mono text-[0.75rem] text-white/50 in-[.light-mode]:text-black/50"
          >
            {{ learnedFormat(site.key)?.templates[0] }}
          </p>

          <p
            v-if="!selectableSegments(site.key).length"
            class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55"
          >
            This format has no configurable parts yet.
          </p>

          <div
            v-for="segment in selectableSegments(site.key)"
            :key="segment.part"
            class="rounded-lg border border-(--glass-border) bg-black/10 in-[.light-mode]:bg-white/35"
          >
            <label class="flex items-center gap-3 px-3 py-2">
              <Checkbox
                :checked="isSelected(site.key, segment.part)"
                :aria-label="`Use ${segment.label}`"
                @update:checked="(v: boolean) => setSelected(site.key, segment, v)"
              />
              <span class="min-w-0 flex-1">
                <span class="block truncate font-mono text-[0.8125rem]">
                  {{ segment.label }}
                </span>
                <span
                  class="block truncate text-[0.75rem] text-white/50 in-[.light-mode]:text-black/50"
                >
                  {{ segment.part }}
                </span>
              </span>
            </label>

            <div
              v-if="isSelected(site.key, segment.part)"
              class="grid grid-cols-1 gap-3 border-t border-(--glass-border) px-3 py-3 lg:grid-cols-2"
            >
              <label class="flex flex-col gap-1.5">
                <span class="text-xs uppercase tracking-wider opacity-55">
                  Token
                </span>
                <Input
                  :model-value="tokenForPart(site.key, segment.part)"
                  aria-label="Token name"
                  input-class="font-mono"
                  @update:model-value="(v) => setTokenName(site.key, segment.part, String(v))"
                />
              </label>

              <div class="flex flex-col gap-1.5">
                <span class="text-xs uppercase tracking-wider opacity-55">
                  Role
                </span>
                <SegmentedControl
                  :model-value="tokenRole(site.key, tokenForPart(site.key, segment.part))"
                  class="flex-wrap h-auto min-h-9"
                  @update:model-value="(value) => updateRole(site.key, tokenForPart(site.key, segment.part), value)"
                >
                  <SegmentedControlItem
                    v-for="role in ROLE_ITEMS"
                    :key="role.key"
                    :value="role.key"
                    :disabled="
                      role.key === 'title' &&
                      isTitleRoleDisabled(site.key, tokenForPart(site.key, segment.part))
                    "
                  >
                    {{ role.label }}
                  </SegmentedControlItem>
                </SegmentedControl>
              </div>
            </div>
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
