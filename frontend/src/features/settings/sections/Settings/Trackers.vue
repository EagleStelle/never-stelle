<script setup lang="ts">
import { Combobox } from "@/components/ui/combobox";
import {
  Field,
  FieldContent,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSeparator,
  FieldSet,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useSettingsContext } from "@/features/settings/context";
import { TRACKER_INTERVALS } from "@/ui";
import { TRACKER_SETTINGS_DEFAULTS } from "@/utils/dashboard";

const { settingsDraft } = useSettingsContext();

const COUNT_FIELDS: {
  key: "page_size" | "caught_up_after";
  id: string;
  label: string;
  help: string;
}[] = [
  {
    key: "page_size",
    id: "trackerPageSizeInput",
    label: "Batch size",
    help:
      "New items one check queues. The next check starts with anything posted since, then continues with older ones.",
  },
  {
    key: "caught_up_after",
    id: "trackerMatchStreakInput",
    label: "Match streak",
    help:
      "After this many saved items in a row, a check knows nothing is new. It then goes back to where it last stopped, or ends if there is nothing left. Raise it if old posts are pinned at the top.",
  },
];

// A blank or partial entry leaves the saved value alone; the server clamps the range.
function setCount(key: "page_size" | "caught_up_after", raw: string | number): void {
  const value = Math.floor(Number(raw));
  if (String(raw).trim() && Number.isFinite(value) && value > 0) settingsDraft.tracker_settings[key] = value;
}

function setIntervalSeconds(value: string | string[]): void {
  if (typeof value === "string" && value) settingsDraft.tracker_settings.interval_seconds = Number(value);
}
</script>

<template>
  <TooltipProvider>
    <div class="flex flex-col gap-4">
      <FieldSet>
        <FieldLegend>Checks</FieldLegend>
        <FieldGroup>
          <Field v-for="field in COUNT_FIELDS" :key="field.key">
            <FieldLabel :for="field.id" class="items-center gap-1.5">
              <span>{{ field.label }}</span>
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
                <TooltipContent side="top">
                  {{ field.help }}
                </TooltipContent>
              </Tooltip>
            </FieldLabel>
            <FieldContent>
              <Input
                :id="field.id"
                type="number"
                min="1"
                max="500"
                :placeholder="String(TRACKER_SETTINGS_DEFAULTS[field.key])"
                :model-value="String(settingsDraft.tracker_settings[field.key])"
                class="w-24"
                @update:model-value="(value: string | number) => setCount(field.key, value)"
              />
            </FieldContent>
          </Field>
        </FieldGroup>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>New trackers</FieldLegend>
        <FieldGroup>
          <Combobox
            :model-value="String(settingsDraft.tracker_settings.interval_seconds)"
            :items="TRACKER_INTERVALS"
            label="Check interval"
            label-placement="start"
            placeholder="Select..."
            empty-text="No intervals."
            @update:model-value="setIntervalSeconds"
          />
        </FieldGroup>
      </FieldSet>
    </div>
  </TooltipProvider>
</template>
