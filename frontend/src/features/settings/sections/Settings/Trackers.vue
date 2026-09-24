<script setup lang="ts">
import IconInfo from "~icons/material-symbols/info-outline";
import { Combobox } from "@/components/ui/combobox";
import {
  Field,
  FieldContent,
  FieldGroup,
  FieldLabel,
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
      <Field v-for="field in COUNT_FIELDS" :key="field.key">
        <FieldLabel :for="field.id" class="items-center gap-1.5">
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
            :id="field.id"
            type="number"
            min="1"
            max="500"
            :placeholder="String(TRACKER_SETTINGS_DEFAULTS[field.key])"
            :model-value="String(settingsDraft.tracker_settings[field.key])"
            @update:model-value="(value: string | number) => setCount(field.key, value)"
          />
        </FieldContent>
      </Field>
    </FieldGroup>
  </TooltipProvider>
</template>
