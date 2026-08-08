<script setup lang="ts">
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Combobox } from "@/components/ui/combobox";
import { useSettingsContext } from "@/features/settings/context";
import { displayUrlTemplate } from "@/utils/dashboard";

const { settings, settingsDraft, learnedFormatsDraft, editableSourceProfiles } =
  useSettingsContext();

function formatsFor(key: string): string[] {
  return (
    learnedFormatsDraft?.[key]?.templates ||
    settings.learned_formats?.[key]?.templates ||
    []
  );
}

function location(siteKey: string, format: string): string {
  return (
    settingsDraft.source_locations[siteKey]?.[format] ??
    settings.source_locations[siteKey]?.[format] ??
    ""
  );
}

function setLocation(siteKey: string, format: string, value: string): void {
  if (!settingsDraft.source_locations[siteKey]) {
    settingsDraft.source_locations[siteKey] = {};
  }
  settingsDraft.source_locations[siteKey][format] = value;
}

function displayPath(siteKey: string, subpath: string): string {
  const root = String(settings.media_root || "").replace(/[\\/]+$/, "");
  return [root, siteKey, subpath].filter(Boolean).join("/").replace(/\\/g, "/");
}

// The source's existing subfolders, plus the current value so a saved subpath stays
// selectable even when the folder has not been created on disk yet.
function locationItems(
  siteKey: string,
  format: string,
): { key: string; label: string }[] {
  const current = location(siteKey, format);
  const options = settings.source_location_options?.[siteKey] || [""];
  const paths = options.includes(current) ? options : [current, ...options];
  return paths.map((path) => ({
    key: path,
    label: displayPath(siteKey, path),
  }));
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
        <p
          v-if="!formatsFor(site.key).length"
          class="py-2 text-[0.8125rem] text-muted-foreground"
        >
          Download once from this source to learn its URL format, then choose
          where its files go.
        </p>

        <div v-else class="flex flex-col gap-4 py-2">
          <Combobox
            v-for="format in formatsFor(site.key)"
            :key="format"
            :model-value="location(site.key, format)"
            :items="locationItems(site.key, format)"
            @update:model-value="(val) => setLocation(site.key, format, val)"
            :label="displayUrlTemplate(format)"
            label-placement="start"
            label-width="lg"
            placeholder="Choose a save path"
            empty-text="No locations."
          />
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
