<script setup lang="ts">
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Combobox } from "@/components/ui/combobox";
import { Label } from "@/components/ui/label";
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
    settingsDraft.site_locations[siteKey]?.[format] ??
    settings.site_locations[siteKey]?.[format] ??
    ""
  );
}

function setLocation(siteKey: string, format: string, value: string): void {
  if (!settingsDraft.site_locations[siteKey]) {
    settingsDraft.site_locations[siteKey] = {};
  }
  settingsDraft.site_locations[siteKey][format] = value;
}

function pathKey(path: string): string {
  return String(path || "")
    .replace(/\\/g, "/")
    .replace(/\/+$/g, "")
    .toLowerCase();
}

function isSameOrChildPath(path: string, parent: string): boolean {
  const childKey = pathKey(path);
  const parentKey = pathKey(parent);
  return Boolean(
    childKey &&
      parentKey &&
      (childKey === parentKey || childKey.startsWith(`${parentKey}/`)),
  );
}

function sourceRoot(siteKey: string): string {
  return settings.source_location_roots?.[siteKey] || "";
}

// The source root and its children, plus the current valid value so a saved path
// stays selectable even when the folder has not been created on disk yet.
function locationItems(
  siteKey: string,
  format: string,
): { key: string; label: string }[] {
  const current = location(siteKey, format);
  const root = sourceRoot(siteKey);
  const paths = root ? [root] : [];
  for (const path of settings.download_locations || []) {
    if (root && isSameOrChildPath(path, root)) paths.push(path);
  }
  if (current && (!root || isSameOrChildPath(current, root))) {
    paths.unshift(current);
  }
  const seen = new Set<string>();
  return paths.map((path) => ({ key: path, label: path }))
    .filter((item) => {
      const key = pathKey(item.key);
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
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
          <div
            v-for="format in formatsFor(site.key)"
            :key="format"
            class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3"
          >
            <Label class="sm:w-48 sm:shrink-0 break-all wrap-anywhere">
              {{ displayUrlTemplate(format) }}
            </Label>
            <div class="w-full sm:flex-auto">
              <Combobox
                :model-value="location(site.key, format)"
                :items="locationItems(site.key, format)"
                @update:model-value="
                  (val) => setLocation(site.key, format, val)
                "
                layout="fill"
                placeholder="Choose a save path"
                empty-text="No locations."
              />
            </div>
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
