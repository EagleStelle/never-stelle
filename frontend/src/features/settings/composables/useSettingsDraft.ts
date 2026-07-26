import { computed, watch, type ComputedRef } from "vue";

import type { RuntimeSettings, SettingsDraft, SourceProfile } from "@/types";
import {
  createPlatformScrapeRules,
  createTemplateSettings,
  mergeSourceProfiles,
  settingsManagedSourceProfiles,
} from "@/utils/dashboard";

interface DraftSources {
  settings: RuntimeSettings;
  settingsDraft: SettingsDraft;
  sourceProfiles: SourceProfile[];
}

// Derives the editable source list and guarantees each source owns its draft
// entries before any section binds to them.
export function useSettingsDraft(props: DraftSources): {
  editableSourceProfiles: ComputedRef<SourceProfile[]>;
} {
  const mergedProfiles = computed<SourceProfile[]>(() =>
    mergeSourceProfiles(props.sourceProfiles, props.settingsDraft.source_profiles),
  );

  const editableSourceProfiles = computed<SourceProfile[]>(() =>
    settingsManagedSourceProfiles(mergedProfiles.value)
      .filter((profile) => profile.key)
      .sort((a, b) => a.label.localeCompare(b.label)),
  );

  watch(
    editableSourceProfiles,
    (profiles) => {
      const draft = props.settingsDraft;
      for (const profile of profiles) {
        if (!draft.source_profiles.some((item) => item.key === profile.key)) {
          draft.source_profiles.push(profile);
        }
        if (!draft.source_templates[profile.key]) {
          draft.source_templates[profile.key] = {};
        }
        if (!draft.source_scrape_rules[profile.key]) {
          draft.source_scrape_rules[profile.key] = createPlatformScrapeRules();
        }
        if (!draft.source_token_roles[profile.key]) {
          draft.source_token_roles[profile.key] = {};
        }
        if (!draft.source_slug_tokens[profile.key]) {
          draft.source_slug_tokens[profile.key] = [];
        }
        if (!(profile.key in draft.site_locations)) {
          draft.site_locations[profile.key] = props.settings.site_locations[profile.key] || "";
        }
      }
    },
    { immediate: true },
  );

  return { editableSourceProfiles };
}
