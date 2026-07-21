import { computed, type ComputedRef } from "vue";

import type {
  LearnedFormat,
  LearnedSegment,
  RuntimeSettings,
  SavedSettings,
  SlugToken,
  SourceProfile,
} from "../../../types";
import { normalizeTokenName } from "../../../utils/dashboard";
import { useTokenRoles } from "./useTokenRoles";

// Per-source slug tokens: map a learned-format URL part to a user-named token, with the
// same role selector as the scraper. Candidates come strictly from the learned format.
export function useSlugTokens(
  settingsDraft: SavedSettings,
  settings: RuntimeSettings,
  _editableSourceProfiles: ComputedRef<SourceProfile[]>,
) {
  const { tokenRole, isTitleRoleDisabled, setTokenRole } = useTokenRoles(settingsDraft);

  function learnedFormat(key: string): LearnedFormat | undefined {
    return settings.learned_formats?.[key];
  }

  function slugList(key: string): SlugToken[] {
    if (!settingsDraft.source_slug_tokens[key]) {
      settingsDraft.source_slug_tokens[key] = [];
    }
    return settingsDraft.source_slug_tokens[key];
  }

  function entryForPart(key: string, part: string): SlugToken | undefined {
    return slugList(key).find((entry) => entry.part === part);
  }

  function isSelected(key: string, part: string): boolean {
    return Boolean(entryForPart(key, part));
  }

  function tokenForPart(key: string, part: string): string {
    return entryForPart(key, part)?.token || "";
  }

  function suggestedToken(segment: LearnedSegment): string {
    if (segment.part.startsWith("query:")) {
      return normalizeTokenName(segment.part.slice("query:".length)) || "slug";
    }
    return normalizeTokenName(segment.label) || "slug";
  }

  function setSelected(key: string, segment: LearnedSegment, selected: boolean): void {
    const list = slugList(key);
    const index = list.findIndex((entry) => entry.part === segment.part);
    if (selected) {
      if (index === -1) list.push({ part: segment.part, token: suggestedToken(segment) });
      return;
    }
    if (index !== -1) {
      const [removed] = list.splice(index, 1);
      // Drop any role/creator-field connection the removed token owned.
      if (removed?.token) setTokenRole(key, removed.token, "ignore");
    }
  }

  function setTokenName(key: string, part: string, name: string): void {
    const entry = entryForPart(key, part);
    if (entry) entry.token = name;
  }

  return {
    learnedFormat,
    isSelected,
    tokenForPart,
    setSelected,
    setTokenName,
    tokenRole,
    isTitleRoleDisabled,
    setTokenRole,
  };
}
