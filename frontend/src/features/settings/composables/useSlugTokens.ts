import { type ComputedRef } from "vue";

import type {
  LearnedFormat,
  LearnedSegment,
  RuntimeSettings,
  SavedSettings,
  SlugToken,
  SourceProfile,
  TokenRole,
} from "../../../types";
import { tokenLabel } from "../../../utils/dashboard";
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
    return settingsDraft.source_slug_tokens[key] || [];
  }

  function ensureSlugList(key: string): SlugToken[] {
    if (!settingsDraft.source_slug_tokens[key]) {
      settingsDraft.source_slug_tokens[key] = [];
    }
    return settingsDraft.source_slug_tokens[key];
  }

  function entryForPart(key: string, part: string): SlugToken | undefined {
    return slugList(key).find((entry) => entry.part === part);
  }

  function ensureEntryForPart(
    key: string,
    part: string,
    segment?: LearnedSegment,
  ): SlugToken | undefined {
    const list = ensureSlugList(key);
    let entry = list.find((entry) => entry.part === part);
    if (!entry && segment) {
      entry = { part: part, token: suggestedToken(key, segment) };
      list.push(entry);
    }
    return entry;
  }

  function isSelected(key: string, part: string): boolean {
    return true;
  }

  function tokenForPart(key: string, part: string, segment?: LearnedSegment): string {
    const entry = entryForPart(key, part);
    return entry ? entry.token : segment ? suggestedToken(key, segment) : "";
  }

  function selectableSegments(key: string): LearnedSegment[] {
    return (learnedFormat(key)?.segments || []).filter((segment) => !segment.reserved);
  }

  function varOrdinal(key: string, part: string): number {
    return selectableSegments(key).findIndex((segment) => segment.part === part);
  }

  function suggestedToken(key: string, segment: LearnedSegment): string {
    const idx = varOrdinal(key, segment.part);
    return idx >= 0 ? `var${idx}` : "slug";
  }

  // Effective token name for a segment: the user's typed token wins; then the numbered
  // var default; then the reserved id/creator name (auto tokens). A constant route word
  // or an unnamed literal has no token name (returns "").
  function tokenNameFor(key: string, segment: LearnedSegment): string {
    const entry = entryForPart(key, segment.part);
    if (entry) return entry.token;
    if (!segment.reserved) {
      const idx = varOrdinal(key, segment.part);
      return idx >= 0 ? `var${idx}` : "slug";
    }
    if (segment.kind === "id" || segment.kind === "creator") {
      return segment.label.replace(/[{}]/g, "");
    }
    return "";
  }

  // Checkbox label: a live {{token}} chip that reflects the input, sharing the Scraper's
  // token-label formatting so both panes render tokens identically.
  function segmentLabel(key: string, segment: LearnedSegment): string {
    const name = tokenNameFor(key, segment);
    return name ? tokenLabel(name) : segment.label;
  }

  // Rewrite a learned template so every token position shows its live {{token}}; a
  // constant route word stays verbatim, and a creator's @ handle marker is preserved.
  function displayTemplate(key: string, template: string): string {
    const byPart = new Map(
      (learnedFormat(key)?.segments || []).map((segment) => [segment.part, segment]),
    );
    const match = template.match(/^([a-z][\w+.-]*:\/\/[^/?#]+)([^?#]*)(.*)$/i);
    if (!match) return template;
    const [, origin, path, rest] = match;
    let index = -1;
    const rebuilt = path
      .split("/")
      .map((raw) => {
        if (!raw) return raw;
        index += 1;
        const segment = byPart.get(`path:${index}`);
        if (!segment) return raw;
        const name = tokenNameFor(key, segment);
        if (!name) return raw;
        const label = tokenLabel(name);
        return raw.startsWith("@") ? `@${label}` : label;
      })
      .join("/");
    return `${origin}${rebuilt}${rest}`;
  }

  function setSelected(key: string, segment: LearnedSegment, selected: boolean): void {
    const list = ensureSlugList(key);
    const index = list.findIndex((entry) => entry.part === segment.part);
    if (selected) {
      if (index === -1) {
        list.push({ part: segment.part, token: suggestedToken(key, segment) });
        settingsDraft.source_slug_tokens[key] = [...list];
      }
      return;
    }
    if (index !== -1) {
      const [removed] = list.splice(index, 1);
      settingsDraft.source_slug_tokens[key] = [...list];
      // Drop any role/creator-field connection the removed token owned.
      if (removed?.token) setTokenRole(key, removed.token, "ignore");
    }
  }

  function setTokenName(key: string, part: string, name: string, segment?: LearnedSegment): void {
    const entry = ensureEntryForPart(key, part, segment);
    if (entry) {
      const prev = entry.token;
      entry.token = name;
      settingsDraft.source_slug_tokens[key] = [...slugList(key)];
      if (segment) {
        const defaultName = suggestedToken(key, segment);
        const oldToken = prev || defaultName;
        const currentRole = tokenRole(key, oldToken);
        if (currentRole !== "ignore" && oldToken !== name) {
          setTokenRole(key, oldToken, "ignore");
          if (name) setTokenRole(key, name, currentRole);
        }
      }
    }
  }

  function setSegmentRole(
    key: string,
    segment: LearnedSegment,
    role: TokenRole,
  ): void {
    const token = tokenForPart(key, segment.part, segment);
    if (!token) return;
    if (role !== "ignore") ensureEntryForPart(key, segment.part, segment);
    setTokenRole(key, token, role);
  }

  return {
    learnedFormat,
    isSelected,
    tokenForPart,
    setSelected,
    setTokenName,
    suggestedToken,
    segmentLabel,
    displayTemplate,
    tokenRole,
    isTitleRoleDisabled,
    setTokenRole,
    setSegmentRole,
  };
}
