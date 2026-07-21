import { computed, type ComputedRef } from "vue";

import type {
  LearnedFormat,
  LearnedSegment,
  RuntimeSettings,
  SavedSettings,
  SlugToken,
  SourceProfile,
} from "../../../types";
import { normalizeTokenName, tokenLabel } from "../../../utils/dashboard";
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

  // Position of a {var} segment among all {var} segments of a source, so its default
  // token reads var0, var1, var2 in order — a stable, predictable name for the user.
  function varOrdinal(key: string, part: string): number {
    let n = 0;
    for (const segment of learnedFormat(key)?.segments || []) {
      if (segment.kind !== "var") continue;
      if (segment.part === part) return n;
      n += 1;
    }
    return 0;
  }

  function suggestedToken(key: string, segment: LearnedSegment): string {
    if (segment.kind === "var") return `var${varOrdinal(key, segment.part)}`;
    if (segment.part.startsWith("query:")) {
      return normalizeTokenName(segment.part.slice("query:".length)) || "slug";
    }
    return normalizeTokenName(segment.label) || "slug";
  }

  // Effective token name for a segment: the user's typed token wins; then the numbered
  // var default; then the reserved id/creator name (auto tokens). A constant route word
  // or an unnamed literal has no token name (returns "").
  function tokenNameFor(key: string, segment: LearnedSegment): string {
    const configured = tokenForPart(key, segment.part);
    if (configured) return configured;
    if (segment.kind === "var") return `var${varOrdinal(key, segment.part)}`;
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
    const list = slugList(key);
    const index = list.findIndex((entry) => entry.part === segment.part);
    if (selected) {
      if (index === -1) list.push({ part: segment.part, token: suggestedToken(key, segment) });
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
    segmentLabel,
    displayTemplate,
    tokenRole,
    isTitleRoleDisabled,
    setTokenRole,
  };
}
