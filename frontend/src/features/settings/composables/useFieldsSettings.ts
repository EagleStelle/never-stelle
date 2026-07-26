import { computed, reactive, watch, type ComputedRef } from "vue";

import type {
  CreatorFieldRoles,
  LearnedFormats,
  ProbeFieldsResponse,
  ProbeField,
  RuntimeSettings,
  SavedSettings,
  SourceProfile,
  TitleCleaningRule,
} from "../../../types";
import {
  createCreatorFieldRoles,
  errorMessage,
  isScraperCreatorField,
  normalizeCreatorField,
  normalizeTokenName,
  scraperCreatorField,
  scraperTokenFromCreatorField,
} from "../../../utils/dashboard";

export type FieldRole = "username" | "nickname" | "title";
export type CreatorRole = FieldRole;

export interface CreatorFieldListItem {
  key: string;
  label: string;
  scraper: boolean;
}

interface ProbeState {
  url: string;
  loading: boolean;
  fields: ProbeField[];
  message: string;
}

interface CreatorSettingsActions {
  probeCreatorFields: (
    url: string,
    sourceKey?: string,
  ) => Promise<ProbeFieldsResponse>;
}

const DEFAULT_MAX_CHARS = 100;
const FALLBACK_TITLE_LENGTH_RULE: TitleCleaningRule = {
  key: "shorten",
  label: "Limit overly long titles",
  default: false,
};

const probes = reactive<Record<string, ProbeState>>({});

// Per-source username/nickname/title field lists, title-cleaning toggles, and the field probe.
export function useFieldsSettings(
  settingsDraft: SavedSettings,
  settings: RuntimeSettings,
  learnedFormatsDraft: LearnedFormats,
  editableSourceProfiles: ComputedRef<SourceProfile[]>,
  actions: CreatorSettingsActions,
) {
  watch(
    editableSourceProfiles,
    (profiles) => {
      for (const profile of profiles) {
        if (!probes[profile.key]) {
          probes[profile.key] = {
            url: "",
            loading: false,
            fields: [],
            message: "",
          };
        }
      }
    },
    { immediate: true },
  );

  const rules = computed<TitleCleaningRule[]>(
    () => settings.title_cleaning_rules || [],
  );
  const cleanupRules = computed<TitleCleaningRule[]>(() =>
    rules.value.some((rule) => rule.key === "shorten")
      ? rules.value
      : [...rules.value, FALLBACK_TITLE_LENGTH_RULE],
  );
  const titleLengthRule = computed<TitleCleaningRule>(
    () =>
      rules.value.find((rule) => rule.key === "shorten") ||
      FALLBACK_TITLE_LENGTH_RULE,
  );

  const defaults = computed<CreatorFieldRoles>(() => ({
    username: settings.creator_field_defaults?.username || [],
    nickname: settings.creator_field_defaults?.nickname || [],
    title: settings.creator_field_defaults?.title || [],
  }));

  function creatorRoles(key: string): CreatorFieldRoles {
    if (!settingsDraft.source_creator_fields[key]) {
      settingsDraft.source_creator_fields[key] = { username: [], nickname: [], title: [] };
    }
    return settingsDraft.source_creator_fields[key];
  }

  function sourceHasSavedCreatorFields(key: string): boolean {
    const roles = settings.source_creator_fields[key];
    return Boolean(roles?.username?.length || roles?.nickname?.length || roles?.title?.length);
  }

  function sourceHasSavedFormats(key: string): boolean {
    return Boolean(settings.learned_formats?.[key]?.templates?.length);
  }

  function sourceHasDraftFormats(key: string): boolean {
    if (Object.prototype.hasOwnProperty.call(learnedFormatsDraft, key)) {
      return Boolean(learnedFormatsDraft[key]?.templates?.length);
    }
    return sourceHasSavedFormats(key);
  }

  function sourceFormatsClearedInDraft(key: string): boolean {
    return (
      sourceHasSavedFormats(key) &&
      Object.prototype.hasOwnProperty.call(learnedFormatsDraft, key) &&
      !learnedFormatsDraft[key]?.templates?.length
    );
  }

  function sourceHasDraftRole(key: string, role: FieldRole): boolean {
    const roles = creatorRoles(key);
    return roles[role]?.length > 0;
  }

  function roleDefaultList(key: string, role: FieldRole): string[] {
    if (sourceFormatsClearedInDraft(key)) return [];
    if (sourceHasSavedCreatorFields(key)) {
      return settings.source_creator_fields[key]?.[role] || [];
    }
    const learned = settings.source_creator_field_defaults[key]?.[role] || [];
    if (learned.length) return learned;
    return [];
  }

  function isTokenRoleMatchingFieldRole(tokenRole: string, role: FieldRole): boolean {
    if (tokenRole === role) return true;
    if (tokenRole === "creator" && (role === "username" || role === "nickname")) return true;
    return false;
  }

  function scraperRoleTokens(key: string, role: FieldRole): string[] {
    const roles = settingsDraft.source_token_roles[key] || {};
    const seen = new Set<string>();
    const out: string[] = [];
    const candidates = [
      ...(settingsDraft.source_scrape_rules[key]?.rules || []).map((rule) => rule.token),
      ...(settingsDraft.source_slug_tokens[key] || []).map((slug) => slug.token),
    ];
    for (const raw of candidates) {
      const token = normalizeTokenName(raw);
      if (!token || seen.has(token)) continue;
      const tokenRole = roles[token] || "ignore";
      if (!isTokenRoleMatchingFieldRole(tokenRole, role)) continue;
      seen.add(token);
      out.push(token);
    }
    return out;
  }

  function assignedScraperFields(key: string, role: FieldRole): string[] {
    return scraperRoleTokens(key, role)
      .map((token) => scraperCreatorField(token))
      .filter(Boolean);
  }

  function withAssignedScraperFields(
    key: string,
    role: FieldRole,
    values: string[],
  ): string[] {
    const assigned = assignedScraperFields(key, role);
    const assignedSet = new Set(assigned);
    const out: string[] = [];
    for (const field of assigned) {
      if (!values.map(normalizeCreatorField).includes(field)) {
        out.push(field);
      }
    }
    for (const value of values) {
      const field = normalizeCreatorField(value);
      if (!field) continue;
      if (isScraperCreatorField(field) && !assignedSet.has(field)) continue;
      if (!out.includes(field)) out.push(field);
    }
    return out;
  }

  function fieldList(key: string, role: FieldRole): string[] {
    const roles = creatorRoles(key);
    const list = roles[role];
    const base = list.length
      ? list
      : sourceHasDraftRole(key, role)
        ? []
        : roleDefaultList(key, role);
    return withAssignedScraperFields(key, role, base);
  }

  function fieldListItems(key: string, role: FieldRole): CreatorFieldListItem[] {
    return fieldList(key, role).map((field) => {
      const scraperToken = scraperTokenFromCreatorField(field);
      return {
        key: `field:${field}`,
        label: scraperToken || field,
        scraper: Boolean(scraperToken),
      };
    });
  }

  function sameAsDefault(
    key: string,
    role: FieldRole,
    list: string[],
  ): boolean {
    const base = withAssignedScraperFields(key, role, roleDefaultList(key, role));
    return (
      list.length === base.length &&
      list.every((value, index) => value === base[index])
    );
  }

  function commit(key: string, role: FieldRole, list: string[]): void {
    const normalized = withAssignedScraperFields(key, role, list);
    creatorRoles(key)[role] =
      sameAsDefault(key, role, normalized) && !sourceHasSavedCreatorFields(key)
        ? []
        : normalized;
  }

  function addField(key: string, role: FieldRole, raw: string): void {
    const field = normalizeCreatorField(raw);
    if (!field) return;
    const list = [...fieldList(key, role)];
    if (list.includes(field)) return;
    list.push(field);
    commit(key, role, list);
  }

  function reorderField(
    key: string,
    role: FieldRole,
    from: number,
    to: number,
  ): void {
    const list = [...fieldList(key, role)];
    if (
      from === to ||
      from < 0 ||
      from >= list.length ||
      to < 0 ||
      to >= list.length
    )
      return;
    const [item] = list.splice(from, 1);
    list.splice(to, 0, item);
    commit(key, role, list);
  }

  function resetRole(key: string, role: FieldRole): void {
    creatorRoles(key)[role] = sourceHasSavedCreatorFields(key)
      ? [...roleDefaultList(key, role)]
      : [];
  }

  function isConfigured(key: string, role: FieldRole): boolean {
    return !sameAsDefault(key, role, fieldList(key, role));
  }

  function cleaningFlags(key: string): Record<string, boolean | number> {
    if (!settingsDraft.source_title_cleaning[key])
      settingsDraft.source_title_cleaning[key] = {};
    return settingsDraft.source_title_cleaning[key];
  }

  function ruleEnabled(key: string, rule: TitleCleaningRule): boolean {
    const stored = cleaningFlags(key)[rule.key];
    return typeof stored === "boolean" ? stored : rule.default;
  }

  function setRule(
    key: string,
    rule: TitleCleaningRule,
    enabled: boolean,
  ): void {
    cleaningFlags(key)[rule.key] = enabled;
  }

  function maxChars(key: string): number {
    const value = cleaningFlags(key).max_chars;
    return typeof value === "number" && value > 0 ? value : DEFAULT_MAX_CHARS;
  }

  function setMaxChars(key: string, value: number): void {
    const parsed = Math.floor(Number(value));
    if (Number.isFinite(parsed) && parsed > 0)
      cleaningFlags(key).max_chars = parsed;
  }

  function learnedFields(values: string[]): string[] {
    const out: string[] = [];
    for (const value of values) {
      const field = normalizeCreatorField(value);
      if (field && !isScraperCreatorField(field) && !out.includes(field)) {
        out.push(field);
      }
    }
    return out;
  }

  function mergeLearnedFields(
    key: string,
    role: FieldRole,
    learned: string[],
  ): string[] {
    const next: string[] = [];
    for (const field of fieldList(key, role)) {
      if (!next.includes(field)) next.push(field);
    }
    for (const raw of learnedFields(learned)) {
      if (!next.includes(raw)) next.push(raw);
    }
    return withAssignedScraperFields(key, role, next);
  }

  async function runProbe(key: string, overrideUrl?: string): Promise<void> {
    const state = probes[key];
    if (state?.loading) return;
    const url = (overrideUrl || state?.url || "").trim();
    if (!url) {
      if (state) state.message = "Paste a link to test.";
      return;
    }
    if (!state) {
      probes[key] = { url, loading: true, fields: [], message: "" };
    } else {
      state.url = url;
      state.loading = true;
      state.message = "";
    }
    const currentState = probes[key];
    try {
      const response = await actions.probeCreatorFields(url, key);
      currentState.fields = response.fields;
      const learned = createCreatorFieldRoles(response.creator_fields || {});
      const targetKey = response.source_key || key;
      if (learned.username.length > 0 || learned.nickname.length > 0 || learned.title.length > 0) {
        const draftRoles = creatorRoles(targetKey);
        for (const role of ["username", "nickname", "title"] as const) {
          draftRoles[role] = mergeLearnedFields(
            targetKey,
            role,
            learned[role] || [],
          );
        }
      }
      currentState.message = response.fields.length ? "" : "No fields found.";
    } catch (error) {
      currentState.fields = [];
      currentState.message = errorMessage(error, "Could not read that link.");
    } finally {
      currentState.loading = false;
    }
  }

  return {
    probes,
    cleanupRules,
    titleLengthRule,
    fieldList,
    fieldListItems,
    addField,
    reorderField,
    resetRole,
    isConfigured,
    ruleEnabled,
    setRule,
    maxChars,
    setMaxChars,
    runProbe,
  };
}

export const useCreatorSettings = useFieldsSettings;
