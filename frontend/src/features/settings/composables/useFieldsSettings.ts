import { computed, reactive, watch, type ComputedRef } from "vue";

import type {
  FieldRoles,
  LearnedFormats,
  ProbeFieldsResponse,
  ProbeField,
  RuntimeSettings,
  SavedSettings,
  SourceProfile,
} from "@/types";
import {
  createFieldRoles,
  errorMessage,
  isScraperField,
  normalizeField,
  normalizeTokenName,
  scraperField,
  scraperTokenFromField,
} from "@/utils/dashboard";

export const FIELD_ROLE_KEYS = ["username", "nickname", "title"] as const;
export type FieldRole = (typeof FIELD_ROLE_KEYS)[number];

export const FIELD_ROLE_DEFS: { key: FieldRole; label: string }[] = [
  { key: "username", label: "Username" },
  { key: "nickname", label: "Nickname" },
  { key: "title", label: "Title" },
];

export interface FieldListItem {
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

interface FieldSettingsActions {
  probeFields: (
    url: string,
    sourceKey?: string,
  ) => Promise<ProbeFieldsResponse>;
}

const probes = reactive<Record<string, ProbeState>>({});

// Per-source username/nickname/title field lists and the field probe. Title cleaning and
// filename styling live in useNamingSettings.
export function useFieldsSettings(
  settingsDraft: SavedSettings,
  settings: RuntimeSettings,
  learnedFormatsDraft: LearnedFormats,
  editableSourceProfiles: ComputedRef<SourceProfile[]>,
  actions: FieldSettingsActions,
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

  const globalDefaults = computed<FieldRoles>(() =>
    createFieldRoles(settingsDraft.default_fields || {}),
  );

  function fieldRoles(key: string): FieldRoles {
    if (!settingsDraft.source_fields[key]) {
      settingsDraft.source_fields[key] = { username: [], nickname: [], title: [] };
    }
    return settingsDraft.source_fields[key];
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
    const roles = fieldRoles(key);
    return roles[role]?.length > 0;
  }

  function roleDefaultList(key: string, role: FieldRole): string[] {
    if (sourceFormatsClearedInDraft(key)) return [];
    const globalDefault = globalDefaults.value[role] || [];
    if (globalDefault.length) return globalDefault;
    const learned = settings.source_field_defaults[key]?.[role] || [];
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
      .map((token) => scraperField(token))
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
      if (!values.map(normalizeField).includes(field)) {
        out.push(field);
      }
    }
    for (const value of values) {
      const field = normalizeField(value);
      if (!field) continue;
      if (isScraperField(field) && !assignedSet.has(field)) continue;
      if (!out.includes(field)) out.push(field);
    }
    return out;
  }

  function fieldList(key: string, role: FieldRole): string[] {
    const roles = fieldRoles(key);
    const list = roles[role];
    const base = list.length
      ? list
      : sourceHasDraftRole(key, role)
        ? []
        : roleDefaultList(key, role);
    return withAssignedScraperFields(key, role, base);
  }

  function fieldListItems(key: string, role: FieldRole): FieldListItem[] {
    return fieldList(key, role).map((field) => {
      const scraperToken = scraperTokenFromField(field);
      return {
        key: `field:${field}`,
        label: scraperToken || field,
        scraper: Boolean(scraperToken),
      };
    });
  }

  function savedFieldRoles(key: string, role: FieldRole): string[] {
    return settings.source_fields[key]?.[role] || [];
  }

  function unmodifiedList(key: string, role: FieldRole): string[] {
    const saved = savedFieldRoles(key, role);
    const base = saved.length ? saved : roleDefaultList(key, role);
    return withAssignedScraperFields(key, role, base);
  }

  function commit(key: string, role: FieldRole, list: string[]): void {
    const normalized = withAssignedScraperFields(key, role, list);
    const targetUnmodified = unmodifiedList(key, role);
    const isUnmodified =
      normalized.length === targetUnmodified.length &&
      normalized.every((val, i) => val === targetUnmodified[i]);

    if (isUnmodified) {
      fieldRoles(key)[role] = [...savedFieldRoles(key, role)];
    } else {
      fieldRoles(key)[role] = normalized;
    }
  }

  function addField(key: string, role: FieldRole, raw: string): void {
    const field = normalizeField(raw);
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
    const targetUnmodified = unmodifiedList(key, role);
    commit(key, role, targetUnmodified);
  }

  function isConfigured(key: string, role: FieldRole): boolean {
    const current = fieldList(key, role);
    const targetUnmodified = unmodifiedList(key, role);
    return !(
      current.length === targetUnmodified.length &&
      current.every((val, i) => val === targetUnmodified[i])
    );
  }

  function learnedFields(values: string[]): string[] {
    const out: string[] = [];
    for (const value of values) {
      const field = normalizeField(value);
      if (field && !isScraperField(field) && !out.includes(field)) {
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
      const response = await actions.probeFields(url, key);
      currentState.fields = response.fields;
      const learned = createFieldRoles(response.field_roles || {});
      const targetKey = response.source_key || key;
      if (learned.username.length > 0 || learned.nickname.length > 0 || learned.title.length > 0) {
        const draftRoles = fieldRoles(targetKey);
        for (const role of FIELD_ROLE_KEYS) {
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
    fieldList,
    fieldListItems,
    addField,
    reorderField,
    resetRole,
    isConfigured,
    runProbe,
  };
}

export const useFieldSettings = useFieldsSettings;
