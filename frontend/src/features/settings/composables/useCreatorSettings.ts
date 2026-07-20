import { computed, reactive, watch, type ComputedRef } from "vue";
import { toast } from "vue-sonner";

import { probeCreatorFields } from "../../../api";
import type {
  CreatorFieldRoles,
  ProbeField,
  RuntimeSettings,
  SavedSettings,
  SourceProfile,
  TitleCleaningRule,
} from "../../../types";
import {
  createCreatorFieldRoles,
  errorMessage,
  normalizeCreatorField,
} from "../../../utils/dashboard";

export type CreatorRole = "username" | "nickname";

interface ProbeState {
  url: string;
  loading: boolean;
  fields: ProbeField[];
  message: string;
}

const DEFAULT_MAX_CHARS = 100;
const FALLBACK_TITLE_LENGTH_RULE: TitleCleaningRule = {
  key: "shorten",
  label: "Limit overly long titles",
  default: false,
};

// Per-source username/nickname field lists, title-cleaning toggles, and the field probe.
export function useCreatorSettings(
  settingsDraft: SavedSettings,
  settings: RuntimeSettings,
  editableSourceProfiles: ComputedRef<SourceProfile[]>,
) {
  const probes = reactive<Record<string, ProbeState>>({});

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

  // The ordered default field list per role; an empty per-source list tracks this.
  const defaults = computed<CreatorFieldRoles>(() => ({
    username: settings.creator_field_defaults?.username || [],
    nickname: settings.creator_field_defaults?.nickname || [],
  }));

  function creatorRoles(key: string): CreatorFieldRoles {
    if (!settingsDraft.source_creator_fields[key]) {
      settingsDraft.source_creator_fields[key] = { username: [], nickname: [] };
    }
    return settingsDraft.source_creator_fields[key];
  }

  function sourceHasSavedCreatorFields(key: string): boolean {
    const roles = settings.source_creator_fields[key];
    return Boolean(
      roles && (roles.username.length > 0 || roles.nickname.length > 0),
    );
  }

  function sourceHasDraftCreatorFields(key: string): boolean {
    const roles = creatorRoles(key);
    return roles.username.length > 0 || roles.nickname.length > 0;
  }

  function roleDefaultList(key: string, role: CreatorRole): string[] {
    if (sourceHasSavedCreatorFields(key)) {
      return settings.source_creator_fields[key]?.[role] || [];
    }
    return defaults.value[role];
  }

  // Shown list: source-learned fields once configured, otherwise the cold-start default.
  function fieldList(key: string, role: CreatorRole): string[] {
    const roles = creatorRoles(key);
    const list = roles[role];
    if (list.length) return list;
    return sourceHasDraftCreatorFields(key) ? [] : roleDefaultList(key, role);
  }

  function sameAsDefault(
    key: string,
    role: CreatorRole,
    list: string[],
  ): boolean {
    const base = roleDefaultList(key, role);
    return (
      list.length === base.length &&
      list.every((value, index) => value === base[index])
    );
  }

  // Persist changed order. Unlearned sources can keep tracking global defaults with an empty list;
  // learned sources must retain their real fields so a save never erases the probe result.
  function commit(key: string, role: CreatorRole, list: string[]): void {
    creatorRoles(key)[role] =
      sameAsDefault(key, role, list) && !sourceHasSavedCreatorFields(key)
        ? []
        : list;
  }

  function addField(key: string, role: CreatorRole, raw: string): void {
    const field = normalizeCreatorField(raw);
    if (!field) return;
    const list = [...fieldList(key, role)];
    if (list.includes(field)) return;
    list.push(field);
    commit(key, role, list);
  }

  function reorderField(
    key: string,
    role: CreatorRole,
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

  function resetRole(key: string, role: CreatorRole): void {
    creatorRoles(key)[role] = sourceHasSavedCreatorFields(key)
      ? [...roleDefaultList(key, role)]
      : [];
  }

  // True once the source order differs from its learned/default order.
  function isConfigured(key: string, role: CreatorRole): boolean {
    const list = creatorRoles(key)[role];
    return list.length > 0 && !sameAsDefault(key, role, list);
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

  async function runProbe(key: string): Promise<void> {
    const state = probes[key];
    if (state.loading) return;
    const url = state.url.trim();
    if (!url) {
      toast.error("Paste a link to test.");
      return;
    }
    state.loading = true;
    state.message = "";
    try {
      const response = await probeCreatorFields(url, key);
      state.fields = response.fields;
      const learned = createCreatorFieldRoles(response.creator_fields || {});
      if (learned.username.length || learned.nickname.length) {
        const targetKey = response.source_key || key;
        const draftRoles = creatorRoles(targetKey);
        draftRoles.username = learned.username;
        draftRoles.nickname = learned.nickname;
        settings.source_creator_fields[targetKey] = {
          username: [...learned.username],
          nickname: [...learned.nickname],
        };
      }
      state.message = response.fields.length ? "" : "No creator fields found.";
    } catch (error) {
      state.fields = [];
      state.message = errorMessage(error, "Could not read that link.");
    } finally {
      state.loading = false;
    }
  }

  return {
    probes,
    cleanupRules,
    titleLengthRule,
    fieldList,
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
