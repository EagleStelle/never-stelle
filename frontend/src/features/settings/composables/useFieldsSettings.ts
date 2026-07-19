import { computed, reactive, watch, type ComputedRef } from "vue";
import { toast } from "vue-sonner";

import { probeCreatorFields } from "../../../api";
import type {
  CreatorFieldCatalog,
  CreatorFieldCatalogItem,
  ProbeField,
  RuntimeSettings,
  SavedSettings,
  SourceProfile,
  TitleCleaningRule,
} from "../../../types";
import { errorMessage, normalizeCreatorField } from "../../../utils/dashboard";

export type CreatorRole = "username" | "nickname";

interface ProbeState {
  url: string;
  loading: boolean;
  fields: ProbeField[];
  message: string;
}

const DEFAULT_MAX_CHARS = 50;

// Per-source username/nickname field lists, title-cleaning toggles, and the field probe.
export function useFieldsSettings(
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
          probes[profile.key] = { url: "", loading: false, fields: [], message: "" };
        }
      }
    },
    { immediate: true },
  );

  const catalog = computed<CreatorFieldCatalog>(() => settings.creator_field_catalog || {});
  const rules = computed<TitleCleaningRule[]>(() => settings.title_cleaning_rules || []);

  // Unique fields across every engine, as add-suggestions when no probe has run yet.
  const suggestions = computed<CreatorFieldCatalogItem[]>(() => {
    const seen = new Set<string>();
    const out: CreatorFieldCatalogItem[] = [];
    for (const items of Object.values(catalog.value)) {
      for (const item of items) {
        if (!seen.has(item.field)) {
          seen.add(item.field);
          out.push(item);
        }
      }
    }
    return out;
  });

  function creatorRoles(key: string) {
    if (!settingsDraft.source_creator_fields[key]) {
      settingsDraft.source_creator_fields[key] = { username: [], nickname: [] };
    }
    return settingsDraft.source_creator_fields[key];
  }

  function fieldList(key: string, role: CreatorRole): string[] {
    return creatorRoles(key)[role];
  }

  function addField(key: string, role: CreatorRole, raw: string): void {
    const field = normalizeCreatorField(raw);
    if (!field) return;
    const list = creatorRoles(key)[role];
    if (!list.includes(field)) list.push(field);
  }

  function removeField(key: string, role: CreatorRole, index: number): void {
    creatorRoles(key)[role].splice(index, 1);
  }

  function moveField(key: string, role: CreatorRole, index: number, delta: number): void {
    const list = creatorRoles(key)[role];
    const target = index + delta;
    if (target < 0 || target >= list.length) return;
    const [item] = list.splice(index, 1);
    list.splice(target, 0, item);
  }

  function cleaningFlags(key: string): Record<string, boolean | number> {
    if (!settingsDraft.source_title_cleaning[key]) settingsDraft.source_title_cleaning[key] = {};
    return settingsDraft.source_title_cleaning[key];
  }

  function ruleEnabled(key: string, rule: TitleCleaningRule): boolean {
    const stored = cleaningFlags(key)[rule.key];
    return typeof stored === "boolean" ? stored : rule.default;
  }

  function setRule(key: string, rule: TitleCleaningRule, enabled: boolean): void {
    cleaningFlags(key)[rule.key] = enabled;
  }

  function maxChars(key: string): number {
    const value = cleaningFlags(key).max_chars;
    return typeof value === "number" && value > 0 ? value : DEFAULT_MAX_CHARS;
  }

  function setMaxChars(key: string, value: number): void {
    const parsed = Math.floor(Number(value));
    if (Number.isFinite(parsed) && parsed > 0) cleaningFlags(key).max_chars = parsed;
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
    catalog,
    rules,
    suggestions,
    fieldList,
    addField,
    removeField,
    moveField,
    ruleEnabled,
    setRule,
    maxChars,
    setMaxChars,
    runProbe,
  };
}
