import { computed } from "vue";

import type {
  NamingChoice,
  NamingFlagValue,
  RuntimeSettings,
  SavedSettings,
  TitleCleaningRule,
} from "@/types";

const DEFAULT_MAX_CHARS = 100;
const TITLE_MAX_CHARS_KEY = "max_chars";
const STEM_MAX_CHARS_KEY = "stem_max_chars";
// The Defaults pane edits the global flags; every other pane passes a source key.
export const GLOBAL_NAMING_KEY = "";
// Served by the backend, but a source can be edited before the settings response lands.
const FALLBACK_TITLE_LENGTH_RULE: TitleCleaningRule = {
  key: "shorten",
  label: "Limit overly long titles",
  default: false,
};

// Title cleaning and filename styling. Pass a source key, or GLOBAL_NAMING_KEY to edit
// the global defaults every source falls back to.
export function useNamingSettings(
  settingsDraft: SavedSettings,
  settings: RuntimeSettings,
) {
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
  const namingChoices = computed<NamingChoice[]>(
    () => settings.naming_choices || [],
  );

  function isGlobal(key: string): boolean {
    return key === GLOBAL_NAMING_KEY;
  }

  function flags(key: string): Record<string, NamingFlagValue> {
    if (isGlobal(key)) return settingsDraft.default_naming;
    if (!settingsDraft.source_title_cleaning[key])
      settingsDraft.source_title_cleaning[key] = {};
    return settingsDraft.source_title_cleaning[key];
  }

  // What an unset flag resolves to: the built-in for the global pane, the configured
  // global default for a source.
  function inherited(
    key: string,
    flagKey: string,
    builtin: NamingFlagValue,
  ): NamingFlagValue {
    if (isGlobal(key)) return builtin;
    const stored = settingsDraft.default_naming[flagKey];
    return stored === undefined ? builtin : stored;
  }

  function ruleEnabled(key: string, rule: TitleCleaningRule): boolean {
    const stored = flags(key)[rule.key];
    if (typeof stored === "boolean") return stored;
    return Boolean(inherited(key, rule.key, rule.default));
  }

  // Writing back the inherited value clears the override, so the flag keeps following it.
  function setFlag(key: string, flagKey: string, value: NamingFlagValue, builtin: NamingFlagValue): void {
    const source = flags(key);
    if (value === inherited(key, flagKey, builtin)) delete source[flagKey];
    else source[flagKey] = value;
  }

  function setRule(
    key: string,
    rule: TitleCleaningRule,
    enabled: boolean,
  ): void {
    setFlag(key, rule.key, enabled, rule.default);
  }

  function integerValue(
    value: NamingFlagValue | undefined,
    allowZero = false,
  ): number | null {
    const parsed = Math.floor(Number(value));
    if (!Number.isFinite(parsed)) return null;
    if (parsed > 0 || (allowZero && parsed === 0)) return parsed;
    return null;
  }

  function numberFlag(
    key: string,
    flagKey: string,
    builtin: number,
    allowZero = false,
  ): number {
    const stored = integerValue(flags(key)[flagKey], allowZero);
    if (stored !== null) return stored;
    const fallback = integerValue(inherited(key, flagKey, builtin), allowZero);
    return fallback === null ? builtin : fallback;
  }

  function setNumberFlag(
    key: string,
    flagKey: string,
    value: NamingFlagValue,
    builtin: number,
    allowZero = false,
    clearWhenUnset = false,
  ): void {
    const source = flags(key);
    const parsed = integerValue(value, allowZero);
    if (parsed !== null) setFlag(key, flagKey, parsed, builtin);
    else if (clearWhenUnset) delete source[flagKey];
  }

  function maxChars(key: string): number {
    return numberFlag(key, TITLE_MAX_CHARS_KEY, DEFAULT_MAX_CHARS);
  }

  function setMaxChars(key: string, value: number): void {
    setNumberFlag(key, TITLE_MAX_CHARS_KEY, value, DEFAULT_MAX_CHARS);
  }

  // 0 means no whole-stem cap, unlike max_chars which always has a default.
  function stemMaxChars(key: string): number {
    return numberFlag(key, STEM_MAX_CHARS_KEY, 0, true);
  }

  function setStemMaxChars(key: string, value: number): void {
    setNumberFlag(key, STEM_MAX_CHARS_KEY, value, 0, true, true);
  }

  function choiceValue(key: string, choice: NamingChoice): string {
    const stored = flags(key)[choice.key];
    if (typeof stored === "string" && stored) return stored;
    return String(inherited(key, choice.key, choice.default) || choice.default);
  }

  function setChoice(key: string, choice: NamingChoice, value: string): void {
    setFlag(key, choice.key, value, choice.default);
  }

  return {
    cleanupRules,
    titleLengthRule,
    namingChoices,
    ruleEnabled,
    setRule,
    maxChars,
    setMaxChars,
    stemMaxChars,
    setStemMaxChars,
    choiceValue,
    setChoice,
  };
}
