import { computed } from "vue";

import type {
  NamingChoice,
  NamingFlagValue,
  RuntimeSettings,
  SavedSettings,
  TitleCleaningRule,
} from "../../../types";

const DEFAULT_MAX_CHARS = 100;
const TITLE_MAX_CHARS_KEY = "max_chars";
const STEM_MAX_CHARS_KEY = "stem_max_chars";
// Served by the backend, but a source can be edited before the settings response lands.
const FALLBACK_TITLE_LENGTH_RULE: TitleCleaningRule = {
  key: "shorten",
  label: "Limit overly long titles",
  default: false,
};

// Per-source title cleaning and filename styling.
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

  function flags(key: string): Record<string, NamingFlagValue> {
    if (!settingsDraft.source_title_cleaning[key])
      settingsDraft.source_title_cleaning[key] = {};
    return settingsDraft.source_title_cleaning[key];
  }

  function ruleEnabled(key: string, rule: TitleCleaningRule): boolean {
    const stored = flags(key)[rule.key];
    return typeof stored === "boolean" ? stored : rule.default;
  }

  function setRule(
    key: string,
    rule: TitleCleaningRule,
    enabled: boolean,
  ): void {
    flags(key)[rule.key] = enabled;
  }

  function positiveInteger(value: NamingFlagValue): number {
    const parsed = Math.floor(Number(value));
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function numberFlag(key: string, flagKey: string, fallback: number): number {
    return positiveInteger(flags(key)[flagKey]) || fallback;
  }

  function setNumberFlag(
    key: string,
    flagKey: string,
    value: NamingFlagValue,
    clearWhenUnset = false,
  ): void {
    const source = flags(key);
    const parsed = positiveInteger(value);
    if (parsed > 0) source[flagKey] = parsed;
    else if (clearWhenUnset) delete source[flagKey];
  }

  function maxChars(key: string): number {
    return numberFlag(key, TITLE_MAX_CHARS_KEY, DEFAULT_MAX_CHARS);
  }

  function setMaxChars(key: string, value: number): void {
    setNumberFlag(key, TITLE_MAX_CHARS_KEY, value);
  }

  // 0 means no whole-stem cap, unlike max_chars which always has a default.
  function stemMaxChars(key: string): number {
    return numberFlag(key, STEM_MAX_CHARS_KEY, 0);
  }

  function setStemMaxChars(key: string, value: number): void {
    setNumberFlag(key, STEM_MAX_CHARS_KEY, value, true);
  }

  function choiceValue(key: string, choice: NamingChoice): string {
    const stored = flags(key)[choice.key];
    return typeof stored === "string" && stored ? stored : choice.default;
  }

  function setChoice(key: string, choice: NamingChoice, value: string): void {
    if (value === choice.default) delete flags(key)[choice.key];
    else flags(key)[choice.key] = value;
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
