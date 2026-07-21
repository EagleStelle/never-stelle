import { reactive, watch, type ComputedRef } from "vue";
import { toast } from "vue-sonner";

import { testScrapeRules } from "../../../api";
import type {
  PlatformScrapeRules,
  RuntimeSettings,
  SavedSettings,
  ScrapeRule,
  ScrapeTestResult,
  SourceProfile,
} from "../../../types";
import {
  createScrapeRule,
  errorMessage,
  normalizeTokenName,
} from "../../../utils/dashboard";
import { useTokenRoles } from "./useTokenRoles";

interface ScrapeTestState {
  url: string;
  loading: boolean;
  results: ScrapeTestResult[];
  message: string;
}

// Per-source scraper editing: rule mutation and the live sample-URL test. Each rule is
// scoped to one learned format (rule.format); new rules default to the first learned
// format. Token roles come from the shared useTokenRoles so scraper and slug behave alike.
export function useScrapeTests(
  settingsDraft: SavedSettings,
  settings: RuntimeSettings,
  editableSourceProfiles: ComputedRef<SourceProfile[]>,
) {
  const scrapeTests = reactive<Record<string, ScrapeTestState>>({});
  const { tokenRoles, tokenRole, titleRoleOwner, isTitleRoleDisabled, removeCreatorMarker, setTokenRole } =
    useTokenRoles(settingsDraft);

  watch(
    editableSourceProfiles,
    (profiles) => {
      for (const profile of profiles) {
        if (!scrapeTests[profile.key]) {
          scrapeTests[profile.key] = { url: "", loading: false, results: [], message: "" };
        }
      }
    },
    { immediate: true },
  );

  function formatsFor(key: string): string[] {
    return settings.learned_formats?.[key]?.templates || [];
  }

  function platformRules(key: string): PlatformScrapeRules {
    return settingsDraft.source_scrape_rules[key];
  }

  // Rules shown under one format: those tagged with it, plus legacy (formatless) rules
  // folded into the first format — the same target the backend treats them as. The flat
  // index rides along so edit/remove/expand address the one per-source rules list.
  function rulesForFormat(key: string, template: string): { rule: ScrapeRule; index: number }[] {
    const first = formatsFor(key)[0] || "";
    const out: { rule: ScrapeRule; index: number }[] = [];
    platformRules(key).rules.forEach((rule, index) => {
      const belongs = rule.format ? rule.format === template : template === first;
      if (belongs) {
        out.push({ rule, index });
      }
    });
    return out;
  }

  function addScrapeRule(key: string, format: string): void {
    const rulesList = platformRules(key).rules;
    const index = rulesList.length;
    rulesList.push(createScrapeRule({ format, token: `var${index}` }));
  }

  function removeScrapeRule(key: string, index: number): void {
    const [removed] = platformRules(key).rules.splice(index, 1);
    const normalized = normalizeTokenName(removed?.token);
    if (normalized) {
      const roles = { ...tokenRoles(key) };
      const previousRole = roles[normalized];
      delete roles[normalized];
      settingsDraft.source_token_roles[key] = roles;
      removeCreatorMarker(key, normalized, previousRole);
    }
  }

  async function runScrapeTest(key: string): Promise<void> {
    const state = scrapeTests[key];
    const url = state.url.trim();
    if (!url) {
      toast.error("Paste a sample URL to test.");
      return;
    }
    state.loading = true;
    state.message = "";
    try {
      const response = await testScrapeRules(url, key, platformRules(key).rules);
      state.results = response.results;
      state.message = response.fetched
        ? response.results.length
          ? ""
          : "Add a rule to test."
        : response.detail || "Could not fetch that page.";
    } catch (error) {
      state.results = [];
      state.message = errorMessage(error, "Could not test scrape rules.");
    } finally {
      state.loading = false;
    }
  }

  function setRuleToken(siteKey: string, rule: ScrapeRule, index: number, nextName: string): void {
    const prev = rule.token;
    rule.token = nextName;
    const defaultName = `var${index}`;
    const oldToken = prev || defaultName;
    const currentRole = tokenRole(siteKey, oldToken);
    if (currentRole !== "ignore") {
      const newToken = nextName || defaultName;
      if (oldToken !== newToken) {
        setTokenRole(siteKey, oldToken, "ignore");
        setTokenRole(siteKey, newToken, currentRole);
      }
    }
  }

  return {
    scrapeTests,
    formatsFor,
    rulesForFormat,
    platformRules,
    tokenRole,
    titleRoleOwner,
    isTitleRoleDisabled,
    setTokenRole,
    addScrapeRule,
    removeScrapeRule,
    runScrapeTest,
    setRuleToken,
  };
}
