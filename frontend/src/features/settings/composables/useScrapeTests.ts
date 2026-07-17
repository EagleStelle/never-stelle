import { reactive, watch, type ComputedRef } from "vue";
import { toast } from "vue-sonner";

import { testScrapeRules } from "../../../api";
import type {
  PlatformScrapeRules,
  SavedSettings,
  ScrapeTestResult,
  SourceProfile,
  TokenRole,
} from "../../../types";
import { createScrapeRule, errorMessage, normalizeTokenName } from "../../../utils/dashboard";

interface ScrapeTestState {
  url: string;
  loading: boolean;
  results: ScrapeTestResult[];
  message: string;
}

// Per-source scraper editing: rule mutation, creator-token roles, and the live
// sample-URL test. Owns its own test state so the shell never sees it.
export function useScrapeTests(
  settingsDraft: SavedSettings,
  editableSourceProfiles: ComputedRef<SourceProfile[]>,
) {
  const scrapeTests = reactive<Record<string, ScrapeTestState>>({});

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

  function platformRules(key: string): PlatformScrapeRules {
    return settingsDraft.source_scrape_rules[key];
  }

  function tokenRoles(key: string): Record<string, TokenRole> {
    if (!settingsDraft.source_token_roles[key]) {
      settingsDraft.source_token_roles[key] = {};
    }
    return settingsDraft.source_token_roles[key];
  }

  function isCreatorTokenRole(key: string, token: string): boolean {
    const normalized = normalizeTokenName(token);
    return Boolean(normalized && tokenRoles(key)[normalized] === "creator");
  }

  function setCreatorTokenRole(key: string, token: string, enabled: boolean): void {
    const normalized = normalizeTokenName(token);
    if (!normalized) return;
    const roles = { ...tokenRoles(key) };
    if (enabled) {
      roles[normalized] = "creator";
    } else if (roles[normalized] === "creator") {
      delete roles[normalized];
    }
    settingsDraft.source_token_roles[key] = roles;
  }

  function addScrapeRule(key: string): void {
    platformRules(key).rules.push(createScrapeRule());
  }

  function removeScrapeRule(key: string, index: number): void {
    platformRules(key).rules.splice(index, 1);
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

  return {
    scrapeTests,
    platformRules,
    isCreatorTokenRole,
    setCreatorTokenRole,
    addScrapeRule,
    removeScrapeRule,
    runScrapeTest,
  };
}
