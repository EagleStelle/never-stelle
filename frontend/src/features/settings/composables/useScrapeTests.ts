import { reactive, watch, type ComputedRef } from "vue";
import { toast } from "vue-sonner";

import { testScrapeRules } from "../../../api";
import type {
  PlatformScrapeRules,
  SavedSettings,
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

// Per-source scraper editing: rule mutation and the live sample-URL test. Token roles
// come from the shared useTokenRoles so scraper and slug tokens behave identically.
export function useScrapeTests(
  settingsDraft: SavedSettings,
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

  function platformRules(key: string): PlatformScrapeRules {
    return settingsDraft.source_scrape_rules[key];
  }

  function addScrapeRule(key: string): void {
    platformRules(key).rules.push(createScrapeRule());
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

  return {
    scrapeTests,
    platformRules,
    tokenRole,
    titleRoleOwner,
    isTitleRoleDisabled,
    setTokenRole,
    addScrapeRule,
    removeScrapeRule,
    runScrapeTest,
  };
}
