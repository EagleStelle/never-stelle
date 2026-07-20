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
import {
  createScrapeRule,
  errorMessage,
  normalizeTokenName,
  scraperCreatorField,
} from "../../../utils/dashboard";

interface ScrapeTestState {
  url: string;
  loading: boolean;
  results: ScrapeTestResult[];
  message: string;
}

const TEMPLATE_TOKEN_RE = /{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}/g;

// Per-source scraper editing: rule mutation, scraper-token roles, and the live
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

  function tokenRole(key: string, token: string): TokenRole {
    const normalized = normalizeTokenName(token);
    return normalized ? tokenRoles(key)[normalized] || "ignore" : "ignore";
  }

  function titleRoleOwner(key: string): string {
    const entry = Object.entries(tokenRoles(key)).find(([, role]) => role === "title");
    return entry?.[0] || "";
  }

  function isTitleRoleDisabled(key: string, token: string): boolean {
    const normalized = normalizeTokenName(token);
    const owner = titleRoleOwner(key);
    return Boolean(normalized && owner && owner !== normalized);
  }

  function migrateTemplateToken(key: string, token: string, role: TokenRole): void {
    const normalized = normalizeTokenName(token);
    if (!normalized || role === "ignore" || normalized === role) return;
    const templates = settingsDraft.source_templates[key];
    if (!templates) return;
    for (const field of ["folder_template", "filename_template"] as const) {
      templates[field] = String(templates[field] || "").replace(
        TEMPLATE_TOKEN_RE,
        (match, rawToken) =>
          normalizeTokenName(rawToken) === normalized ? `{{${role}}}` : match,
      );
    }
  }

  function removeCreatorMarker(key: string, token: string, role?: TokenRole): void {
    const field = scraperCreatorField(token);
    if (!field) return;
    const roles = settingsDraft.source_creator_fields[key];
    if (!roles) return;
    const targetRoles =
      role === "username" || role === "nickname"
        ? [role]
        : (["username", "nickname"] as const);
    for (const targetRole of targetRoles) {
      roles[targetRole] = (roles[targetRole] || []).filter((value) => value !== field);
    }
  }

  function setTokenRole(key: string, token: string, role: TokenRole): void {
    const normalized = normalizeTokenName(token);
    if (!normalized) return;
    const previousRole = tokenRole(key, normalized);
    const roles = { ...tokenRoles(key) };
    if (role === "title" && isTitleRoleDisabled(key, token)) return;
    if (role === "ignore") {
      delete roles[normalized];
    } else {
      roles[normalized] = role;
    }
    settingsDraft.source_token_roles[key] = roles;
    if (previousRole !== role) removeCreatorMarker(key, normalized, previousRole);
    migrateTemplateToken(key, token, role);
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
