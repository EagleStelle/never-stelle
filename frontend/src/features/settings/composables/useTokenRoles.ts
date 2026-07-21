import type { SavedSettings, TokenRole } from "../../../types";
import {
  normalizeTokenName,
  scraperCreatorField,
} from "../../../utils/dashboard";

const TEMPLATE_TOKEN_RE = /{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}/g;

// Per-source token roles shared by every named-token subsystem (scraper HTML tokens
// and slug URL-part tokens live in one namespace). Owns role assignment, the single
// title-owner rule, template {{token}}→{{role}} migration, and creator-field markers.
export function useTokenRoles(settingsDraft: SavedSettings) {
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

  return {
    tokenRoles,
    tokenRole,
    titleRoleOwner,
    isTitleRoleDisabled,
    removeCreatorMarker,
    setTokenRole,
  };
}
