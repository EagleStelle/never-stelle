import type { SavedSettings, TokenRole } from "@/types";
import { normalizeTokenName, scraperField } from "@/utils/dashboard";

const TEMPLATE_TOKEN_RE = /{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}/g;

// Per-source token roles shared by every named-token subsystem. Scraper HTML tokens
// and slug URL-part tokens live in one namespace, and Fields order decides priority.
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

  function isRoleDisabled(_key: string, _token: string, _role: TokenRole): boolean {
    return false;
  }

  function applyTemplateTokenRole(key: string, token: string, role: TokenRole): void {
    const normalized = normalizeTokenName(token);
    if (!normalized || role === "ignore" || role === "creator" || normalized === role) return;
    const formats = settingsDraft.source_templates[key];
    if (!formats) return;
    for (const format of Object.keys(formats)) {
      const templateSettings = formats[format];
      for (const field of ["folder_template", "filename_template"] as const) {
        templateSettings[field] = String(templateSettings[field] || "").replace(
          TEMPLATE_TOKEN_RE,
          (match, rawToken) =>
            normalizeTokenName(rawToken) === normalized ? `{{${role}}}` : match,
        );
      }
    }
  }

  function removeFieldMarker(key: string, token: string, role?: TokenRole): void {
    const field = scraperField(token);
    if (!field) return;
    const roles = settingsDraft.source_fields[key];
    if (!roles) return;
    const targetRoles =
      role === "creator" || !role
        ? (["username", "nickname"] as const)
        : role === "username" || role === "nickname"
        ? [role]
        : ([] as const);
    for (const targetRole of targetRoles) {
      roles[targetRole] = (roles[targetRole] || []).filter((value) => value !== field);
    }
  }

  function setTokenRole(key: string, token: string, role: TokenRole): void {
    const normalized = normalizeTokenName(token);
    if (!normalized) return;
    const previousRole = tokenRole(key, normalized);
    const roles = { ...tokenRoles(key) };
    if (role === "ignore") {
      delete roles[normalized];
    } else {
      roles[normalized] = role;
    }
    settingsDraft.source_token_roles[key] = roles;
    if (previousRole !== role) removeFieldMarker(key, normalized, previousRole);
    applyTemplateTokenRole(key, token, role);
  }

  return {
    tokenRoles,
    tokenRole,
    isRoleDisabled,
    removeFieldMarker,
    setTokenRole,
  };
}
