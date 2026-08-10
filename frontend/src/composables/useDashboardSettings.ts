import { computed, nextTick, reactive, ref, watch } from "vue";
import { useMutation, useQuery, useQueryClient } from "@tanstack/vue-query";

import {
  deletePlatformCookies,
  getUiConfig,
  learnPlatformFormat,
  probeFields as probeFieldsRequest,
  saveSettings,
  setFormatTemplates,
  reorderPlatformCookies,
  uploadPlatformCookies,
} from "@/api";
import { useAuth } from "@/composables/useAuth";
import { DEFAULT_SOURCE_PROFILES, UI_CONFIG_QUERY_KEY } from "@/ui";
import { FIELD_ROLE_KEYS } from "@/features/settings/composables/useFieldsSettings";
import type {
  CookiePolicy,
  CookiePolicyField,
  CookiesMap,
  CookiesStatus,
  SourceCookiePolicies,
  FieldRoles,
  LearnedFormats,
  NamingDefaults,
  ProbeFieldsResponse,
  RuntimeSettings,
  SavedSettings,
  SettingsDraft,
  SettingsSection,
  SourceFields,
  SourceLocations,
  SourceProfile,
  SourceSlugTokens,
  SourceTemplates,
  SourceTitleCleaning,
  SourceTokenRoles,
  ToastType,
  UiConfigResponse,
} from "@/types";
import {
  createCookiePolicy,
  createCookiePolicyDefaults,
  createCookiesStatus,
  createNamingFlags,
  createPostProcessingSelection,
  createSourceCookiePolicies,
  createQualityOptions,
  createFieldRoles,
  createQualitySelection,
  createSourceFields,
  createSourceLocationOptions,
  createSourceLocations,
  createSourceProfile,
  createSourceScrapeRules,
  createSourceSlugTokens,
  createSourceTemplates,
  createSourceTitleCleaning,
  createSourceTokenRoles,
  createTemplateSettings,
  displayHost,
  errorMessage,
  faviconUrlForHost,
  hostFromUrl,
  mergeSourceProfiles,
  normalizeSourceKey,
  settingsManagedSourceProfiles,
  sourceLabelFromKey,
} from "@/utils/dashboard";

function replaceRecord<T>(
  target: Record<string, T>,
  source: Record<string, T>,
): void {
  for (const key of Object.keys(target)) delete target[key];
  Object.assign(target, source);
}

// A cookie policy holds only the fields it overrides, so stale keys must go.
function replaceCookiePolicy(target: CookiePolicy, source: CookiePolicy): void {
  for (const key of Object.keys(target) as CookiePolicyField[]) delete target[key];
  Object.assign(target, source);
}

function replaceProfiles(
  target: SourceProfile[],
  source: SourceProfile[],
): void {
  target.splice(
    0,
    target.length,
    ...source.map((profile) => createSourceProfile(profile)),
  );
}

function cloneJson<T>(source: T): T {
  return JSON.parse(JSON.stringify(source)) as T;
}

function sameJson(left: unknown, right: unknown): boolean {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}

function stableValue(source: unknown): unknown {
  if (Array.isArray(source)) return source.map((item) => stableValue(item));
  if (!source || typeof source !== "object") return source;
  return Object.fromEntries(
    Object.keys(source as Record<string, unknown>)
      .sort()
      .map((key) => [key, stableValue((source as Record<string, unknown>)[key])]),
  );
}

function replaceLearnedFormats(
  target: LearnedFormats,
  source: LearnedFormats = {},
): void {
  for (const key of Object.keys(target)) delete target[key];
  Object.assign(target, cloneJson(source));
}

function recordForProfiles<T>(
  source: Record<string, T> = {},
  profiles: SourceProfile[],
): Record<string, T> {
  const keys = new Set(profiles.map((profile) => profile.key));
  return Object.fromEntries(
    Object.entries(source)
      .map(([key, value]) => [normalizeSourceKey(key), value] as const)
      .filter(([key]) => key && keys.has(key)),
  ) as Record<string, T>;
}

function createCookiesMap(
  source: Record<string, Partial<CookiesStatus>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): CookiesMap {
  const normalizedSource = Object.fromEntries(
    Object.entries(source)
      .map(([key, value]) => [normalizeSourceKey(key), value] as const)
      .filter(([key]) => key),
  ) as Record<string, Partial<CookiesStatus>>;
  const keys = new Set([
    ...profiles.map((profile) => profile.key),
    ...Object.keys(normalizedSource),
  ]);
  return Object.fromEntries(
    [...keys].map((key) => [
      key,
      createCookiesStatus(normalizedSource[key] || {}),
    ]),
  ) as CookiesMap;
}

interface UseDashboardSettingsOptions {
  toast: (message: string, type?: ToastType) => void;
}

interface PendingFormatLearn {
  url: string;
  sourceKey: string;
  template: string;
}

interface PendingCookieUpload {
  id: string;
  file: File;
}

const PENDING_COOKIE_PREFIX = "pending:";
let pendingCookieSeq = 0;

function cookieUploadStamp(): string {
  // Mirrors the backend rename: UTC `<date> <time>`, dashes instead of colons.
  return new Date().toISOString().slice(0, 19).replace("T", " ").replace(/:/g, "-");
}

function nextCookieFilename(sourceKey: string, taken: Set<string>): string {
  const stem = `${cookieUploadStamp()} ${normalizeSourceKey(sourceKey)}`.trim() || "cookies";
  let candidate = `${stem}.txt`;
  let suffix = 2;
  while (taken.has(candidate)) {
    candidate = `${stem} (${suffix}).txt`;
    suffix += 1;
  }
  taken.add(candidate);
  return candidate;
}

const ID_TOKEN = "{id}";
const CREATOR_TOKEN = "{creator}";
const USERNAME_TOKEN = "{username}";
const NICKNAME_TOKEN = "{nickname}";
const VAR_TOKEN = "{var}";
const COMMON_SECOND_LEVEL_TLDS = new Set(["ac", "co", "com", "edu", "gov", "net", "org"]);
const STATIC_ROUTE_SEGMENTS = new Set([
  "album",
  "albums",
  "clip",
  "clips",
  "media",
  "p",
  "photo",
  "photos",
  "post",
  "posts",
  "reel",
  "reels",
  "share",
  "short",
  "shorts",
  "status",
  "story",
  "stories",
  "v",
  "video",
  "videos",
  "view",
  "watch",
]);
const ROLE_TOKENS = new Set([CREATOR_TOKEN, USERNAME_TOKEN, NICKNAME_TOKEN]);

function safeDecode(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function preparedUrl(value: string): string {
  const url = String(value || "").trim();
  return url && !url.includes("://") ? `https://${url}` : url;
}

function domainStem(host: string): string {
  const parts = displayHost(host).split(".").filter(Boolean);
  if (!parts.length) return "";
  if (parts.length >= 3 && parts.at(-1)?.length === 2 && COMMON_SECOND_LEVEL_TLDS.has(parts.at(-2) || "")) {
    return parts.at(-3) || "";
  }
  return parts.length >= 2 ? parts.at(-2) || "" : parts[0];
}

function sourceKeyFromUrlDraft(url: string, profiles: SourceProfile[]): string {
  const host = hostFromUrl(url);
  if (!host) return "";
  for (const profile of profiles) {
    for (const pattern of profile.hosts || []) {
      const normalized = hostFromUrl(String(pattern).replace(/^\*\./, ""));
      if (normalized && (host === normalized || host.endsWith(`.${normalized}`))) {
        return normalizeSourceKey(profile.key);
      }
    }
  }
  return normalizeSourceKey(domainStem(host));
}

function sourceProfileFromUrlDraft(url: string, profiles: SourceProfile[]): SourceProfile | null {
  const host = hostFromUrl(url);
  const key = sourceKeyFromUrlDraft(url, profiles);
  if (!host || !key) return null;
  const existing = profiles.find((profile) => normalizeSourceKey(profile.key) === key);
  if (existing) {
    return createSourceProfile({
      ...existing,
      hosts: [...new Set([...(existing.hosts || []), host])],
      icon_url: existing.icon_url || faviconUrlForHost(host),
    });
  }
  return createSourceProfile({
    key,
    label: sourceLabelFromKey(key),
    hosts: [host],
    icon_url: faviconUrlForHost(host),
    settings_managed: true,
  });
}

function isStaticRouteSegment(value: string): boolean {
  return STATIC_ROUTE_SEGMENTS.has(safeDecode(String(value || "")).trim().toLowerCase());
}

function withoutAt(value: string): string {
  return String(value || "").replace(/^@/, "");
}

function isRoleToken(value: string): boolean {
  return ROLE_TOKENS.has(withoutAt(value));
}

function isVarToken(value: string): boolean {
  return withoutAt(value) === VAR_TOKEN;
}

function canonicalDraftUrl(sourceUrl: string): URL | null {
  try {
    const parsed = new URL(preparedUrl(sourceUrl));
    parsed.protocol = parsed.protocol.toLowerCase();
    parsed.hostname = parsed.hostname.toLowerCase();
    parsed.hash = "";
    parsed.pathname = parsed.pathname.replace(/\/+/g, "/");
    if (parsed.pathname !== "/") parsed.pathname = parsed.pathname.replace(/\/+$/, "");
    return parsed;
  } catch {
    return null;
  }
}

function describeDraftTemplates(templates: string[]): LearnedFormats[string] {
  const seen = new Map<string, LearnedFormats[string]["segments"][number]>();
  const segments: LearnedFormats[string]["segments"] = [];
  for (const template of templates) {
    const parsed = canonicalDraftUrl(template);
    if (!parsed) continue;
    const rawSegments = parsed.pathname.split("/").filter(Boolean);
    for (let index = 0; index < rawSegments.length; index += 1) {
      const segment = rawSegments[index];
      const decoded = safeDecode(segment);
      const part = `path:${index}`;
      let item: LearnedFormats[string]["segments"][number];
      if (decoded === ID_TOKEN) item = { part, label: ID_TOKEN, kind: "id", reserved: true };
      else if (isRoleToken(decoded)) {
        const kind = withoutAt(decoded).replace(/[{}]/g, "") as "creator" | "username" | "nickname";
        item = { part, label: `{${kind}}`, kind, reserved: true };
      } else if (isVarToken(decoded)) {
        item = { part, label: VAR_TOKEN, kind: "var", reserved: false };
      } else {
        item = {
          part,
          label: decoded,
          kind: "literal",
          reserved: isStaticRouteSegment(segment),
        };
      }
      if (!seen.has(part)) {
        seen.set(part, item);
        segments.push(item);
      } else if (item.reserved && !seen.get(part)?.reserved) {
        Object.assign(seen.get(part)!, item);
      }
    }
    for (const [key, value] of parsed.searchParams.entries()) {
      const part = `query:${key}`;
      const item: LearnedFormats[string]["segments"][number] =
        value === ID_TOKEN
          ? { part, label: `${key}=${ID_TOKEN}`, kind: "id", reserved: true }
          : isRoleToken(value)
            ? {
                part,
                label: `${key}=${withoutAt(value)}`,
                kind: withoutAt(value).replace(/[{}]/g, "") as "creator" | "username" | "nickname",
                reserved: true,
              }
            : isVarToken(value)
              ? { part, label: `${key}=${VAR_TOKEN}`, kind: "var", reserved: false }
          : { part, label: `${key}=${value}`, kind: "query", reserved: false };
      if (!seen.has(part)) {
        seen.set(part, item);
        segments.push(item);
      }
    }
  }
  return { templates, segments };
}

function pendingTemplatesForKey(
  pendingLearns: Record<string, PendingFormatLearn>,
  sourceKey: string,
): Set<string> {
  const key = normalizeSourceKey(sourceKey);
  return new Set(
    Object.values(pendingLearns)
      .filter((item) => normalizeSourceKey(item.sourceKey) === key)
      .map((item) => item.template),
  );
}

export function useDashboardSettings({ toast }: UseDashboardSettingsOptions) {
  const auth = useAuth();
  const queryClient = useQueryClient();
  const defaults = reactive<SavedSettings>({
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    source_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
    default_post_processing: createPostProcessingSelection(),
    source_scrape_rules: createSourceScrapeRules(),
    source_token_roles: createSourceTokenRoles(),
    source_slug_tokens: createSourceSlugTokens(),
    source_fields: createSourceFields(),
    source_title_cleaning: createSourceTitleCleaning(),
    source_cookie_policies: createSourceCookiePolicies(),
    default_cookie_policy: createCookiePolicy(),
    default_fields: createFieldRoles(),
    default_naming: createNamingFlags(),
  });
  const settings = reactive<RuntimeSettings>({
    auth: { username: "", password_configured: false },
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    source_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
    default_post_processing: createPostProcessingSelection(),
    source_scrape_rules: createSourceScrapeRules(),
    source_token_roles: createSourceTokenRoles(),
    source_slug_tokens: createSourceSlugTokens(),
    source_fields: createSourceFields(),
    source_title_cleaning: createSourceTitleCleaning(),
    source_cookie_policies: createSourceCookiePolicies(),
    default_cookie_policy: createCookiePolicy(),
    default_fields: createFieldRoles(),
    default_naming: createNamingFlags(),
    media_root: "",
    source_location_options: {},
    ytdlp_cookies: createCookiesMap(),
    cookie_policy_defaults: createCookiePolicyDefaults(),
    quality_options: createQualityOptions(),
    template_tokens: [],
    field_defaults: { username: [], nickname: [], title: [] },
    source_field_defaults: createSourceFields(),
    title_cleaning_rules: [],
    naming_choices: [],
    learned_formats: {},
  });
  const settingsDraft = reactive<SettingsDraft>({
    account: {
      username: "",
      current_password: "",
      new_password: "",
      confirm_password: "",
    },
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    source_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
    default_post_processing: createPostProcessingSelection(),
    source_scrape_rules: createSourceScrapeRules(),
    source_token_roles: createSourceTokenRoles(),
    source_slug_tokens: createSourceSlugTokens(),
    source_fields: createSourceFields(),
    source_title_cleaning: createSourceTitleCleaning(),
    source_cookie_policies: createSourceCookiePolicies(),
    default_cookie_policy: createCookiePolicy(),
    default_fields: createFieldRoles(),
    default_naming: createNamingFlags(),
  });
  const learnedFormatsDraft = reactive<LearnedFormats>({});
  // A source keeps a list of jars, so uploads stack and deletes name one jar.
  const pendingCookieUploads = reactive<Record<string, PendingCookieUpload[]>>({});
  const pendingCookieDeletes = reactive<Record<string, string[]>>({});
  const pendingFormatLearns = reactive<Record<string, PendingFormatLearn>>({});

  const settingsOpen = ref(false);
  const settingsSection = ref<SettingsSection>("locations");
  const lastFocusedTrigger = ref<HTMLElement | null>(null);
  let lastSavedSnapshot = "";
  let lastDraftSnapshot = "";
  let lastFormatDraftSnapshot = "";

  const uiConfigQuery = useQuery<UiConfigResponse>({
    queryKey: UI_CONFIG_QUERY_KEY,
    queryFn: getUiConfig,
    enabled: auth.authenticated,
    staleTime: 60_000,
  });
  const saveSettingsMutation = useMutation({ mutationFn: saveSettings });
  const uploadCookiesMutation = useMutation({
    mutationFn: ({ platform, file }: { platform: string; file: File }) =>
      uploadPlatformCookies(platform, file),
  });
  const deleteCookiesMutation = useMutation({
    mutationFn: ({ platform, cookieId }: { platform: string; cookieId: string }) =>
      deletePlatformCookies(platform, cookieId),
  });
  const reorderCookiesMutation = useMutation({
    mutationFn: ({ platform, cookieIds }: { platform: string; cookieIds: string[] }) =>
      reorderPlatformCookies(platform, cookieIds),
  });
  const learnFormatMutation = useMutation({
    mutationFn: (url: string) => learnPlatformFormat(url),
  });
  const probeFieldsMutation = useMutation({
    mutationFn: ({ url, sourceKey }: { url: string; sourceKey: string }) =>
      probeFieldsRequest(url, sourceKey),
  });
  const formatTemplatesMutation = useMutation({
    mutationFn: ({ sourceKey, templates }: { sourceKey: string; templates: string[] }) =>
      setFormatTemplates(sourceKey, templates),
  });

  const sourceProfiles = computed<SourceProfile[]>(() =>
    mergeSourceProfiles(settings.source_profiles),
  );
  const savedSettings = computed<SavedSettings>(() => getSavedSettings());
  const cookieStatuses = computed<CookiesMap>(() => {
    const statuses = createCookiesMap(settings.ytdlp_cookies, sourceProfiles.value);
    for (const [key, ids] of Object.entries(pendingCookieDeletes)) {
      const removed = new Set(ids);
      statuses[key] = createCookiesStatus({
        cookies: (statuses[key]?.cookies || []).filter((cookie) => !removed.has(cookie.id)),
      });
    }
    for (const [key, uploads] of Object.entries(pendingCookieUploads)) {
      const existing = statuses[key]?.cookies || [];
      const taken = new Set(existing.map((cookie) => cookie.filename));
      statuses[key] = createCookiesStatus({
        cookies: [
          ...existing,
          ...uploads.map((upload) => ({
            id: upload.id,
            filename: nextCookieFilename(key, taken),
            uploaded_at: "",
          })),
        ],
      });
    }
    return statuses;
  });

  function normalizeSavedPayload(source: SavedSettings): SavedSettings {
    const profiles = settingsManagedSourceProfiles(
      mergeSourceProfiles(DEFAULT_SOURCE_PROFILES, source.source_profiles),
    );
    const templateSettings = createTemplateSettings(source.template_settings);
    return {
      source_profiles: profiles,
      source_locations: createSourceLocations(
        recordForProfiles(source.source_locations, profiles),
        profiles,
      ),
      template_settings: templateSettings,
      source_templates: createSourceTemplates(
        recordForProfiles(source.source_templates, profiles),
        profiles,
        templateSettings,
      ),
      default_quality: createQualitySelection(
        source.default_quality,
        settings.quality_options,
      ),
      default_post_processing: createPostProcessingSelection(
        source.default_post_processing,
      ),
      source_scrape_rules: createSourceScrapeRules(
        recordForProfiles(source.source_scrape_rules, profiles),
        profiles,
      ),
      source_token_roles: createSourceTokenRoles(
        source.source_token_roles as SourceTokenRoles,
        profiles,
      ),
      source_slug_tokens: createSourceSlugTokens(
        recordForProfiles(source.source_slug_tokens as SourceSlugTokens, profiles),
        profiles,
      ),
      source_fields: createSourceFields(
        recordForProfiles(
          source.source_fields as SourceFields,
          profiles,
        ),
        profiles,
      ),
      source_title_cleaning: createSourceTitleCleaning(
        recordForProfiles(
          source.source_title_cleaning as SourceTitleCleaning,
          profiles,
        ),
        profiles,
      ),
      source_cookie_policies: createSourceCookiePolicies(
        recordForProfiles(
          source.source_cookie_policies as SourceCookiePolicies,
          profiles,
        ),
        profiles,
      ),
      default_cookie_policy: createCookiePolicy(source.default_cookie_policy),
      default_fields: createFieldRoles(source.default_fields),
      default_naming: createNamingFlags(source.default_naming),
    };
  }

  function getSavedSettings(): SavedSettings {
    const profiles = settingsManagedSourceProfiles(
      mergeSourceProfiles(
        DEFAULT_SOURCE_PROFILES,
        defaults.source_profiles,
        settings.source_profiles,
      ),
    );
    const templateSettings = createTemplateSettings({
      ...defaults.template_settings,
      ...settings.template_settings,
    });
    return {
      source_profiles: profiles,
      source_locations: createSourceLocations(
        recordForProfiles(
          {
            ...defaults.source_locations,
            ...settings.source_locations,
          } as SourceLocations,
          profiles,
        ),
        profiles,
      ),
      template_settings: templateSettings,
      source_templates: createSourceTemplates(
        recordForProfiles(
          {
            ...defaults.source_templates,
            ...settings.source_templates,
          } as SourceTemplates,
          profiles,
        ),
        profiles,
        templateSettings,
      ),
      default_quality: createQualitySelection(
        {
          ...defaults.default_quality,
          ...settings.default_quality,
        },
        settings.quality_options,
      ),
      default_post_processing: createPostProcessingSelection({
        ...defaults.default_post_processing,
        ...settings.default_post_processing,
      }),
      source_scrape_rules: createSourceScrapeRules(
        recordForProfiles(
          { ...defaults.source_scrape_rules, ...settings.source_scrape_rules },
          profiles,
        ),
        profiles,
      ),
      source_token_roles: createSourceTokenRoles(
        {
          ...defaults.source_token_roles,
          ...settings.source_token_roles,
        } as SourceTokenRoles,
        profiles,
      ),
      source_slug_tokens: createSourceSlugTokens(
        recordForProfiles(
          {
            ...defaults.source_slug_tokens,
            ...settings.source_slug_tokens,
          } as SourceSlugTokens,
          profiles,
        ),
        profiles,
      ),
      source_fields: createSourceFields(
        recordForProfiles(
          {
            ...defaults.source_fields,
            ...settings.source_fields,
          },
          profiles,
        ),
        profiles,
      ),
      source_title_cleaning: createSourceTitleCleaning(
        recordForProfiles(
          {
            ...defaults.source_title_cleaning,
            ...settings.source_title_cleaning,
          },
          profiles,
        ),
        profiles,
      ),
      source_cookie_policies: createSourceCookiePolicies(
        recordForProfiles(
          {
            ...defaults.source_cookie_policies,
            ...settings.source_cookie_policies,
          },
          profiles,
        ),
        profiles,
      ),
      default_cookie_policy: createCookiePolicy({
        ...defaults.default_cookie_policy,
        ...settings.default_cookie_policy,
      }),
      default_fields: createFieldRoles({
        ...defaults.default_fields,
        ...settings.default_fields,
      }),
      default_naming: createNamingFlags({
        ...defaults.default_naming,
        ...settings.default_naming,
      }),
    };
  }

  function savedPayloadFromSnapshot(): SavedSettings {
    if (lastSavedSnapshot) {
      try {
        return JSON.parse(lastSavedSnapshot) as SavedSettings;
      } catch {}
    }
    return normalizeSavedPayload(getSavedSettings());
  }

  function snapshotFor(payload: SavedSettings): string {
    return JSON.stringify(stableValue(normalizeSavedPayload(payload)));
  }

  function draftSnapshot(): string {
    return JSON.stringify(
      stableValue({
        source_profiles: settingsDraft.source_profiles,
        source_locations: settingsDraft.source_locations,
        template_settings: settingsDraft.template_settings,
        source_templates: settingsDraft.source_templates,
        default_quality: settingsDraft.default_quality,
        default_post_processing: settingsDraft.default_post_processing,
        source_scrape_rules: settingsDraft.source_scrape_rules,
        source_token_roles: settingsDraft.source_token_roles,
        source_slug_tokens: settingsDraft.source_slug_tokens,
        source_fields: settingsDraft.source_fields,
        source_title_cleaning: settingsDraft.source_title_cleaning,
        source_cookie_policies: settingsDraft.source_cookie_policies,
        default_cookie_policy: settingsDraft.default_cookie_policy,
        default_fields: settingsDraft.default_fields,
        default_naming: settingsDraft.default_naming,
      }),
    );
  }

  function formatDraftSnapshot(): string {
    return JSON.stringify(stableValue(learnedFormatsDraft));
  }

  function savedLearnedFormatsFromSnapshot(): LearnedFormats {
    if (lastFormatDraftSnapshot) {
      try {
        return JSON.parse(lastFormatDraftSnapshot) as LearnedFormats;
      } catch {}
    }
    return cloneJson(settings.learned_formats || {});
  }

  function hasPendingCookieChanges(): boolean {
    return Boolean(
      Object.values(pendingCookieUploads).some((uploads) => uploads.length) ||
        Object.values(pendingCookieDeletes).some((ids) => ids.length),
    );
  }

  function clearCookieDraft(): void {
    for (const key of Object.keys(pendingCookieUploads)) {
      delete pendingCookieUploads[key];
    }
    for (const key of Object.keys(pendingCookieDeletes)) {
      delete pendingCookieDeletes[key];
    }
  }

  function copySavedFormatsToDraft(): void {
    replaceLearnedFormats(learnedFormatsDraft, settings.learned_formats || {});
  }

  function clearFormatDraftDirty(): void {
    lastFormatDraftSnapshot = formatDraftSnapshot();
  }

  function clearPendingFormatLearns(): void {
    for (const url of Object.keys(pendingFormatLearns)) {
      delete pendingFormatLearns[url];
    }
  }

  function clearSettingsDraftDirty(): void {
    lastDraftSnapshot = draftSnapshot();
  }

  function accountUsername(): string {
    return auth.username.value || settings.auth.username || "";
  }

  function resetAccountDraft(): void {
    settingsDraft.account.username = accountUsername();
    settingsDraft.account.current_password = "";
    settingsDraft.account.new_password = "";
    settingsDraft.account.confirm_password = "";
  }

  function accountDraftDirty(): boolean {
    return (
      settingsDraft.account.username !== accountUsername() ||
      settingsDraft.account.current_password.length > 0 ||
      settingsDraft.account.new_password.length > 0 ||
      settingsDraft.account.confirm_password.length > 0
    );
  }

  async function persistAccountDraft(): Promise<void> {
    if (!settingsDraft.account.current_password) {
      const message = "Enter your current password.";
      toast(message, "error");
      throw new Error(message);
    }
    if (settingsDraft.account.new_password !== settingsDraft.account.confirm_password) {
      const message = "New passwords do not match.";
      toast(message, "error");
      throw new Error(message);
    }
    try {
      await auth.updateCredentials({
        username: settingsDraft.account.username,
        current_password: settingsDraft.account.current_password,
        new_password: settingsDraft.account.new_password || "",
      });
      settings.auth.username = auth.username.value || settingsDraft.account.username;
      resetAccountDraft();
    } catch (error) {
      toast(errorMessage(error, "Could not save account."), "error");
      throw error;
    }
  }

  function applyServerSettings(data: UiConfigResponse): void {
    settings.auth = {
      username: String(data.auth?.username || ""),
      password_configured: Boolean(data.auth?.password_configured),
    };

    const profiles = mergeSourceProfiles(
      DEFAULT_SOURCE_PROFILES,
      data.source_profiles,
    );
    const managedProfiles = settingsManagedSourceProfiles(profiles);
    replaceProfiles(defaults.source_profiles, profiles);
    replaceProfiles(settings.source_profiles, profiles);

    const locations =
      data.source_locations || {};
    replaceRecord(
      defaults.source_locations,
      createSourceLocations(
        recordForProfiles(locations, managedProfiles),
        managedProfiles,
      ),
    );
    replaceRecord(
      settings.source_locations,
      createSourceLocations(
        recordForProfiles(locations, managedProfiles),
        managedProfiles,
      ),
    );

    const templateSettings = createTemplateSettings(
      data.template_settings || {},
    );
    Object.assign(defaults.template_settings, templateSettings);
    Object.assign(settings.template_settings, templateSettings);

    const templates = createSourceTemplates(
      recordForProfiles(data.source_templates || {}, managedProfiles),
      managedProfiles,
      templateSettings,
    );
    replaceRecord(defaults.source_templates, templates);
    replaceRecord(settings.source_templates, templates);

    const scrapeRules = createSourceScrapeRules(
      recordForProfiles(data.source_scrape_rules || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_scrape_rules, scrapeRules);
    replaceRecord(settings.source_scrape_rules, scrapeRules);

    const tokenRoles = createSourceTokenRoles(
      data.source_token_roles || {},
      managedProfiles,
    );
    replaceRecord(defaults.source_token_roles, tokenRoles);
    replaceRecord(settings.source_token_roles, tokenRoles);

    const slugTokens = createSourceSlugTokens(
      recordForProfiles(data.source_slug_tokens || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_slug_tokens, slugTokens);
    replaceRecord(settings.source_slug_tokens, slugTokens);

    settings.learned_formats = (data.learned_formats || {}) as LearnedFormats;

    const fields = createSourceFields(
      recordForProfiles(data.source_fields || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_fields, fields);
    replaceRecord(settings.source_fields, fields);

    const titleCleaning = createSourceTitleCleaning(
      recordForProfiles(data.source_title_cleaning || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_title_cleaning, titleCleaning);
    replaceRecord(settings.source_title_cleaning, titleCleaning);

    const cookiePolicies = createSourceCookiePolicies(
      recordForProfiles(data.source_cookie_policies || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_cookie_policies, cookiePolicies);
    replaceRecord(settings.source_cookie_policies, cookiePolicies);
    settings.cookie_policy_defaults = createCookiePolicyDefaults(
      data.cookie_policy_defaults || {},
    );

    const defaultCookiePolicy = createCookiePolicy(data.default_cookie_policy || {});
    replaceCookiePolicy(defaults.default_cookie_policy, defaultCookiePolicy);
    replaceCookiePolicy(settings.default_cookie_policy, defaultCookiePolicy);

    const defaultFields = createFieldRoles(data.default_fields || {});
    Object.assign(defaults.default_fields, defaultFields);
    Object.assign(settings.default_fields, defaultFields);

    const defaultNaming = createNamingFlags(data.default_naming || {});
    replaceRecord(defaults.default_naming, defaultNaming);
    replaceRecord(settings.default_naming, defaultNaming);

    settings.field_defaults = createFieldRoles(
      data.field_defaults || {},
    );
    replaceRecord(
      settings.source_field_defaults,
      createSourceFields(
        recordForProfiles(data.source_field_defaults || {}, managedProfiles),
        managedProfiles,
      ),
    );
    settings.title_cleaning_rules = Array.isArray(data.title_cleaning_rules)
      ? data.title_cleaning_rules
      : [];
    settings.naming_choices = Array.isArray(data.naming_choices)
      ? data.naming_choices
      : [];

    const qualityOptions = createQualityOptions(data.quality_options || {});
    settings.quality_options = qualityOptions;

    const defaultQuality = createQualitySelection(
      data.default_quality || {},
      qualityOptions,
    );
    Object.assign(defaults.default_quality, defaultQuality);
    Object.assign(settings.default_quality, defaultQuality);
    const defaultPostProcessing = createPostProcessingSelection(
      data.default_post_processing || {},
    );
    Object.assign(defaults.default_post_processing, defaultPostProcessing);
    Object.assign(settings.default_post_processing, defaultPostProcessing);

    settings.template_tokens = Array.isArray(data.template_tokens)
      ? data.template_tokens
      : [];
    settings.media_root = String(data.media_root || "");
    replaceRecord(
      settings.source_location_options,
      createSourceLocationOptions(
        data.source_location_options || {},
        managedProfiles,
      ),
    );
    replaceRecord(
      settings.ytdlp_cookies,
      createCookiesMap(
        recordForProfiles(data.ytdlp_cookies || {}, managedProfiles),
        managedProfiles,
      ),
    );
  }

  function mergeCleanRecordEntries<T>(
    target: Record<string, T>,
    previous: Record<string, T> = {},
    server: Record<string, T> = {},
  ): void {
    const current = target;
    const next = { ...current };
    const keys = new Set([
      ...Object.keys(previous),
      ...Object.keys(server),
      ...Object.keys(current),
    ]);
    for (const key of keys) {
      if (!sameJson(current[key], previous[key])) continue;
      if (key in server) next[key] = cloneJson(server[key]);
      else delete next[key];
    }
    replaceRecord(target, next);
  }

  function mergeCleanObjectProps<T extends Record<string, unknown>>(
    target: T,
    previous: Partial<T> = {},
    server: Partial<T> = {},
  ): void {
    const keys = new Set([
      ...Object.keys(previous),
      ...Object.keys(server),
      ...Object.keys(target),
    ] as Array<keyof T & string>);
    for (const key of keys) {
      if (!sameJson(target[key], previous[key])) continue;
      if (key in server) target[key] = cloneJson(server[key]) as T[typeof key];
      else delete target[key];
    }
  }

  function mergeCleanProfilesFromServer(
    previous: SourceProfile[],
    server: SourceProfile[],
  ): void {
    const previousByKey = new Map(
      previous.map((profile) => [normalizeSourceKey(profile.key), createSourceProfile(profile)]),
    );
    const next = mergeSourceProfiles(server);
    for (const profile of settingsDraft.source_profiles) {
      const key = normalizeSourceKey(profile.key);
      if (!key) continue;
      const previousProfile = previousByKey.get(key);
      if (!previousProfile || !sameJson(createSourceProfile(profile), previousProfile)) {
        const index = next.findIndex((item) => normalizeSourceKey(item.key) === key);
        if (index >= 0) next[index] = createSourceProfile(profile);
        else next.push(createSourceProfile(profile));
      }
    }
    replaceProfiles(settingsDraft.source_profiles, next);
  }

  function mergeCleanDraftFromServer(
    previous: SavedSettings,
    server: SavedSettings,
  ): void {
    mergeCleanProfilesFromServer(previous.source_profiles, server.source_profiles);
    mergeCleanRecordEntries(
      settingsDraft.source_locations,
      previous.source_locations,
      server.source_locations,
    );
    mergeCleanObjectProps(
      settingsDraft.template_settings,
      previous.template_settings,
      server.template_settings,
    );
    mergeCleanRecordEntries(
      settingsDraft.source_templates,
      previous.source_templates,
      server.source_templates,
    );
    mergeCleanObjectProps(
      settingsDraft.default_quality,
      previous.default_quality,
      server.default_quality,
    );
    mergeCleanObjectProps(
      settingsDraft.default_post_processing,
      previous.default_post_processing,
      server.default_post_processing,
    );
    mergeCleanRecordEntries(
      settingsDraft.source_scrape_rules,
      previous.source_scrape_rules,
      server.source_scrape_rules,
    );
    mergeCleanRecordEntries(
      settingsDraft.source_token_roles,
      previous.source_token_roles,
      server.source_token_roles,
    );
    mergeCleanRecordEntries(
      settingsDraft.source_slug_tokens,
      previous.source_slug_tokens,
      server.source_slug_tokens,
    );
    mergeCleanRecordEntries(
      settingsDraft.source_fields,
      previous.source_fields,
      server.source_fields,
    );
    mergeCleanRecordEntries(
      settingsDraft.source_title_cleaning,
      previous.source_title_cleaning,
      server.source_title_cleaning,
    );
    mergeCleanRecordEntries(
      settingsDraft.source_cookie_policies,
      previous.source_cookie_policies,
      server.source_cookie_policies,
    );
    mergeCleanObjectProps(
      settingsDraft.default_cookie_policy,
      previous.default_cookie_policy,
      server.default_cookie_policy,
    );
    for (const role of FIELD_ROLE_KEYS) {
      if (!sameJson(settingsDraft.default_fields[role], previous.default_fields[role])) continue;
      settingsDraft.default_fields[role] = cloneJson(server.default_fields[role] || []);
    }
    mergeCleanObjectProps(
      settingsDraft.default_naming,
      previous.default_naming,
      server.default_naming,
    );
  }

  function mergeCleanFormatsFromServer(
    previous: LearnedFormats,
    server: LearnedFormats,
  ): void {
    const current = learnedFormatsDraft;
    const next = cloneJson(current);
    const keys = new Set([
      ...Object.keys(previous),
      ...Object.keys(server),
      ...Object.keys(current),
    ]);
    for (const key of keys) {
      if (!sameJson(current[key], previous[key])) continue;
      if (key in server) next[key] = cloneJson(server[key]);
      else delete next[key];
    }
    replaceLearnedFormats(learnedFormatsDraft, next);
  }

  function ensureDraftSourceProfile(profile: SourceProfile): void {
    const key = normalizeSourceKey(profile.key);
    if (!key) return;
    const existing = settingsDraft.source_profiles.find(
      (item) => normalizeSourceKey(item.key) === key,
    );
    if (!existing) {
      settingsDraft.source_profiles.push(profile);
      return;
    }
    for (const host of profile.hosts || []) {
      if (host && !existing.hosts.includes(host)) existing.hosts.push(host);
    }
    if (!existing.icon_url && profile.icon_url) existing.icon_url = profile.icon_url;
  }

  function syncPendingLearnsForKey(sourceKey: string, templates: string[]): void {
    const key = normalizeSourceKey(sourceKey);
    const remaining = new Set(templates);
    for (const [url, item] of Object.entries(pendingFormatLearns)) {
      if (item.sourceKey === key && !remaining.has(item.template)) {
        delete pendingFormatLearns[url];
      }
    }
  }

  function describeEditableTemplates(
    sourceKey: string,
    templates: string[],
  ): LearnedFormats[string] {
    const pending = pendingTemplatesForKey(pendingFormatLearns, sourceKey);
    const learnedTemplates = templates.filter((template) => !pending.has(template));
    return {
      templates,
      segments: describeDraftTemplates(learnedTemplates).segments,
    };
  }

  function removeDraftOnlySourceIfEmpty(sourceKey: string): void {
    const key = normalizeSourceKey(sourceKey);
    if (!key || learnedFormatsDraft[key]?.templates?.length) return;
    if (Object.values(pendingFormatLearns).some((item) => item.sourceKey === key)) return;
    if (pendingCookieUploads[key]?.length) return;
    const saved = mergeSourceProfiles(DEFAULT_SOURCE_PROFILES, settings.source_profiles).some(
      (profile) => normalizeSourceKey(profile.key) === key,
    );
    if (saved) return;
    const index = settingsDraft.source_profiles.findIndex(
      (profile) => normalizeSourceKey(profile.key) === key,
    );
    if (index >= 0) settingsDraft.source_profiles.splice(index, 1);
    delete learnedFormatsDraft[key];
    delete settingsDraft.source_locations[key];
    delete settingsDraft.source_templates[key];
    delete settingsDraft.source_scrape_rules[key];
    delete settingsDraft.source_token_roles[key];
    delete settingsDraft.source_slug_tokens[key];
    delete settingsDraft.source_fields[key];
    delete settingsDraft.source_title_cleaning[key];
    delete settingsDraft.source_cookie_policies[key];
  }

  function replaceDesiredTemplate(
    desired: LearnedFormats,
    sourceKey: string,
    fromTemplate: string,
    actualTemplates: string[],
  ): void {
    const key = normalizeSourceKey(sourceKey);
    if (!key || !actualTemplates.length) return;
    const fallback = learnedFormatsDraft[key] || settings.learned_formats?.[key] || { templates: [], segments: [] };
    const current = desired[key] || fallback;
    const next: string[] = [];
    let replaced = false;
    for (const template of current.templates || []) {
      if (template === fromTemplate) {
        for (const actual of actualTemplates) {
          if (actual && !next.includes(actual)) next.push(actual);
        }
        replaced = true;
      } else if (template && !next.includes(template)) {
        next.push(template);
      }
    }
    if (!replaced) {
      for (const actual of actualTemplates) {
        if (actual && !next.includes(actual)) next.push(actual);
      }
    }
    desired[key] = {
      ...cloneJson(current),
      ...describeDraftTemplates(next),
    };
  }

  function removeDesiredTemplate(
    desired: LearnedFormats,
    sourceKey: string,
    template: string,
  ): void {
    const key = normalizeSourceKey(sourceKey);
    const current = desired[key];
    if (!key || !current) return;
    const next = (current.templates || []).filter((item) => item !== template);
    if (!next.length && !settings.learned_formats?.[key]) {
      delete desired[key];
      return;
    }
    desired[key] = {
      ...cloneJson(current),
      ...describeDraftTemplates(next),
    };
  }

  function syncSavedServerConfig(
    data: UiConfigResponse,
    options: { updateCache?: boolean; cleanSettings?: boolean; cleanFormats?: boolean } = {},
  ): void {
    const previous = savedPayloadFromSnapshot();
    const previousFormats = savedLearnedFormatsFromSnapshot();
    const draftWasDirty = hasSettingsUnsavedChanges.value;
    const formatWasDirty = hasFormatUnsavedChanges.value;
    applyServerSettings(data);
    if (options.updateCache !== false) {
      queryClient.setQueryData(UI_CONFIG_QUERY_KEY, data);
    }
    if (options.cleanSettings || !draftWasDirty) {
      copySavedSettingsToDraft();
      lastSavedSnapshot = snapshotFor(settingsDraft);
      clearSettingsDraftDirty();
    } else {
      const server = normalizeSavedPayload(getSavedSettings());
      mergeCleanDraftFromServer(previous, server);
      lastSavedSnapshot = snapshotFor(server);
      lastDraftSnapshot = snapshotFor(server);
    }

    const serverFormats = cloneJson(settings.learned_formats || {});
    if (options.cleanFormats || !formatWasDirty) {
      copySavedFormatsToDraft();
    } else {
      mergeCleanFormatsFromServer(previousFormats, serverFormats);
    }
    lastFormatDraftSnapshot = JSON.stringify(stableValue(serverFormats));
  }

  function fieldRolesHaveValues(fields: FieldRoles): boolean {
    return fields.username.length > 0 || fields.nickname.length > 0 || fields.title.length > 0;
  }

  function syncSavedFields(
    sourceKey: string,
    fields: Partial<FieldRoles>,
  ): void {
    const key = normalizeSourceKey(sourceKey);
    if (!key) return;

    const normalized = createFieldRoles(fields);
    if (!fieldRolesHaveValues(normalized)) return;

    const previous = savedPayloadFromSnapshot();
    const draftWasDirty = hasSettingsUnsavedChanges.value;
    const previousFields = createFieldRoles(
      previous.source_fields[key] || {},
    );
    const draftFields = createFieldRoles(
      settingsDraft.source_fields[key] || {},
    );
    const draftWasClean = sameJson(draftFields, previousFields);
    const savedFields = cloneJson(normalized);

    settings.source_fields[key] = cloneJson(savedFields);
    defaults.source_fields[key] = cloneJson(savedFields);
    if (draftWasClean) settingsDraft.source_fields[key] = cloneJson(savedFields);

    previous.source_fields[key] = cloneJson(savedFields);
    lastSavedSnapshot = snapshotFor(previous);
    if (!draftWasDirty) clearSettingsDraftDirty();
  }

  async function persistSettings(
    payload: SavedSettings,
    successMessage = "",
  ): Promise<UiConfigResponse> {
    const data = await saveSettingsMutation.mutateAsync(payload);
    syncSavedServerConfig(data, { cleanSettings: true });
    if (successMessage) toast(successMessage);
    return data;
  }

  function copySavedSettingsToDraft(): void {
    const current = getSavedSettings();
    const normalized = normalizeSavedPayload(current);
    replaceProfiles(settingsDraft.source_profiles, normalized.source_profiles);
    replaceRecord(settingsDraft.source_locations, normalized.source_locations);
    Object.assign(
      settingsDraft.template_settings,
      normalized.template_settings,
    );
    replaceRecord(settingsDraft.source_templates, normalized.source_templates);
    Object.assign(settingsDraft.default_quality, normalized.default_quality);
    Object.assign(
      settingsDraft.default_post_processing,
      normalized.default_post_processing,
    );
    replaceRecord(
      settingsDraft.source_scrape_rules,
      normalized.source_scrape_rules,
    );
    replaceRecord(
      settingsDraft.source_token_roles,
      normalized.source_token_roles,
    );
    replaceRecord(
      settingsDraft.source_slug_tokens,
      normalized.source_slug_tokens,
    );
    replaceRecord(
      settingsDraft.source_fields,
      normalized.source_fields,
    );
    replaceRecord(
      settingsDraft.source_title_cleaning,
      normalized.source_title_cleaning,
    );
    replaceRecord(
      settingsDraft.source_cookie_policies,
      normalized.source_cookie_policies,
    );
    replaceCookiePolicy(
      settingsDraft.default_cookie_policy,
      normalized.default_cookie_policy,
    );
    Object.assign(settingsDraft.default_fields, normalized.default_fields);
    replaceRecord(settingsDraft.default_naming, normalized.default_naming);
  }

  function copySettingsToDraft(): void {
    copySavedSettingsToDraft();
    copySavedFormatsToDraft();
    clearPendingFormatLearns();
    clearCookieDraft();
    resetAccountDraft();
    lastSavedSnapshot = snapshotFor(settingsDraft);
    clearSettingsDraftDirty();
    clearFormatDraftDirty();
  }

  function setSettingsSection(
    section: SettingsSection,
    shouldFocus = false,
  ): void {
    settingsSection.value = section;
    if (!shouldFocus) return;
    const firstSource =
      settingsManagedSourceProfiles(sourceProfiles.value).find((profile) => profile.key)?.key || "settings";
    const focusTargets: Record<SettingsSection, string> = {
      account: "accountUsernameInput",
      defaults: "",
      locations: "",
      cookies: "",
      format: "formatLearnInput",
      fields: `${firstSource}FieldsProbeInput`,
      scraper: `${firstSource}ScraperProbeInput`,
      slug: `${firstSource}SlugSection`,
      templates: "",
      naming: "",
    };
    void nextTick(() =>
      document.getElementById(focusTargets[settingsSection.value])?.focus(),
    );
  }

  function openSettings(
    event?: Event,
    section: SettingsSection = "locations",
  ): void {
    lastFocusedTrigger.value =
      event?.currentTarget instanceof HTMLElement
        ? event.currentTarget
        : document.activeElement instanceof HTMLElement
          ? document.activeElement
          : null;
    copySettingsToDraft();
    settingsOpen.value = true;
    setSettingsSection(section, true);
  }

  function closeSettings(): void {
    settingsOpen.value = false;
  }

  const hasSettingsUnsavedChanges = computed(() => {
    if (!draftSeeded.value) return false;
    return draftSnapshot() !== lastDraftSnapshot;
  });

  const hasAccountUnsavedChanges = computed(() => {
    return draftSeeded.value && accountDraftDirty();
  });

  const hasFormatUnsavedChanges = computed(() => {
    return (
      draftSeeded.value &&
      formatDraftSnapshot() !== lastFormatDraftSnapshot
    );
  });

  const hasCookieUnsavedChanges = computed(() => {
    return draftSeeded.value && hasPendingCookieChanges();
  });

  const hasUnsavedChanges = computed(() => {
    return (
      hasSettingsUnsavedChanges.value ||
      hasAccountUnsavedChanges.value ||
      hasFormatUnsavedChanges.value ||
      hasCookieUnsavedChanges.value
    );
  });

  function changedFormatKeys(): string[] {
    const keys = new Set([
      ...Object.keys(settings.learned_formats || {}),
      ...Object.keys(learnedFormatsDraft),
    ]);
    return [...keys].filter((key) => {
      const savedTemplates = settings.learned_formats?.[key]?.templates || [];
      const draftTemplates = learnedFormatsDraft[key]?.templates || [];
      return !sameJson(draftTemplates, savedTemplates);
    });
  }

  async function persistFormatDraft(): Promise<void> {
    let latest: UiConfigResponse | null = null;
    let learnedLatest: UiConfigResponse | null = null;
    const desired = cloneJson(learnedFormatsDraft);

    for (const pending of Object.values(pendingFormatLearns)) {
      const previousFormats = cloneJson(
        (learnedLatest?.learned_formats || settings.learned_formats || {}) as LearnedFormats,
      );
      learnedLatest = await learnFormatMutation.mutateAsync(pending.url);
      const key = normalizeSourceKey(learnedLatest.learn_result?.source_key || pending.sourceKey);
      const serverEntry = learnedLatest.learned_formats?.[key];
      const previousTemplates = previousFormats[key]?.templates || [];
      const serverTemplates = serverEntry?.templates || [];
      const matchedTemplate = String(
        learnedLatest.learn_result?.format_template || "",
      ).trim();
      const addedTemplates = serverTemplates.filter(
        (template) => !previousTemplates.includes(template),
      );
      const actualTemplates =
        matchedTemplate
          ? [matchedTemplate]
          : addedTemplates.length > 0
            ? addedTemplates
            : serverTemplates.includes(pending.template)
              ? [pending.template]
              : [];
      if (!actualTemplates.length) {
        throw new Error("Could not identify a format from that link.");
      }
      if (key && actualTemplates.length) {
        if (key !== pending.sourceKey) {
          removeDesiredTemplate(desired, pending.sourceKey, pending.template);
          replaceDesiredTemplate(desired, key, pending.template, actualTemplates);
        } else {
          replaceDesiredTemplate(desired, pending.sourceKey, pending.template, actualTemplates);
        }
      }
    }

    if (learnedLatest) {
      replaceLearnedFormats(learnedFormatsDraft, desired);
      clearPendingFormatLearns();
      syncSavedServerConfig(learnedLatest);
    }

    for (const key of changedFormatKeys()) {
      latest = await formatTemplatesMutation.mutateAsync({
        sourceKey: key,
        templates: learnedFormatsDraft[key]?.templates || [],
      });
    }
    if (latest) {
      syncSavedServerConfig(latest, { cleanFormats: true });
    } else if (!learnedLatest) {
      copySavedFormatsToDraft();
      clearFormatDraftDirty();
    }
  }

  async function persistCookieDraft(): Promise<void> {
    let latest: UiConfigResponse | null = null;
    for (const [key, ids] of Object.entries(pendingCookieDeletes)) {
      for (const cookieId of ids) {
        latest = await deleteCookiesMutation.mutateAsync({ platform: key, cookieId });
      }
    }
    for (const [key, uploads] of Object.entries(pendingCookieUploads)) {
      for (const upload of uploads) {
        latest = await uploadCookiesMutation.mutateAsync({
          platform: key,
          file: upload.file,
        });
      }
    }
    clearCookieDraft();
    if (latest) {
      syncSavedServerConfig(latest);
    }
  }

  async function saveSettingsDraft(): Promise<void> {
    const shouldSaveAccount = hasAccountUnsavedChanges.value;
    const shouldSaveSettings = hasSettingsUnsavedChanges.value;
    const shouldSaveFormats = hasFormatUnsavedChanges.value;
    const shouldSaveCookies = hasCookieUnsavedChanges.value;
    if (
      !shouldSaveAccount &&
      !shouldSaveSettings &&
      !shouldSaveFormats &&
      !shouldSaveCookies
    ) {
      return;
    }

    if (shouldSaveAccount) {
      await persistAccountDraft();
    }

    if (shouldSaveFormats) {
      try {
        await persistFormatDraft();
      } catch (error) {
        toast(errorMessage(error, "Could not update formats."), "error");
        throw error;
      }
    }

    if (shouldSaveSettings) {
      const payload = normalizeSavedPayload(settingsDraft);
      try {
        await persistSettings(payload);
        copySavedSettingsToDraft();
        lastSavedSnapshot = snapshotFor(settingsDraft);
        clearSettingsDraftDirty();
      } catch (error) {
        toast(errorMessage(error, "Could not save settings."), "error");
        throw error;
      }
    }

    if (shouldSaveCookies) {
      try {
        await persistCookieDraft();
      } catch (error) {
        toast(errorMessage(error, "Could not update cookies."), "error");
        throw error;
      }
    }

    toast("Settings saved.");
  }

  function queueCookieUpload(key: string, file: File): void {
    pendingCookieSeq += 1;
    const uploads = pendingCookieUploads[key] || [];
    uploads.push({ id: `${PENDING_COOKIE_PREFIX}${pendingCookieSeq}`, file });
    pendingCookieUploads[key] = uploads;
  }

  async function connectCookies(platform: string, file?: File): Promise<void> {
    if (!file) {
      toast("Choose a cookies file first.", "error");
      return;
    }
    const key = normalizeSourceKey(platform);
    if (!key) {
      toast("Choose a source first.", "error");
      return;
    }
    queueCookieUpload(key, file);
  }

  async function learnFormat(url: string): Promise<string> {
    const value = url.trim();
    if (!value) {
      toast("Paste a link first.", "error");
      return "";
    }
    const profiles = mergeSourceProfiles(
      DEFAULT_SOURCE_PROFILES,
      settings.source_profiles,
      settingsDraft.source_profiles,
    );
    const profile = sourceProfileFromUrlDraft(value, profiles);
    const template = value;
    if (!profile || !canonicalDraftUrl(value)) {
      toast("Paste a valid link first.", "error");
      return "";
    }

    ensureDraftSourceProfile(profile);
    const key = profile.key;
    const entry = learnedFormatsDraft[key] || settings.learned_formats?.[key] || { templates: [], segments: [] };
    const templates = [...(entry.templates || [])];
    if (templates.includes(template)) {
      return key;
    }
    templates.push(template);
    pendingFormatLearns[value] = { url: value, sourceKey: key, template };
    learnedFormatsDraft[key] = {
      ...cloneJson(entry),
      ...describeEditableTemplates(key, templates),
    };
    return key;
  }

  async function reorderFormatTemplates(
    sourceKey: string,
    templates: string[],
  ): Promise<void> {
    const key = normalizeSourceKey(sourceKey);
    const entry = learnedFormatsDraft[key] || settings.learned_formats?.[key];
    if (!key || !entry) return;
    const existing = new Set((entry.templates || []).map(String));
    const nextTemplates = templates.filter(
      (template, index, list) =>
        existing.has(template) && list.indexOf(template) === index,
    );
    learnedFormatsDraft[key] = {
      ...cloneJson(entry),
      ...describeEditableTemplates(key, nextTemplates),
    };
    syncPendingLearnsForKey(key, nextTemplates);
    removeDraftOnlySourceIfEmpty(key);
  }

  async function probeFields(
    url: string,
    sourceKey = "",
  ): Promise<ProbeFieldsResponse> {
    const response = await probeFieldsMutation.mutateAsync({
      url: url.trim(),
      sourceKey: normalizeSourceKey(sourceKey),
    });
    if (response.saved && response.field_roles) {
      syncSavedFields(
        response.source_key || sourceKey,
        response.field_roles,
      );
      toast("Fields saved.");
    }
    return response;
  }

  async function removeCookies(platform: string, cookieId: string): Promise<void> {
    const key = normalizeSourceKey(platform);
    if (!key || !cookieId) return;
    if (cookieId.startsWith(PENDING_COOKIE_PREFIX)) {
      pendingCookieUploads[key] = (pendingCookieUploads[key] || []).filter(
        (upload) => upload.id !== cookieId,
      );
      if (!pendingCookieUploads[key].length) delete pendingCookieUploads[key];
      removeDraftOnlySourceIfEmpty(key);
      return;
    }
    const deletes = pendingCookieDeletes[key] || [];
    if (deletes.includes(cookieId)) return;
    deletes.push(cookieId);
    pendingCookieDeletes[key] = deletes;
  }

  async function reorderCookies(platform: string, cookieIds: string[]): Promise<void> {
    const key = normalizeSourceKey(platform);
    if (!key) return;
    const saved = settings.ytdlp_cookies?.[key]?.cookies || [];
    const known = new Set(saved.map((cookie) => cookie.id));
    const ordered = cookieIds.filter(
      (cookieId, index) => known.has(cookieId) && cookieIds.indexOf(cookieId) === index,
    );
    if (!ordered.length) return;
    try {
      // Order is the rotation's tie-breaker, so it is saved as soon as it is dragged.
      syncSavedServerConfig(
        await reorderCookiesMutation.mutateAsync({ platform: key, cookieIds: ordered }),
      );
    } catch (error) {
      toast(errorMessage(error, "Could not reorder cookies."), "error");
    }
  }

  const draftSeeded = ref(false);
  watch(
    () => uiConfigQuery.data.value,
    (data) => {
      if (!data) return;
      if (!draftSeeded.value) {
        applyServerSettings(data);
        copySettingsToDraft();
        draftSeeded.value = true;
      } else {
        syncSavedServerConfig(data, { updateCache: false });
      }
    },
    { immediate: true },
  );
  watch(settingsOpen, (open) => {
    document.body.classList.toggle("dialog-open", open);
    if (open) {
      // Downloads learn fields server-side; refetch so the list is current on open.
      queryClient.invalidateQueries({ queryKey: UI_CONFIG_QUERY_KEY });
    } else {
      lastFocusedTrigger.value?.focus();
    }
  });

  return {
    closeSettings,
    connectCookies,
    cookieStatuses,
    reorderCookies,
    learnFormat,
    probeFields,
    reorderFormatTemplates,
    getSavedSettings,
    openSettings,
    removeCookies,
    savedSettings,
    setSettingsSection,
    settings,
    settingsDraft,
    learnedFormatsDraft,
    settingsOpen,
    settingsSection,
    sourceProfiles,
    hasUnsavedChanges,
    saveSettingsDraft,
    copySettingsToDraft,
  };
}
