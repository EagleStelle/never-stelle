const init = window.__APP_INIT__ || {};

const currentTabId = (() => {
  const key = "neverstelle.tabId";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value =
      window.crypto && typeof window.crypto.randomUUID === "function"
        ? window.crypto.randomUUID()
        : `tab-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    sessionStorage.setItem(key, value);
  }
  return value;
})();

export const state = {
  defaults: {
    site_locations: init.site_default_locations || {},
    save_mode: init.saved_save_mode || "nas",
    template_settings: { filename_template: "", folder_template: "" },
  },
  settings: {
    site_locations: { ...(init.site_default_locations || {}) },
    save_mode: init.saved_save_mode || "nas",
    template_settings: { filename_template: "", folder_template: "" },
    download_locations: [],
    instagram_auth: {
      configured: false,
      identifier_type: "username",
      identifier: "",
      session_username: "",
      username: "",
      session_saved: false,
      last_login_at: "",
      last_error: "",
    },
    instagram_ytdlp_cookies: { configured: false, source: "none", filename: "", uploaded_at: "" },
  },
  tabId: currentTabId,
  pollHandle: null,
  pollIntervalMs: 0,
  activeFilter: localStorage.getItem("neverstelle.activeFilter") || "all",
  viewMode: localStorage.getItem("neverstelle.viewMode") || "grid",
  activeMenu: localStorage.getItem("neverstelle.activeMenu") || "all",
  deliveredDeviceDownloads: (() => {
    try {
      return JSON.parse(sessionStorage.getItem("neverstelle.deviceDelivered") || "{}") || {};
    } catch {
      return {};
    }
  })(),
  visibleTasks: [],
  taskCache: new Map(),
  lastFocusedTrigger: null,
  settingsSection: "downloads",
  searchQuery: "",
  notifyPermission: typeof Notification !== "undefined" ? Notification.permission : "default",
};

export let _lastSettingsSignature = "";
export function setLastSettingsSignature(sig) {
  _lastSettingsSignature = sig;
}
