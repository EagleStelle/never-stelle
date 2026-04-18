import { state, _lastSettingsSignature, setLastSettingsSignature } from "./state.js";
import { toast, formatTimestamp } from "./utils.js";
import { renderLocationOptions, updateSaveModeButtons, setSettingsSection } from "./render.js";

export function getSavedSettings() {
  return {
    site_locations: {
      youtube: state.settings.site_locations.youtube || state.defaults.site_locations.youtube || "",
      facebook: state.settings.site_locations.facebook || state.defaults.site_locations.facebook || "",
      instagram: state.settings.site_locations.instagram || state.defaults.site_locations.instagram || "",
      tiktok: state.settings.site_locations.tiktok || state.defaults.site_locations.tiktok || "",
      iwara: state.settings.site_locations.iwara || state.defaults.site_locations.iwara || "",
      others: state.settings.site_locations.others || state.defaults.site_locations.others || "",
    },
    save_mode: state.settings.save_mode === "device" ? "device" : (state.defaults.save_mode || "nas"),
    template_settings: {
      filename_template:
        state.settings.template_settings.filename_template ||
        state.defaults.template_settings.filename_template ||
        "",
      folder_template:
        state.settings.template_settings.folder_template ||
        state.defaults.template_settings.folder_template ||
        "",
    },
  };
}

export function persistDeliveredDeviceDownloads() {
  sessionStorage.setItem(
    "neverstelle.deviceDelivered",
    JSON.stringify(state.deliveredDeviceDownloads || {})
  );
}

export function syncInstagramAuthInputs() {
  const info = state.settings.instagram_auth || {};
  const typeInput = document.getElementById("instagramIdentifierTypeInput");
  const identifierInput = document.getElementById("instagramIdentifierInput");
  if (typeInput) typeInput.value = info.identifier_type || "username";
  if (identifierInput) identifierInput.value = info.identifier || "";
  updateInstagramIdentifierField();
}

export function updateInstagramIdentifierField() {
  const typeInput = document.getElementById("instagramIdentifierTypeInput");
  const label = document.getElementById("instagramIdentifierLabel");
  const identifierInput = document.getElementById("instagramIdentifierInput");
  const type = typeInput ? typeInput.value : "username";
  const presets = {
    username: { label: "Username", placeholder: "Enter your Instagram username", autocomplete: "username" },
    email: { label: "Email", placeholder: "Enter your Instagram email", autocomplete: "email" },
    phone: { label: "Mobile number", placeholder: "Enter your Instagram mobile number", autocomplete: "tel" },
  };
  const preset = presets[type] || presets.username;
  if (label) label.textContent = preset.label;
  if (identifierInput) {
    identifierInput.placeholder = preset.placeholder;
    identifierInput.autocomplete = preset.autocomplete;
  }
}

export function renderInstagramAuthStatus() {
  const info = state.settings.instagram_auth || {};
  const status = document.getElementById("instagramAuthStatus");
  const removeButton = document.getElementById("removeInstagramAuthButton");
  if (!status || !removeButton) return;
  removeButton.classList.remove("opacity-50", "cursor-not-allowed");

  if (info.session_saved) {
    const parts = [];
    if (info.session_username) parts.push(`Connected as @${info.session_username}`);
    else if (info.identifier) parts.push(`Connected with ${info.identifier}`);
    else parts.push("Connected to Instagram");
    const when = formatTimestamp(info.last_login_at);
    if (when) parts.push(when);
    if (info.last_error) parts.push(`Last error: ${info.last_error}`);
    status.textContent = parts.join(" · ");
    removeButton.disabled = false;
    return;
  }
  if (info.identifier) {
    const parts = [`Saved ${info.identifier_type || "username"}: ${info.identifier}`];
    if (info.last_error) parts.push(`Last error: ${info.last_error}`);
    status.textContent = parts.join(" · ");
    removeButton.disabled = false;
    return;
  }
  if (info.last_error) {
    status.textContent = info.last_error;
    removeButton.disabled = false;
    return;
  }
  status.textContent = "No Instagram login saved.";
  removeButton.disabled = true;
  removeButton.classList.add("opacity-50", "cursor-not-allowed");
}

export function renderInstagramYtdlpCookiesStatus() {
  const info = state.settings.instagram_ytdlp_cookies || {};
  const status = document.getElementById("instagramYtdlpCookiesStatus");
  const removeButton = document.getElementById("removeInstagramYtdlpCookiesButton");
  if (!status || !removeButton) return;
  removeButton.classList.remove("opacity-50", "cursor-not-allowed");
  if (info.configured) {
    const parts = info.filename ? [`Connected with ${info.filename}`] : ["Connected to yt-dlp cookies"];
    const when = formatTimestamp(info.uploaded_at);
    if (when) parts.push(when);
    status.textContent = parts.join(" · ");
    removeButton.disabled = false;
    return;
  }
  status.textContent = "No yt-dlp cookies saved.";
  removeButton.disabled = true;
  removeButton.classList.add("opacity-50", "cursor-not-allowed");
}

export function applyServerSettings(data) {
  if (data.site_default_locations) {
    state.defaults.site_locations = { ...state.defaults.site_locations, ...data.site_default_locations };
    state.settings.site_locations = { ...state.settings.site_locations, ...data.site_default_locations };
  }
  state.settings.save_mode = data.save_mode === "device" ? "device" : "nas";
  state.defaults.save_mode = state.settings.save_mode;
  if (data.template_settings) {
    state.defaults.template_settings = { ...state.defaults.template_settings, ...data.template_settings };
    state.settings.template_settings = { ...state.settings.template_settings, ...data.template_settings };
  }
  state.settings.download_locations = Array.isArray(data.download_locations) ? data.download_locations : [];
  state.settings.instagram_auth = data.instagram_auth || {
    configured: false, identifier_type: "username", identifier: "", session_username: "",
    username: "", session_saved: false, last_login_at: "", last_error: "",
  };
  state.settings.instagram_ytdlp_cookies = data.instagram_ytdlp_cookies || {
    configured: false, source: "none", filename: "", uploaded_at: "",
  };
  syncInstagramAuthInputs();
  renderInstagramAuthStatus();
  renderInstagramYtdlpCookiesStatus();
}

export async function persistSettings(settings, successMessage = "") {
  const response = await fetch("/api/settings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(settings),
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Could not save settings.");
  applyServerSettings(data);
  if (successMessage) toast(successMessage);
  return data;
}

export async function fetchUIConfig() {
  try {
    const response = await fetch("/api/ui-config");
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Could not load UI config.");
    const sig = data.settings_signature || "";
    if (sig && sig === _lastSettingsSignature) return;
    setLastSettingsSignature(sig);
    applyServerSettings(data);
    renderLocationOptions(state.settings.download_locations || []);
    updateSaveModeButtons();
  } catch (error) {
    console.error(error);
  }
}

export async function configureInstagramAuth(identifierType, identifier, password) {
  const response = await fetch("/api/settings/instagram-auth", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ identifier_type: identifierType, identifier, password }),
  });
  const data = await response.json();
  if (data && typeof data === "object" && (data.instagram_auth || data.download_locations || data.site_default_locations || data.save_mode)) {
    applyServerSettings(data);
  }
  if (!response.ok) throw new Error(data.error || "Could not connect Instagram login.");
  return data;
}

export async function removeInstagramAuth() {
  const response = await fetch("/api/settings/instagram-auth", { method: "DELETE" });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Could not remove Instagram login.");
  applyServerSettings(data);
  const identifierInput = document.getElementById("instagramIdentifierInput");
  const passwordInput = document.getElementById("instagramPasswordInput");
  if (identifierInput) identifierInput.value = "";
  if (passwordInput) passwordInput.value = "";
  syncInstagramAuthInputs();
  return data;
}

export async function configureInstagramYtdlpCookies(file) {
  const formData = new FormData();
  formData.append("file", file);
  const response = await fetch("/api/settings/instagram-ytdlp-cookies", { method: "POST", body: formData });
  const data = await response.json();
  if (data && typeof data === "object" && (data.instagram_ytdlp_cookies || data.instagram_auth || data.download_locations || data.site_default_locations || data.save_mode)) {
    applyServerSettings(data);
  }
  if (!response.ok) throw new Error(data.error || "Could not connect yt-dlp cookies.");
  return data;
}

export async function removeInstagramYtdlpCookies() {
  const response = await fetch("/api/settings/instagram-ytdlp-cookies", { method: "DELETE" });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Could not remove yt-dlp cookies.");
  applyServerSettings(data);
  const input = document.getElementById("instagramYtdlpCookiesInput");
  if (input) input.value = "";
  return data;
}

export function openSettings(trigger = document.activeElement, section = "downloads") {
  state.lastFocusedTrigger = trigger;
  const settings = getSavedSettings();
  renderLocationOptions(state.settings.download_locations || []);
  document.getElementById("youtubeLocationInput").value = settings.site_locations.youtube;
  document.getElementById("facebookLocationInput").value = settings.site_locations.facebook;
  document.getElementById("instagramLocationInput").value = settings.site_locations.instagram;
  document.getElementById("tiktokLocationInput").value = settings.site_locations.tiktok;
  document.getElementById("iwaraLocationInput").value = settings.site_locations.iwara;
  document.getElementById("othersLocationInput").value = settings.site_locations.others;
  document.getElementById("folderTemplateInput").value = settings.template_settings.folder_template;
  document.getElementById("filenameTemplateInput").value = settings.template_settings.filename_template;
  syncInstagramAuthInputs();
  document.getElementById("instagramPasswordInput").value = "";
  renderInstagramAuthStatus();
  const modal = document.getElementById("settingsModal");
  modal.classList.remove("hidden");
  modal.classList.add("flex");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("overflow-hidden");
  setSettingsSection(section, true);
}

export function closeSettings() {
  const modal = document.getElementById("settingsModal");
  modal.classList.add("hidden");
  modal.classList.remove("flex");
  modal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("overflow-hidden");
  if (state.lastFocusedTrigger && typeof state.lastFocusedTrigger.focus === "function") {
    state.lastFocusedTrigger.focus();
  }
}
