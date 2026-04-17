import { state } from "./state.js";
import { getFocusableInModal, toast } from "./utils.js";
import { applyTheme, getThemeMode, toggleThemeMode } from "./theme.js";
import {
  renderTasks,
  updateMenuButtons,
  updateSaveModeButtons,
  updateViewButtons,
  updateFilterButtons,
  setViewMode,
  setActiveMenu,
  setSettingsSection,
  updateCountsForMenu,
} from "./render.js";
import {
  getSavedSettings,
  persistSettings,
  fetchUIConfig,
  openSettings,
  closeSettings,
  updateInstagramIdentifierField,
  configureInstagramAuth,
  removeInstagramAuth,
  configureInstagramYtdlpCookies,
  removeInstagramYtdlpCookies,
  renderInstagramAuthStatus,
  renderInstagramYtdlpCookiesStatus,
  syncInstagramAuthInputs,
} from "./settings.js";
import { loadTasks } from "./api.js";
import {
  addTask,
  clearPending,
  clearCompleted,
  cleanNfo,
} from "./tasks.js";

// ── Event listeners ────────────────────────────────────────────────────────────

document.getElementById("downloadForm").addEventListener("submit", addTask);

document.getElementById("taskSearch").addEventListener("input", (e) => {
  state.searchQuery = e.target.value;
  renderTasks(state.visibleTasks || []);
  updateCountsForMenu(state.visibleTasks || []);
});

document.querySelectorAll(".menu-btn").forEach((button) => {
  button.addEventListener("click", () => setActiveMenu(button.dataset.menu));
});

document.querySelectorAll(".save-mode-btn").forEach((button) => {
  button.addEventListener("click", async () => {
    const settings = getSavedSettings();
    const nextMode = button.dataset.saveMode === "device" ? "device" : "nas";
    if (settings.save_mode === nextMode) return;
    settings.save_mode = nextMode;
    try {
      await persistSettings(settings, "Save mode updated.");
      updateSaveModeButtons();
    } catch (error) {
      toast(error.message || "Could not save save mode.", "error");
    }
  });
});

document.getElementById("settingsButton").addEventListener("click", (event) =>
  openSettings(event.currentTarget)
);

document.querySelectorAll(".settings-nav-btn").forEach((button) => {
  button.addEventListener("click", () => setSettingsSection(button.dataset.settingsSection, true));
});

document.getElementById("instagramIdentifierTypeInput").addEventListener("change", updateInstagramIdentifierField);
document.getElementById("cancelSettingsButton").addEventListener("click", closeSettings);
document.getElementById("closeSettingsButton").addEventListener("click", closeSettings);

document.getElementById("saveSettingsButton").addEventListener("click", async () => {
  const settings = getSavedSettings();
  settings.site_locations = {
    youtube: document.getElementById("youtubeLocationInput").value || state.defaults.site_locations.youtube || "",
    facebook: document.getElementById("facebookLocationInput").value || state.defaults.site_locations.facebook || "",
    instagram: document.getElementById("instagramLocationInput").value || state.defaults.site_locations.instagram || "",
    tiktok: document.getElementById("tiktokLocationInput").value || state.defaults.site_locations.tiktok || "",
    iwara: document.getElementById("iwaraLocationInput").value || state.defaults.site_locations.iwara || "",
    others: document.getElementById("othersLocationInput").value || state.defaults.site_locations.others || "",
  };
  settings.template_settings = {
    folder_template: document.getElementById("folderTemplateInput").value || state.defaults.template_settings.folder_template || "",
    filename_template: document.getElementById("filenameTemplateInput").value || state.defaults.template_settings.filename_template || "",
  };
  try {
    await persistSettings(settings, "");
    toast("Settings saved.");
    closeSettings();
  } catch (error) {
    toast(error.message || "Could not save settings.", "error");
  }
});

document.getElementById("connectInstagramAuthButton").addEventListener("click", async () => {
  const identifierType = document.getElementById("instagramIdentifierTypeInput").value || "username";
  const identifier = (document.getElementById("instagramIdentifierInput").value || "").trim();
  const password = document.getElementById("instagramPasswordInput").value || "";
  try {
    const data = await configureInstagramAuth(identifierType, identifier, password);
    if (state.settings.instagram_auth && state.settings.instagram_auth.session_saved) {
      document.getElementById("instagramPasswordInput").value = "";
    }
    syncInstagramAuthInputs();
    return data;
  } catch (error) {
    renderInstagramAuthStatus();
    toast("Could not reach the Instagram auth endpoint.", "error");
  }
});

document.getElementById("removeInstagramAuthButton").addEventListener("click", async () => {
  if (document.getElementById("removeInstagramAuthButton").disabled) return;
  try {
    await removeInstagramAuth();
  } catch (error) {
    toast(error.message || "Could not remove Instagram login.", "error");
  }
});

document.getElementById("connectInstagramYtdlpCookiesButton").addEventListener("click", async () => {
  const input = document.getElementById("instagramYtdlpCookiesInput");
  const file = input && input.files && input.files[0] ? input.files[0] : null;
  if (!file) {
    toast("Choose a cookies file first.", "error");
    return;
  }
  try {
    await configureInstagramYtdlpCookies(file);
    if (input) input.value = "";
  } catch (error) {
    renderInstagramYtdlpCookiesStatus();
    toast(error.message || "Could not connect yt-dlp cookies.", "error");
  }
});

document.getElementById("removeInstagramYtdlpCookiesButton").addEventListener("click", async () => {
  if (document.getElementById("removeInstagramYtdlpCookiesButton").disabled) return;
  try {
    await removeInstagramYtdlpCookies();
  } catch (error) {
    toast(error.message || "Could not remove yt-dlp cookies.", "error");
  }
});

document.getElementById("clearPendingButton").addEventListener("click", clearPending);
document.getElementById("clearCompletedButton").addEventListener("click", clearCompleted);
document.getElementById("cleanNfoButton").addEventListener("click", cleanNfo);

document.getElementById("settingsModal").addEventListener("click", (event) => {
  if (event.target.id === "settingsModal") closeSettings();
});

document.querySelectorAll(".filter-btn").forEach((button) => {
  button.addEventListener("click", () => {
    state.activeFilter = button.dataset.filter;
    localStorage.setItem("neverstelle.activeFilter", state.activeFilter);
    updateFilterButtons();
    renderTasks(state.visibleTasks || []);
  });
});

document.getElementById("gridViewButton").addEventListener("click", () => setViewMode("grid"));
document.getElementById("tableViewButton").addEventListener("click", () => setViewMode("table"));
document.getElementById("themeToggleButton").addEventListener("click", toggleThemeMode);

document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    if (state.pollHandle) {
      clearInterval(state.pollHandle);
      state.pollHandle = null;
    }
  } else {
    loadTasks(true);
  }
});

document.addEventListener("keydown", (event) => {
  const modal = document.getElementById("settingsModal");
  if (event.key === "Escape" && modal.classList.contains("flex")) {
    closeSettings();
    return;
  }
  if (event.key === "Tab" && modal.classList.contains("flex")) {
    const focusable = getFocusableInModal();
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (!first || !last) return;
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }
});

// ── Init ───────────────────────────────────────────────────────────────────────

if (!localStorage.getItem("neverstelle.activeMenu")) {
  localStorage.setItem("neverstelle.activeMenu", "all");
}
state.activeMenu = localStorage.getItem("neverstelle.activeMenu") || "all";
applyTheme(getThemeMode());
updateViewButtons();
updateFilterButtons();
updateMenuButtons();
updateSaveModeButtons();
fetchUIConfig();
loadTasks();
if ("Notification" in window && Notification.permission === "default") {
  Notification.requestPermission();
}
