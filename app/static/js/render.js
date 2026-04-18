import { SITE_LABELS } from "./config.js";
import { state } from "./state.js";
import { escapeHtml } from "./utils.js";

export function getStatusBadgeClass(status) {
  if (status === "pending")
    return "bg-[color-mix(in_srgb,var(--iw-warning)_10%,transparent)] text-[var(--iw-text)] border border-[color-mix(in_srgb,var(--iw-warning)_20%,transparent)]";
  if (status === "running")
    return "bg-[var(--iw-accent)]/10 text-[var(--iw-text)] border border-[color-mix(in_srgb,var(--iw-accent)_20%,transparent)]";
  if (status === "completed")
    return "bg-[color-mix(in_srgb,var(--iw-accent2)_10%,var(--iw-panel))] text-[color-mix(in_srgb,var(--iw-text)_88%,var(--iw-accent2))] border border-[color-mix(in_srgb,var(--iw-accent2)_18%,transparent)] dark:bg-[color-mix(in_srgb,var(--iw-accent2)_14%,var(--iw-panel2))] dark:text-[var(--iw-text)] dark:border-[color-mix(in_srgb,var(--iw-accent2)_24%,transparent)]";
  return "bg-[var(--iw-soft-fill)] text-[var(--iw-text)] border border-[var(--iw-border)]";
}

export function getSiteBadgeClass(siteCategory) {
  if (siteCategory === "youtube") return "border border-[#ff3d3d]/30 bg-[#ff3d3d]/10 text-[var(--iw-text)]";
  if (siteCategory === "facebook") return "border border-[#6ea8ff]/30 bg-[#6ea8ff]/10 text-[var(--iw-text)]";
  if (siteCategory === "instagram") return "border border-[#ff8ad4]/30 bg-[#ff8ad4]/10 text-[var(--iw-text)]";
  if (siteCategory === "tiktok") return "border border-[#7ce7ff]/30 bg-[#7ce7ff]/10 text-[var(--iw-text)]";
  if (siteCategory === "iwara")
    return "border border-[var(--iw-accent2)]/30 bg-[color-mix(in_srgb,var(--iw-accent2)_12%,transparent)] text-[var(--iw-text)]";
  return "border border-[var(--iw-border)] bg-[var(--iw-soft-fill)] text-[var(--iw-muted)]";
}

export function buildLocationOptionsMarkup(locations) {
  const items = Array.isArray(locations) ? locations : [];
  if (!items.length) return "";
  return items.map((loc) => `<option value="${escapeHtml(loc)}"></option>`).join("");
}

export function renderLocationOptions(locations) {
  const list = document.getElementById("downloadLocationSuggestions");
  if (list) list.innerHTML = buildLocationOptionsMarkup(locations);
}

export function mergeTaskData(tasks) {
  return tasks.map((task) => {
    const cached = state.taskCache.get(task.vid) || {};
    const merged = {
      ...task,
      resolved_folder: task.resolved_folder || cached.resolved_folder || "",
      resolved_filename: task.resolved_filename || cached.resolved_filename || "",
      resolved_full_path: task.resolved_full_path || cached.resolved_full_path || "",
      site_category: task.site_category || cached.site_category || "others",
      site_label: task.site_label || SITE_LABELS[task.site_category] || "Others",
    };
    state.taskCache.set(task.vid, {
      resolved_folder: merged.resolved_folder,
      resolved_filename: merged.resolved_filename,
      resolved_full_path: merged.resolved_full_path,
      site_category: merged.site_category,
    });
    return merged;
  });
}

export function getMenuTasks(tasks) {
  const merged = mergeTaskData(tasks);
  if (state.activeMenu === "all") return merged;
  return merged.filter((task) => (task.site_category || "others") === state.activeMenu);
}

export function filterTasks(tasks) {
  let filtered = getMenuTasks(tasks);
  if (state.activeFilter === "active")
    filtered = filtered.filter((t) => ["pending", "running"].includes(t.status));
  if (state.activeFilter === "done")
    filtered = filtered.filter((t) => ["completed", "failed"].includes(t.status));
  const q = (state.searchQuery || "").trim().toLowerCase();
  if (q)
    filtered = filtered.filter(
      (t) =>
        (t.source_url || "").toLowerCase().includes(q) ||
        (t.resolved_filename || "").toLowerCase().includes(q) ||
        (t.resolved_folder || "").toLowerCase().includes(q) ||
        (t.vid || "").toLowerCase().includes(q)
    );
  return filtered;
}

export function countTasks(tasks) {
  return {
    queued: tasks.filter((t) => t.status === "pending").length,
    running: tasks.filter((t) => t.status === "running").length,
    completed: tasks.filter((t) => t.status === "completed").length,
    failed: tasks.filter((t) => t.status === "failed").length,
  };
}

export function updateCounts(counts) {
  document.getElementById("queuedCount").textContent = counts.queued ?? 0;
  document.getElementById("runningCount").textContent = counts.running ?? 0;
  document.getElementById("completedCount").textContent = counts.completed ?? 0;
  document.getElementById("failedCount").textContent = counts.failed ?? 0;
}

export function updateCountsForMenu(tasks) {
  updateCounts(countTasks(getMenuTasks(tasks)));
}

function _buildTaskButtons(task) {
  const BTN_DISMISS = task.can_hide
    ? `<button class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--iw-border)] bg-[var(--iw-soft-fill)] text-[var(--iw-muted)] transition-[background-color,border-color,color,filter,transform,box-shadow] duration-150 hover:-translate-y-px hover:border-[var(--iw-accent)]/30 hover:bg-[var(--iw-panel-strong)] hover:text-[var(--iw-text)]" type="button" onclick="window.hideTask('${task.vid}')" aria-label="Clear task" title="Clear task">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" class="h-4 w-4">
          <path d="M3 3l18 18"></path><path d="M10.58 10.58a2 2 0 0 0 2.84 2.84"></path>
          <path d="M9.36 5.37A10.94 10.94 0 0 1 12 5c5 0 9.27 3.11 11 7-1.02 2.29-2.8 4.24-5.09 5.49"></path>
          <path d="M6.23 6.23C4.17 7.41 2.57 9.05 1 12c1.27 2.86 3.43 5.08 6 6.32"></path>
        </svg>
      </button>`
    : "";

  const BTN_RETRY =
    task.status === "failed"
      ? `<button class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--iw-accent)]/40 bg-[var(--iw-accent)]/10 text-[var(--iw-accent)] transition-[background-color,border-color,color,filter,transform,box-shadow] duration-150 hover:-translate-y-px hover:bg-[var(--iw-accent)]/20" type="button" onclick="window.retryTask('${task.vid}')" aria-label="Retry task" title="Retry task">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="h-4 w-4"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
        </button>`
      : "";

  const BTN_CANCEL =
    task.status === "running" || task.status === "pending"
      ? `<button class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--iw-danger-border)] bg-[var(--iw-danger-bg)] text-[var(--iw-danger-text)] transition-[background-color,border-color,color,filter,transform,box-shadow] duration-150 hover:-translate-y-px hover:border-[var(--iw-danger-hover-border)] hover:bg-[var(--iw-danger-hover-bg)] hover:text-[var(--iw-danger-hover-text)]" type="button" onclick="window.cancelTask('${task.vid}')" aria-label="Cancel task" title="Cancel task">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="h-4 w-4"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>
        </button>`
      : "";

  const BTN_DEVICE =
    task.can_download && task.status !== "failed"
      ? `<button class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--iw-action-border)] bg-[var(--iw-action-bg)] text-[var(--iw-action-text)] transition-[background-color,border-color,color,filter,transform,box-shadow] duration-150 hover:-translate-y-px hover:border-[var(--iw-action-hover-border)] hover:bg-[var(--iw-action-hover-bg)] hover:text-[var(--iw-action-hover-text)]" type="button" onclick="window.downloadTaskFile('${task.vid}')" aria-label="Download file" title="Download file">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="h-4 w-4">
            <path d="M12 5v10"/><path d="M7 10l5 5 5-5"/><path d="M5 19h14"/>
          </svg>
        </button>`
      : "";

  const BTN_REMOVE =
    task.can_remove && task.status !== "running"
      ? `<button class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--iw-danger-border)] bg-[var(--iw-danger-bg)] text-[var(--iw-danger-text)] transition-[background-color,border-color,color,filter,transform,box-shadow] duration-150 hover:-translate-y-px hover:border-[var(--iw-danger-hover-border)] hover:bg-[var(--iw-danger-hover-bg)] hover:text-[var(--iw-danger-hover-text)]" type="button" onclick="window.removeTask('${task.vid}')" aria-label="Remove task from the list" title="Remove task from the list">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="h-4 w-4">
            <path d="M6 6l12 12"></path><path d="M18 6L6 18"></path>
          </svg>
        </button>`
      : "";

  return { BTN_DISMISS, BTN_RETRY, BTN_CANCEL, BTN_DEVICE, BTN_REMOVE };
}

function _buildErrorLine(task) {
  if (task.status !== "failed" || !task.error) return "";
  const lines = task.error.split("\n");
  const short = escapeHtml(lines[0]);
  if (lines.length <= 2) return `<div class="text-xs text-[var(--iw-text)]">${escapeHtml(task.error)}</div>`;
  const full = escapeHtml(task.error);
  const errId = "err-" + task.vid.replace(/[^a-zA-Z0-9]/g, "_");
  return `<div class="text-xs text-[var(--iw-text)]">
    <span id="${errId}-short">${short}… <button class="underline opacity-70 hover:opacity-100" type="button" onclick="document.getElementById('${errId}-short').style.display='none';document.getElementById('${errId}-full').style.display='block'">show more</button></span>
    <span id="${errId}-full" style="display:none;white-space:pre-wrap">${full} <button class="underline opacity-70 hover:opacity-100" type="button" onclick="document.getElementById('${errId}-full').style.display='none';document.getElementById('${errId}-short').style.display='block'">show less</button></span>
  </div>`;
}

function _renderTableView(root, visibleTasks) {
  root.innerHTML = `
    <table class="min-w-full overflow-hidden rounded-2xl border border-[var(--iw-border)] bg-[var(--iw-panel)] text-left shadow-soft">
      <thead>
        <tr class="border-b border-[var(--iw-border)] text-xs uppercase tracking-[0.08em] text-[var(--iw-muted)]">
          <th class="px-4 py-3">Site</th>
          <th class="px-4 py-3">Source</th>
          <th class="px-4 py-3">Folder</th>
          <th class="px-4 py-3">Filename</th>
          <th class="px-4 py-3">Status</th>
          <th class="px-4 py-3">Progress</th>
          <th class="px-4 py-3"></th>
        </tr>
      </thead>
      <tbody>
        ${visibleTasks
          .map((task) => {
            const { BTN_DISMISS, BTN_RETRY, BTN_CANCEL, BTN_DEVICE, BTN_REMOVE } = _buildTaskButtons(task);
            const pct = Math.max(0, Math.min(100, Number(task.progress_pct) || 0));
            return `
              <tr class="border-b border-[var(--iw-border)] last:border-b-0">
                <td class="px-4 py-3 align-top"><span class="inline-flex rounded-xl px-3 py-1 text-xs font-bold ${getSiteBadgeClass(task.site_category)}">${escapeHtml(task.site_label)}</span></td>
                <td class="px-4 py-3 align-top"><div class="break-all text-sm text-[var(--iw-muted)]">${escapeHtml(task.source_url || task.vid)}</div></td>
                <td class="px-4 py-3 align-top text-sm font-semibold">${task.resolved_folder ? escapeHtml(task.resolved_folder) : ""}</td>
                <td class="px-4 py-3 align-top text-sm">${task.resolved_filename ? escapeHtml(task.resolved_filename) : ""}</td>
                <td class="px-4 py-3 align-top"><span class="inline-flex rounded-xl px-3 py-1 text-xs font-bold ${getStatusBadgeClass(task.status)}">${escapeHtml(task.status_label)}</span></td>
                <td class="px-4 py-3 align-top text-sm text-[var(--iw-muted)]">${pct}%</td>
                <td class="px-4 py-3 align-top text-right"><div class="inline-flex gap-2">${BTN_DISMISS}${BTN_RETRY}${BTN_CANCEL}${BTN_DEVICE}${BTN_REMOVE}</div></td>
              </tr>`;
          })
          .join("")}
      </tbody>
    </table>`;
}

function _renderGridView(root, visibleTasks) {
  root.innerHTML = visibleTasks
    .map((task) => {
      const pct = Math.max(0, Math.min(100, Number(task.progress_pct) || 0));
      const { BTN_DISMISS, BTN_RETRY, BTN_CANCEL, BTN_DEVICE, BTN_REMOVE } = _buildTaskButtons(task);
      const errorLine = _buildErrorLine(task);
      return `
        <article class="w-full rounded-2xl border border-[var(--iw-border)] bg-[var(--iw-panel)] p-4 shadow-soft">
          <div class="grid grid-cols-1 gap-3">
            <div class="grid grid-cols-[minmax(0,1fr)_auto] items-start gap-3">
              <div class="min-w-0">
                <span class="inline-flex max-w-full rounded-xl px-3 py-1 text-xs font-bold ${getSiteBadgeClass(task.site_category)}">${escapeHtml(task.site_label)}</span>
              </div>
              <div class="flex shrink-0 flex-wrap items-start justify-end gap-2">
                <span class="inline-flex rounded-xl px-3 py-1 text-xs font-bold ${getStatusBadgeClass(task.status)}">${escapeHtml(task.status_label)}</span>
                ${BTN_DISMISS}${BTN_RETRY}${BTN_CANCEL}${BTN_DEVICE}${BTN_REMOVE}
              </div>
            </div>
            <div class="min-w-0 break-all text-sm text-[var(--iw-muted)]">${escapeHtml(task.source_url || task.vid)}</div>
            <div class="grid gap-2">
              ${task.resolved_folder ? `<div class="break-all text-sm font-semibold">${escapeHtml(task.resolved_folder)}</div>` : ""}
              ${task.resolved_filename ? `<div class="break-all text-sm">${escapeHtml(task.resolved_filename)}</div>` : ""}
              ${errorLine}
            </div>
          </div>
          <div class="mt-4">
            <div class="h-2.5 overflow-hidden rounded-full bg-[var(--iw-soft-fill)]">
              <div class="h-full rounded-full bg-[var(--iw-accent)] transition-all" style="width:${pct}%"></div>
            </div>
            <div class="mt-2 flex justify-end text-xs text-[var(--iw-muted)]">${pct}%</div>
          </div>
        </article>`;
    })
    .join("");
}

export function renderTasks(tasks) {
  const root = document.getElementById("tasks");
  const visibleTasks = filterTasks(tasks);
  root.className =
    state.viewMode === "table"
      ? "w-full overflow-x-auto"
      : "grid w-full grid-cols-1 gap-4 md:grid-cols-2";

  if (!visibleTasks.length) {
    root.innerHTML = `<div class="rounded-2xl border border-dashed border-[var(--iw-border)] bg-[var(--iw-panel)] px-5 py-10 text-center text-[var(--iw-muted)]">No ${escapeHtml(SITE_LABELS[state.activeMenu] || "matching")} tasks right now.</div>`;
    return;
  }

  if (state.viewMode === "table") {
    _renderTableView(root, visibleTasks);
  } else {
    _renderGridView(root, visibleTasks);
  }
}

export function applyButtonState(button, active) {
  button.setAttribute("aria-pressed", String(active));
  button.classList.toggle("bg-[var(--iw-accent)]", active);
  button.classList.toggle("text-[color:var(--iw-strong-text)]", active);
  button.classList.toggle("text-[var(--iw-muted)]", !active);
  button.classList.toggle("hover:text-[var(--iw-text)]", !active);
}

export function updateMenuButtons() {
  document.querySelectorAll(".menu-btn").forEach((btn) => {
    const active = btn.dataset.menu === state.activeMenu;
    btn.setAttribute("aria-pressed", active ? "true" : "false");
    btn.classList.toggle("bg-[var(--iw-accent)]", active);
    btn.classList.toggle("text-[color:var(--iw-strong-text)]", active);
    btn.classList.toggle("text-[var(--iw-muted)]", !active);
    btn.classList.toggle("hover:text-[var(--iw-text)]", !active);
  });
}

export function updateSaveModeButtons() {
  const saveMode = state.settings.save_mode === "device" ? "device" : "nas";
  document.querySelectorAll(".save-mode-btn").forEach((btn) => {
    const active = btn.dataset.saveMode === saveMode;
    btn.setAttribute("aria-pressed", active ? "true" : "false");
    btn.classList.toggle("bg-[var(--iw-accent)]", active);
    btn.classList.toggle("text-[color:var(--iw-strong-text)]", active);
    btn.classList.toggle("text-[var(--iw-muted)]", !active);
  });
}

export function updateViewButtons() {
  const isGrid = state.viewMode === "grid";
  applyButtonState(document.getElementById("gridViewButton"), isGrid);
  applyButtonState(document.getElementById("tableViewButton"), !isGrid);
}

export function updateFilterButtons() {
  document.querySelectorAll(".filter-btn").forEach((btn) => {
    applyButtonState(btn, btn.dataset.filter === state.activeFilter);
  });
}

export function setViewMode(mode) {
  state.viewMode = mode === "table" ? "table" : "grid";
  localStorage.setItem("neverstelle.viewMode", state.viewMode);
  updateViewButtons();
  renderTasks(state.visibleTasks || []);
}

export function updateSettingsSectionButtons() {
  document.querySelectorAll(".settings-nav-btn").forEach((btn) => {
    const active = btn.dataset.settingsSection === state.settingsSection;
    btn.setAttribute("aria-pressed", active ? "true" : "false");
    btn.classList.toggle("text-[var(--iw-muted)]", !active);
    btn.classList.toggle("hover:text-[var(--iw-text)]", !active);
  });
}

export function updateSettingsPanels() {
  document.querySelectorAll(".settings-panel").forEach((panel) => {
    panel.classList.toggle("hidden", panel.dataset.settingsPanel !== state.settingsSection);
  });
}

export function setSettingsSection(section, shouldFocus = false) {
  state.settingsSection = ["downloads", "instagram", "advanced"].includes(section) ? section : "downloads";
  updateSettingsSectionButtons();
  updateSettingsPanels();
  if (!shouldFocus) return;
  const focusTargets = { downloads: "youtubeLocationInput", instagram: "instagramIdentifierInput", advanced: "folderTemplateInput" };
  const target = document.getElementById(focusTargets[state.settingsSection]);
  if (target) requestAnimationFrame(() => target.focus());
}

export function setActiveMenu(menu) {
  state.activeMenu = SITE_LABELS[menu] ? menu : "all";
  localStorage.setItem("neverstelle.activeMenu", state.activeMenu);
  updateMenuButtons();
  renderTasks(state.visibleTasks || []);
  updateCountsForMenu(state.visibleTasks || []);
}
