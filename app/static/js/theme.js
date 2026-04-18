export function getThemeMode() {
  return localStorage.getItem("neverstelle.themeMode") === "light" ? "light" : "dark";
}

export function updateThemeToggleButton() {
  const button = document.getElementById("themeToggleButton");
  const icon = document.getElementById("themeToggleIcon");
  if (!button || !icon) return;
  const isLight = document.body.classList.contains("light-mode");
  button.setAttribute("aria-pressed", String(isLight));
  button.setAttribute("title", isLight ? "Switch to dark mode" : "Switch to light mode");
  button.setAttribute("aria-label", isLight ? "Switch to dark mode" : "Switch to light mode");
  icon.innerHTML = isLight
    ? '<circle cx="12" cy="12" r="4"></circle><path d="M12 2v2.2M12 19.8V22M4.93 4.93l1.56 1.56M17.51 17.51l1.56 1.56M2 12h2.2M19.8 12H22M4.93 19.07l1.56-1.56M17.51 6.49l1.56-1.56"></path>'
    : '<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8Z"></path>';
}

export function applyTheme(mode) {
  const nextMode = mode === "light" ? "light" : "dark";
  document.body.classList.toggle("light-mode", nextMode === "light");
  localStorage.setItem("neverstelle.themeMode", nextMode);
  updateThemeToggleButton();
}

export function toggleThemeMode() {
  applyTheme(document.body.classList.contains("light-mode") ? "dark" : "light");
}
