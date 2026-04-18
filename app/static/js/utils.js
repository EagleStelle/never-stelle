export function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function announce(message) {
  const el = document.getElementById("srStatus");
  if (el) el.textContent = message;
}

export function toast(message, type = "success") {
  const wrap = document.getElementById("toastWrap");
  if (!wrap) return;
  const el = document.createElement("div");
  el.className = [
    "rounded-2xl border px-4 py-3 text-sm font-medium shadow-soft transition",
    type === "error"
      ? "border-[color-mix(in_srgb,var(--iw-danger)_30%,transparent)] bg-[var(--iw-panel)] text-[var(--iw-text)]"
      : "border-[color-mix(in_srgb,var(--iw-success)_30%,transparent)] bg-[var(--iw-panel)] text-[var(--iw-text)]",
  ].join(" ");
  el.textContent = message;
  wrap.appendChild(el);
  announce(message);
  setTimeout(() => {
    el.style.opacity = "0";
    el.style.transform = "translateY(6px)";
    setTimeout(() => el.remove(), 220);
  }, 3200);
}

export function formatTimestamp(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString();
}

export function getFocusableInModal() {
  return Array.from(
    document.querySelectorAll(
      "#settingsModal button, #settingsModal select, #settingsModal input, #settingsModal textarea"
    )
  );
}
