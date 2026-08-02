<script setup lang="ts">
import { computed, nextTick, ref, useTemplateRef, watch } from "vue";
import { useEventListener } from "@vueuse/core";
import IconAccount from "~icons/material-symbols/account-circle";
import IconChevronUp from "~icons/material-symbols/keyboard-arrow-up";
import IconGear from "~icons/material-symbols/settings";
import IconLogout from "~icons/material-symbols/logout";
import IconMoon from "~icons/material-symbols/dark-mode";
import IconSun from "~icons/material-symbols/light-mode";

import { useAuth } from "@/composables/useAuth";
import { useDashboard } from "@/composables/useDashboard";

type AccountMenuVariant = "sidebar" | "sidebar-collapsed" | "bottom";

const props = withDefaults(
  defineProps<{
    variant?: AccountMenuVariant;
  }>(),
  { variant: "sidebar" },
);

const auth = useAuth();
const { isLightMode, openSettings, settingsOpen, toggleThemeMode } =
  useDashboard();
const open = ref(false);
const trigger = useTemplateRef<HTMLButtonElement>("trigger");
const menu = useTemplateRef<HTMLElement>("menu");
const menuStyle = ref<Record<string, string>>({});

const displayUsername = computed(() => auth.username.value || "root");
const isSidebar = computed(() => props.variant === "sidebar");
const isCollapsed = computed(() => props.variant === "sidebar-collapsed");
const isBottom = computed(() => props.variant === "bottom");

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function updateMenuPosition(): void {
  const triggerEl = trigger.value;
  if (!triggerEl) return;

  const gap = 8;
  const edge = 8;
  const triggerRect = triggerEl.getBoundingClientRect();
  const width = isBottom.value
    ? Math.min(260, window.innerWidth - edge * 2)
    : isSidebar.value
      ? triggerRect.width
      : 236;
  const measuredHeight = menu.value?.getBoundingClientRect().height || 176;
  let left = triggerRect.left;
  let top = triggerRect.top - measuredHeight - gap;

  if (isCollapsed.value) {
    left = triggerRect.right + gap;
    top = triggerRect.bottom - measuredHeight;
  }

  if (isBottom.value) {
    left = triggerRect.right - width;
    top = triggerRect.top - measuredHeight - gap;
  }

  menuStyle.value = {
    left: `${clamp(left, edge, window.innerWidth - width - edge)}px`,
    top: `${clamp(top, edge, window.innerHeight - measuredHeight - edge)}px`,
    width: `${width}px`,
  };
}

async function openMenu(): Promise<void> {
  open.value = true;
  await nextTick();
  updateMenuPosition();
  requestAnimationFrame(updateMenuPosition);
}

function toggleMenu(): void {
  if (open.value) {
    open.value = false;
    return;
  }
  void openMenu();
}

function chooseTheme(): void {
  toggleThemeMode();
  open.value = false;
}

function chooseSettings(event: Event): void {
  openSettings(event, "account");
  open.value = false;
}

async function chooseLogout(): Promise<void> {
  open.value = false;
  await auth.logout();
}

function onDocumentPointerDown(event: PointerEvent): void {
  if (!open.value) return;
  const target = event.target;
  if (!(target instanceof Node)) return;
  if (trigger.value?.contains(target) || menu.value?.contains(target)) return;
  open.value = false;
}

useEventListener(document, "pointerdown", onDocumentPointerDown);
useEventListener(window, "resize", () => {
  if (open.value) updateMenuPosition();
});
useEventListener(
  window,
  "scroll",
  () => {
    if (open.value) updateMenuPosition();
  },
  { capture: true, passive: true },
);

watch(
  () => props.variant,
  () => {
    if (open.value) void openMenu();
  },
);
</script>

<template>
  <div class="relative" :class="isCollapsed ? 'flex justify-center' : 'w-full'">
    <button
      v-if="isBottom"
      ref="trigger"
      type="button"
      class="inline-flex h-14 w-full min-w-0 flex-1 flex-col items-center justify-center gap-1 rounded-lg bg-transparent px-0 leading-none text-white/65 transition-all duration-200 ease-glass hover:text-white active:scale-[0.94] in-[.light-mode]:text-black/65 in-[.light-mode]:hover:text-black"
      :aria-expanded="open"
      aria-haspopup="menu"
      @click="toggleMenu"
    >
      <IconAccount class="h-6 w-6" aria-hidden="true" />
      <span class="text-10px font-medium tracking-wide">Account</span>
    </button>

    <button
      v-else
      ref="trigger"
      type="button"
      class="group/account flex min-w-0 items-center gap-3 rounded-lg bg-transparent text-left text-white/80 transition-all duration-300 ease-glass hover:bg-white/10 hover:text-white active:scale-[0.98] in-[.light-mode]:text-black/80 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black"
      :class="isCollapsed ? 'h-10 w-10 justify-center px-0' : 'h-12 w-full px-2.5'"
      :aria-expanded="open"
      aria-haspopup="menu"
      :title="isCollapsed ? displayUsername : undefined"
      @click="toggleMenu"
    >
      <IconAccount class="shrink-0 h-5 w-5" aria-hidden="true" />
      <span v-show="isSidebar" class="min-w-0 flex-1">
        <span class="block truncate text-sm font-semibold leading-tight">
          {{ displayUsername }}
        </span>
        <span
          class="block truncate text-xs leading-tight text-white/45 in-[.light-mode]:text-black/45"
        >
          Root account
        </span>
      </span>
      <IconChevronUp
        v-show="isSidebar"
        class="h-5 w-5 shrink-0 text-white/50 transition-transform duration-300 in-[.light-mode]:text-black/50"
        :class="{ 'rotate-180': open }"
        aria-hidden="true"
      />
    </button>

    <Teleport to="body">
      <div
        v-if="open"
        ref="menu"
        class="fixed z-1000 rounded-lg border border-(--glass-border) bg-(--glass-strong) p-2 shadow-none backdrop-blur-2xl"
        :style="menuStyle"
        role="menu"
        aria-label="Account menu"
      >
        <div
          class="truncate px-3 py-2 text-sm font-medium text-white/60 in-[.light-mode]:text-black/60"
        >
          {{ displayUsername }}
        </div>
        <button
          type="button"
          class="account-menu-item text-white/90 hover:bg-white/10 hover:text-white in-[.light-mode]:text-black/85 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black"
          role="menuitem"
          @click="chooseTheme"
        >
          <IconMoon
            v-if="isLightMode"
            class="h-5 w-5 shrink-0"
            aria-hidden="true"
          />
          <IconSun v-else class="h-5 w-5 shrink-0" aria-hidden="true" />
          <span>{{ isLightMode ? "Dark Mode" : "Light Mode" }}</span>
        </button>
        <button
          type="button"
          class="account-menu-item text-white/90 hover:bg-white/10 hover:text-white in-[.light-mode]:text-black/85 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black"
          :class="{ 'bg-accent! text-black!': settingsOpen }"
          role="menuitem"
          :aria-pressed="settingsOpen"
          @click="chooseSettings"
        >
          <IconGear class="h-5 w-5 shrink-0" aria-hidden="true" />
          <span>Settings</span>
        </button>
        <div class="my-1 h-px bg-white/10 in-[.light-mode]:bg-black/10" />
        <button
          type="button"
          class="account-menu-item text-white/90 hover:bg-white/10 hover:text-white in-[.light-mode]:text-black/85 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black"
          role="menuitem"
          @click="chooseLogout"
        >
          <IconLogout class="h-5 w-5 shrink-0" aria-hidden="true" />
          <span>Logout</span>
        </button>
      </div>
    </Teleport>
  </div>
</template>

<style scoped>
.account-menu-item {
  display: flex;
  width: 100%;
  align-items: center;
  gap: 0.75rem;
  border-radius: 0.5rem;
  padding: 0.7rem 0.75rem;
  font-size: 0.95rem;
  font-weight: 600;
  line-height: 1;
  text-align: left;
  transition:
    background 220ms var(--ease-glass),
    color 220ms var(--ease-glass),
    transform 220ms var(--ease-glass);
}

.account-menu-item:active {
  transform: scale(0.98);
}
</style>
