<script setup lang="ts">
import type { Component } from "vue";

import type { PageKey, SettingsSection } from "../../types";
import AccountMenu from "./AccountMenu.vue";

defineProps<{
  activePage: PageKey;
  isLightMode: boolean;
  pageItems: Array<{ key: PageKey; label: string; icon: Component }>;
  settingsOpen: boolean;
}>();

const emit = defineEmits<{
  openSettings: [event: Event, section?: SettingsSection];
  selectPage: [page: PageKey];
  toggleTheme: [];
}>();
</script>

<template>
  <nav
    class="fixed z-50 inset-x-0 bottom-0 flex w-screen gap-1 overflow-hidden glass-chrome pt-1.5 px-1.5 pb-[max(0.5rem,env(safe-area-inset-bottom))] lg:hidden"
    aria-label="App navigation"
  >
    <button
      v-for="item in pageItems"
      :key="item.key"
      type="button"
      class="inline-flex flex-col items-center justify-center gap-1 flex-1 w-full min-w-0 h-14 px-0 rounded-lg bg-transparent leading-none text-white/65 in-[.light-mode]:text-black/65 transition-all duration-200 ease-glass active:scale-[0.94] hover:text-white in-[.light-mode]:hover:text-black aria-pressed:bg-accent aria-pressed:text-black"
      :aria-pressed="activePage === item.key"
      @click="emit('selectPage', item.key)"
    >
      <component :is="item.icon" class="w-6 h-6" aria-hidden="true" />
      <span class="text-10px font-medium tracking-wide">{{
        item.label
      }}</span>
    </button>
    <AccountMenu
      class="flex-1 min-w-0"
      :is-light-mode="isLightMode"
      :settings-open="settingsOpen"
      variant="bottom"
      @open-settings="(event, section) => emit('openSettings', event, section)"
      @toggle-theme="emit('toggleTheme')"
    />
  </nav>
</template>

<style>
/* Single source of truth for the fixed bottom nav's height.
   = pt-1.5 (0.375rem) + h-14 buttons (3.5rem) + pb (max 0.5rem / safe-area).
   BarStatus reads this so it never has to hardcode a clearance margin. */
:root {
  --nav-bottom-height: calc(
    3.875rem + max(0.5rem, env(safe-area-inset-bottom))
  );
}
</style>
