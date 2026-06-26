<script setup lang="ts">
import type { Component } from "vue";
import IconMoon from "~icons/material-symbols/dark-mode";
import IconGear from "~icons/material-symbols/settings";
import IconSun from "~icons/material-symbols/light-mode";

import type { PageKey } from "../../types";

defineProps<{
  activePage: PageKey;
  isLightMode: boolean;
  pageItems: Array<{ key: PageKey; label: string; icon: Component }>;
}>();

const emit = defineEmits<{
  openSettings: [event: Event];
  selectPage: [page: PageKey];
  toggleTheme: [];
}>();
</script>

<template>
  <nav class="fixed z-50 inset-x-0 bottom-0 flex w-[100vw] gap-[0.25rem] overflow-hidden border-t border-accent-subtle bg-[color-mix(in_srgb,var(--panel)_92%,var(--bg))] pt-[0.45rem] px-[0.35rem] pb-[calc(0.45rem+env(safe-area-inset-bottom))] lg:hidden" aria-label="App navigation">
    <button
      v-for="item in pageItems"
      :key="item.key"
      type="button"
      class="inline-flex flex-col items-center justify-center flex-1 w-full min-w-0 min-h-[3.4rem] px-[0.1rem] border border-transparent rounded-lg bg-transparent text-text-muted text-[0.66rem] font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] aria-pressed:bg-accent aria-pressed:text-bg"
      :aria-pressed="activePage === item.key"
      @click="emit('selectPage', item.key)"
    >
      <component :is="item.icon" class="w-5 h-5 mb-1" aria-hidden="true" />
      <span>{{ item.label }}</span>
    </button>
    <button
      type="button"
      class="inline-flex flex-col items-center justify-center flex-1 w-full min-w-0 min-h-[3.4rem] px-[0.1rem] border border-transparent rounded-lg bg-transparent text-text-muted text-[0.66rem] font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:text-text"
      @click="emit('toggleTheme')"
    >
      <IconSun v-if="isLightMode" class="w-5 h-5 mb-1" aria-hidden="true" />
      <IconMoon v-else class="w-5 h-5 mb-1" aria-hidden="true" />
      <span>Theme</span>
    </button>
    <button
      type="button"
      class="inline-flex flex-col items-center justify-center flex-1 w-full min-w-0 min-h-[3.4rem] px-[0.1rem] border border-transparent rounded-lg bg-transparent text-text-muted text-[0.66rem] font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:text-text"
      @click="emit('openSettings', $event)"
    >
      <IconGear class="w-5 h-5 mb-1" aria-hidden="true" />
      <span>Settings</span>
    </button>
  </nav>
</template>
