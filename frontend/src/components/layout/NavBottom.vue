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
  <nav class="fixed z-50 inset-x-0 bottom-0 flex w-screen gap-1 overflow-hidden bg-secondary pt-1.5 px-1.5 pb-2 lg:hidden" aria-label="App navigation">
    <button
      v-for="item in pageItems"
      :key="item.key"
      type="button"
      class="inline-flex flex-col items-center justify-center flex-1 w-full min-w-0 h-9 px-0 border border-transparent rounded-lg bg-transparent text-text-muted leading-none transition-all duration-200 ease-out active:scale-95 aria-pressed:bg-primary aria-pressed:text-background"
      :aria-pressed="activePage === item.key"
      @click="emit('selectPage', item.key)"
    >
      <component :is="item.icon" class="w-4 h-4 mb-1" aria-hidden="true" />
      <span>{{ item.label }}</span>
    </button>
    <button
      type="button"
      class="inline-flex flex-col items-center justify-center flex-1 w-full min-w-0 h-9 px-0 border border-transparent rounded-lg bg-transparent text-text-muted leading-none transition-all duration-200 ease-out active:scale-95 hover:text-text"
      @click="emit('toggleTheme')"
    >
      <IconSun v-if="isLightMode" class="w-4 h-4 mb-1" aria-hidden="true" />
      <IconMoon v-else class="w-4 h-4 mb-1" aria-hidden="true" />
      <span>Theme</span>
    </button>
    <button
      type="button"
      class="inline-flex flex-col items-center justify-center flex-1 w-full min-w-0 h-9 px-0 border border-transparent rounded-lg bg-transparent text-text-muted leading-none transition-all duration-200 ease-out active:scale-95 hover:text-text aria-pressed:bg-primary aria-pressed:text-background"
      :aria-pressed="activePage === 'settings'"
      @click="emit('openSettings', $event)"
    >
      <IconGear class="w-4 h-4 mb-1" aria-hidden="true" />
      <span>Settings</span>
    </button>
  </nav>
</template>
