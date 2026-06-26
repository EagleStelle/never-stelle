<script setup lang="ts">
import IconMoon from "~icons/material-symbols/dark-mode";
import IconGear from "~icons/material-symbols/settings";
import IconSun from "~icons/material-symbols/light-mode";
import IconPanelOpen from "~icons/material-symbols/left-panel-open";
import IconPanelClose from "~icons/material-symbols/left-panel-close";
import { ref } from "vue";
import type { Component } from "vue";

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

const isExpanded = ref(false);
</script>

<template>
  <aside 
    class="hidden lg:flex sticky top-0 h-[100dvh] min-w-0 flex-col gap-[0.75rem] border-r border-accent-subtle bg-panel py-[0.65rem] px-[0.5rem] transition-all duration-[250ms] ease-[cubic-bezier(0.4,0,0.2,1)]" 
    :class="isExpanded ? 'w-[16rem]' : 'w-[4.5rem] items-center'"
    aria-label="App navigation"
  >
    <div class="group flex items-center w-full mb-2 h-10" :class="isExpanded ? 'px-2 justify-between' : 'justify-center relative'">
      <a href="/" class="inline-flex items-center min-w-0 gap-[0.6rem] text-accent text-[1.4rem] font-[800] leading-none no-underline transition-opacity duration-200" :class="!isExpanded ? 'group-hover:opacity-0' : ''" aria-label="Never Stelle Home">
        <img src="/assets/logo.png" alt="" class="w-[2.2rem] h-[2.2rem] shrink-0" />
        <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden transition-opacity duration-200" :class="isExpanded ? 'opacity-100' : 'opacity-0'">Never Stelle</span>
      </a>
      
      <button 
        @click="isExpanded = !isExpanded" 
        class="flex items-center justify-center text-text-muted hover:text-text transition-all duration-200"
        :class="isExpanded ? 'w-8 h-8 rounded-full hover:bg-panel-subtle shrink-0' : 'absolute inset-0 w-full h-full opacity-0 group-hover:opacity-100 bg-panel'"
        :aria-label="isExpanded ? 'Collapse sidebar' : 'Expand sidebar'"
      >
        <IconPanelClose v-if="isExpanded" class="w-6 h-6" />
        <IconPanelOpen v-else class="w-6 h-6" />
      </button>
    </div>

    <nav class="flex gap-[0.35rem]" :class="isExpanded ? 'w-full flex-col' : 'flex-col items-center w-full'" aria-label="App navigation">
      <button
        v-for="item in pageItems"
        :key="item.key"
        type="button"
        class="inline-flex items-center rounded-lg bg-transparent text-text-muted leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:text-text hover:bg-panel-subtle aria-pressed:bg-accent aria-pressed:text-bg"
        :class="isExpanded ? 'w-full flex-row justify-start px-4 h-10 gap-3' : 'w-10 h-10 justify-center'"
        :aria-pressed="activePage === item.key"
        :title="!isExpanded ? item.label : undefined"
        @click="emit('selectPage', item.key)"
      >
        <component :is="item.icon" class="shrink-0 w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
        <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden text-[0.95rem] font-[600]">{{ item.label }}</span>
      </button>
    </nav>

    <div class="flex gap-[0.35rem] mt-auto" :class="isExpanded ? 'w-full flex-col' : 'flex-col items-center w-full'">
      <button
        type="button"
        class="inline-flex items-center rounded-lg bg-transparent text-text-muted leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:text-text hover:bg-panel-subtle"
        :class="isExpanded ? 'w-full flex-row justify-start px-4 h-10 gap-3' : 'w-10 h-10 justify-center'"
        :aria-label="isLightMode ? 'Switch to dark mode' : 'Switch to light mode'"
        :title="!isExpanded ? (isLightMode ? 'Switch to dark mode' : 'Switch to light mode') : undefined"
        @click="emit('toggleTheme')"
      >
        <IconSun v-if="isLightMode" class="shrink-0 w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
        <IconMoon v-else class="shrink-0 w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
        <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden text-[0.95rem] font-[600]">{{ isLightMode ? 'Dark Mode' : 'Light Mode' }}</span>
      </button>
      <button 
        type="button" 
        class="inline-flex items-center rounded-lg bg-transparent text-text-muted leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:text-text hover:bg-panel-subtle aria-pressed:bg-accent aria-pressed:text-bg"
        :class="[isExpanded ? 'w-full flex-row justify-start px-4 h-10 gap-3' : 'w-10 h-10 justify-center', activePage === 'settings' ? 'bg-accent text-bg' : '']"
        aria-label="Open settings" 
        :title="!isExpanded ? 'Settings' : undefined" 
        @click="emit('openSettings', $event)"
      >
        <IconGear class="shrink-0 w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
        <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden text-[0.95rem] font-[600]">Settings</span>
      </button>
    </div>
  </aside>
</template>
