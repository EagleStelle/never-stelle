<script setup lang="ts">
import IconMoon from"~icons/material-symbols/dark-mode";
import IconGear from"~icons/material-symbols/settings";
import IconSun from"~icons/material-symbols/light-mode";
import IconPanelOpen from"~icons/material-symbols/left-panel-open";
import IconPanelClose from"~icons/material-symbols/left-panel-close";
import { ref } from"vue";
import type { Component } from"vue";

import type { PageKey } from"../../types";

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

const isExpanded = ref(true);
</script>

<template>
 <aside 
 class="hidden lg:flex sticky top-0 h-dvh min-w-0 flex-col gap-2 bg-secondary py-3 px-2.5" 
 :class="isExpanded ?'w-64':'w-16 items-center'"
 aria-label="App navigation"
 >
 <div class="group flex items-center w-full mb-3 h-10" :class="isExpanded ?'px-1.5 justify-between':'justify-center relative'">
 <a href="/" class="inline-flex items-center min-w-0 gap-3 text-text-muted hover:text-text leading-none no-underline" :class="!isExpanded ?'group-hover:opacity-0':''" aria-label="Never Stelle Home">
 <img src="/assets/logo.png" alt="" class="w-8 h-8 shrink-0" />
 <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden font-bold text-text text-xl" :class="isExpanded ?'opacity-100':'opacity-0'">Never Stelle</span>
 </a>
 
 <button 
 @click="isExpanded = !isExpanded" 
 class="flex items-center justify-center text-text-muted hover:text-text"
 :class="isExpanded ?'w-8 h-8 rounded-md hover:bg-secondary-muted shrink-0':'absolute inset-0 w-full h-full opacity-0 group-hover:opacity-100 bg-secondary'"
 :aria-label="isExpanded ?'Collapse sidebar':'Expand sidebar'"
 >
 <IconPanelClose v-if="isExpanded" class="w-5 h-5" />
 <IconPanelOpen v-else class="w-5 h-5" />
 </button>
 </div>

 <nav class="flex gap-1" :class="isExpanded ?'w-full flex-col':'flex-col items-center w-full'" aria-label="App navigation">
 <button
 v-for="item in pageItems"
 :key="item.key"
 type="button"
 class="inline-flex items-center rounded-lg bg-transparent text-text-muted leading-none active:scale-95 hover:text-text hover:bg-secondary-muted aria-pressed:bg-primary aria-pressed:text-text-on-primary"
 :class="isExpanded ?'w-full flex-row justify-start px-3.5 h-10 gap-3':'w-10 h-10 justify-center'"
 :aria-pressed="activePage === item.key"
 :title="!isExpanded ? item.label : undefined"
 @click="emit('selectPage', item.key)"
 >
 <component :is="item.icon" class="shrink-0 w-5 h-5" aria-hidden="true" />
 <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden font-medium">{{ item.label }}</span>
 </button>
 </nav>

 <div class="flex gap-1 mt-auto" :class="isExpanded ?'w-full flex-col':'flex-col items-center w-full'">
 <button
 type="button"
 class="inline-flex items-center rounded-lg bg-transparent text-text-muted leading-none active:scale-95 hover:text-text hover:bg-secondary-muted"
 :class="isExpanded ?'w-full flex-row justify-start px-3.5 h-10 gap-3':'w-10 h-10 justify-center'"
 :aria-label="isLightMode ?'Switch to dark mode':'Switch to light mode'"
 :title="!isExpanded ? (isLightMode ?'Switch to dark mode':'Switch to light mode') : undefined"
 @click="emit('toggleTheme')"
 >
 <IconSun v-if="isLightMode" class="shrink-0 w-5 h-5" aria-hidden="true" />
 <IconMoon v-else class="shrink-0 w-5 h-5" aria-hidden="true" />
 <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden font-medium">{{ isLightMode ?'Dark Mode':'Light Mode'}}</span>
 </button>
 <button 
 type="button" 
 class="inline-flex items-center rounded-lg bg-transparent text-text-muted leading-none active:scale-95 hover:text-text hover:bg-secondary-muted aria-pressed:bg-primary aria-pressed:text-text-on-primary"
 :class="[isExpanded ?'w-full flex-row justify-start px-3.5 h-10 gap-3':'w-10 h-10 justify-center', activePage ==='settings'?'bg-primary text-text-on-primary':'']"
 aria-label="Open settings" 
 :title="!isExpanded ?'Settings': undefined" 
 @click="emit('openSettings', $event)"
 >
 <IconGear class="shrink-0 w-5 h-5" aria-hidden="true" />
 <span v-show="isExpanded" class="whitespace-nowrap overflow-hidden font-medium">Settings</span>
 </button>
 </div>
 </aside>
</template>
