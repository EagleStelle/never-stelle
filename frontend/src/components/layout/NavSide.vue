<script setup lang="ts">
import { ref } from "vue";
import IconPanelOpen from "~icons/material-symbols/left-panel-open";
import IconPanelClose from "~icons/material-symbols/left-panel-close";
import type { Component } from "vue";

import type { PageKey, SettingsSection } from "@/types";
import AccountMenu from "@/components/layout/AccountMenu.vue";
import {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarFooter,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
} from "@/components/ui/sidebar";

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

const isExpanded = ref(true);
</script>

<template>
  <Sidebar
    class="hidden lg:flex sticky top-0 h-dvh min-w-0 glass-border-right py-3 px-2.5 transition-all duration-300"
    :class="isExpanded ? 'w-64' : 'w-16 items-center'"
    aria-label="App navigation"
  >
    <SidebarHeader
      class="w-full mb-3 h-10 flex-row items-center relative group/header"
      :class="isExpanded ? 'px-1.5 justify-between' : 'justify-center'"
    >
      <a
        href="/"
        class="inline-flex items-center min-w-0 gap-3 text-white in-[.light-mode]:text-black hover:text-white in-[.light-mode]:hover:text-black leading-none no-underline transition-opacity duration-300"
        :class="!isExpanded ? 'group-hover/header:opacity-0' : ''"
        aria-label="Never Stelle Home"
      >
        <img src="/assets/logo.png" alt="" class="w-8 h-8 shrink-0" />
        <span
          v-show="isExpanded"
          class="whitespace-nowrap overflow-hidden font-display font-bold text-white in-[.light-mode]:text-black text-xl tracking-tight"
          >Never Stelle</span
        >
      </a>

      <button
        @click="isExpanded = !isExpanded"
        class="flex items-center justify-center text-white/65 in-[.light-mode]:text-black/65 hover:text-white in-[.light-mode]:hover:text-black hover:bg-white/10 in-[.light-mode]:hover:bg-black/5 transition-all duration-300 ease-glass"
        :class="
          isExpanded
            ? 'w-8 h-8 rounded-lg shrink-0'
            : 'absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-10 h-10 rounded-lg opacity-0 group-hover/header:opacity-100'
        "
        :aria-label="isExpanded ? 'Collapse sidebar' : 'Expand sidebar'"
      >
        <IconPanelClose v-if="isExpanded" class="w-5 h-5" />
        <IconPanelOpen v-else class="w-5 h-5" />
      </button>
    </SidebarHeader>

    <SidebarContent
      :class="isExpanded ? 'w-full' : 'items-center w-full'"
      aria-label="App navigation"
    >
      <SidebarMenu>
        <SidebarMenuItem v-for="item in pageItems" :key="item.key">
          <SidebarMenuButton
            @click="emit('selectPage', item.key)"
            :aria-pressed="activePage === item.key"
            :title="!isExpanded ? item.label : undefined"
            :class="!isExpanded ? 'w-10 px-0 justify-center' : ''"
          >
            <component
              :is="item.icon"
              class="shrink-0 w-5 h-5"
              aria-hidden="true"
            />
            <span
              v-show="isExpanded"
              class="whitespace-nowrap overflow-hidden font-medium"
              >{{ item.label }}</span
            >
          </SidebarMenuButton>
        </SidebarMenuItem>
      </SidebarMenu>
    </SidebarContent>

    <SidebarFooter
      :class="isExpanded ? 'w-full' : 'items-center w-full'"
    >
      <AccountMenu
        :is-light-mode="isLightMode"
        :settings-open="settingsOpen"
        :variant="isExpanded ? 'sidebar' : 'sidebar-collapsed'"
        @open-settings="(event, section) => emit('openSettings', event, section)"
        @toggle-theme="emit('toggleTheme')"
      />
    </SidebarFooter>
  </Sidebar>
</template>
