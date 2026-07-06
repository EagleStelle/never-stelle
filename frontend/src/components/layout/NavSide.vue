<script setup lang="ts">
import IconMoon from "~icons/material-symbols/dark-mode";
import IconGear from "~icons/material-symbols/settings";
import IconSun from "~icons/material-symbols/light-mode";
import IconPanelOpen from "~icons/material-symbols/left-panel-open";
import IconPanelClose from "~icons/material-symbols/left-panel-close";
import { ref } from "vue";
import type { Component } from "vue";

import type { PageKey } from "../../types";
import {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarFooter,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
} from "../ui/sidebar";

defineProps<{
  activePage: PageKey;
  isLightMode: boolean;
  pageItems: Array<{ key: PageKey; label: string; icon: Component }>;
  settingsOpen: boolean;
}>();

const emit = defineEmits<{
  openSettings: [event: Event];
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
      class="w-full mb-3 h-10 flex-row items-center"
      :class="isExpanded ? 'px-1.5 justify-between' : 'justify-center'"
    >
      <a
        v-if="isExpanded"
        href="/"
        class="inline-flex items-center min-w-0 gap-3 text-white [.light-mode_&]:text-black hover:text-white [.light-mode_&]:hover:text-black leading-none no-underline"
        aria-label="Never Stelle Home"
      >
        <img src="/assets/logo.png" alt="" class="w-8 h-8 shrink-0" />
        <span
          class="whitespace-nowrap overflow-hidden font-display font-bold text-white [.light-mode_&]:text-black text-xl tracking-tight"
          >Never Stelle</span
        >
      </a>

      <button
        @click="isExpanded = !isExpanded"
        class="flex items-center justify-center text-white [.light-mode_&]:text-black hover:text-white [.light-mode_&]:hover:text-black transition-all duration-300 ease-glass"
        :class="
          isExpanded
            ? 'w-8 h-8 rounded-lg bg-transparent shrink-0'
            : 'w-10 h-10 rounded-lg bg-transparent'
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
      <SidebarMenu>
        <SidebarMenuItem>
          <SidebarMenuButton
            @click="emit('toggleTheme')"
            :aria-label="isLightMode ? 'Switch to dark mode' : 'Switch to light mode'"
            :title="!isExpanded ? (isLightMode ? 'Switch to dark mode' : 'Switch to light mode') : undefined"
            :class="!isExpanded ? 'w-10 px-0 justify-center' : ''"
          >
            <IconSun
              v-if="isLightMode"
              class="shrink-0 w-5 h-5"
              aria-hidden="true"
            />
            <IconMoon v-else class="shrink-0 w-5 h-5" aria-hidden="true" />
            <span
              v-show="isExpanded"
              class="whitespace-nowrap overflow-hidden font-medium"
              >{{ isLightMode ? "Dark Mode" : "Light Mode" }}</span
            >
          </SidebarMenuButton>
        </SidebarMenuItem>

        <SidebarMenuItem>
          <SidebarMenuButton
            @click="emit('openSettings', $event)"
            :aria-pressed="settingsOpen"
            aria-label="Open settings"
            :title="!isExpanded ? 'Settings' : undefined"
            :class="!isExpanded ? 'w-10 px-0 justify-center' : ''"
          >
            <IconGear class="shrink-0 w-5 h-5" aria-hidden="true" />
            <span
              v-show="isExpanded"
              class="whitespace-nowrap overflow-hidden font-medium"
              >Settings</span
            >
          </SidebarMenuButton>
        </SidebarMenuItem>
      </SidebarMenu>
    </SidebarFooter>
  </Sidebar>
</template>
