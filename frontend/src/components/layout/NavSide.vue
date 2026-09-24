<script setup lang="ts">
import { ref } from "vue";
import IconPanelOpen from "~icons/material-symbols/left-panel-open";
import IconPanelClose from "~icons/material-symbols/left-panel-close";

import AccountMenu from "@/components/layout/AccountMenu.vue";
import { useDashboard } from "@/composables/useDashboard";
import {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarFooter,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
} from "@/components/ui/sidebar";

const { activePage, pageItems, setActivePage } = useDashboard();

const isExpanded = ref(true);
</script>

<template>
  <Sidebar
    class="hidden lg:flex sticky top-0 h-dvh min-w-0 glass-border-right py-3 px-3 transition-[width] duration-300 ease-glass motion-reduce:transition-none"
    :class="isExpanded ? 'w-64' : 'w-[calc(4rem+1px)]'"
    aria-label="App navigation"
  >
    <SidebarHeader
      class="w-full mb-3 h-10 px-1 flex-row items-center relative group/header"
    >
      <a
        href="/"
        class="inline-flex items-center min-w-0 gap-3 text-white in-[.light-mode]:text-black hover:text-white in-[.light-mode]:hover:text-black leading-none no-underline transition-opacity duration-300"
        :class="
          !isExpanded
            ? 'group-hover/header:opacity-0 group-has-[button:focus-visible]/header:opacity-0'
            : ''
        "
        aria-label="Never Stelle Home"
      >
        <img src="/assets/logo.png" alt="" class="w-8 h-8 shrink-0" />
        <span
          class="whitespace-nowrap overflow-hidden font-sans font-bold text-white in-[.light-mode]:text-black text-xl tracking-tight transition-opacity duration-300 ease-glass"
          :class="isExpanded ? 'opacity-100' : 'opacity-0'"
          >Never Stelle</span
        >
      </a>

      <button
        @click="isExpanded = !isExpanded"
        class="absolute right-0 top-0 w-10 h-10 rounded-lg flex items-center justify-center text-white/65 in-[.light-mode]:text-black/65 hover:text-white in-[.light-mode]:hover:text-black hover:bg-white/10 in-[.light-mode]:hover:bg-black/5 transition-all duration-300 ease-glass"
        :class="
          !isExpanded
            ? 'opacity-0 group-hover/header:opacity-100 focus-visible:opacity-100'
            : ''
        "
        :aria-label="isExpanded ? 'Collapse sidebar' : 'Expand sidebar'"
      >
        <IconPanelClose v-if="isExpanded" class="w-5 h-5" />
        <IconPanelOpen v-else class="w-5 h-5" />
      </button>
    </SidebarHeader>

    <SidebarContent class="w-full" aria-label="App navigation">
      <SidebarMenu>
        <SidebarMenuItem v-for="item in pageItems" :key="item.key">
          <SidebarMenuButton
            @click="setActivePage(item.key)"
            :aria-pressed="activePage === item.key"
            :title="!isExpanded ? item.label : undefined"
            class="px-2.5!"
          >
            <component
              :is="item.icon"
              class="shrink-0 w-5 h-5"
              aria-hidden="true"
            />
            <span
              class="whitespace-nowrap overflow-hidden font-medium transition-opacity duration-300 ease-glass"
              :class="isExpanded ? 'opacity-100' : 'opacity-0'"
              >{{ item.label }}</span
            >
          </SidebarMenuButton>
        </SidebarMenuItem>
      </SidebarMenu>
    </SidebarContent>

    <SidebarFooter class="w-full">
      <AccountMenu :variant="isExpanded ? 'sidebar' : 'sidebar-collapsed'" />
    </SidebarFooter>
  </Sidebar>
</template>
