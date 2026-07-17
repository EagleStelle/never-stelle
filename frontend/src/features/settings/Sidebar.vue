<script setup lang="ts">
import { computed, ref } from "vue";
import IconClose from "~icons/material-symbols/close";
import IconSave from "~icons/material-symbols/save";
import IconSearch from "~icons/material-symbols/search";
import IconUndo from "~icons/material-symbols/undo";
import { TabsList, TabsTrigger } from "reka-ui";

import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "../../components/ui/sidebar";
import type { SettingsSection } from "../../types";
import { SETTINGS_SECTION_DEFS, SETTINGS_SECTION_GROUPS } from "./sections";

defineProps<{ hasUnsavedChanges: boolean }>();

const emit = defineEmits<{
  save: [];
  clear: [];
  close: [];
  select: [section: SettingsSection];
}>();

const search = ref("");

// Group the registry by label; when a search filters everything out, fall back to
// the full list so the sidebar is never empty.
const groups = computed(() => {
  const term = search.value.trim().toLowerCase();
  const build = (filter: boolean) =>
    SETTINGS_SECTION_GROUPS.map((label) => ({
      label,
      items: SETTINGS_SECTION_DEFS.filter(
        (def) => def.group === label && (!filter || def.label.toLowerCase().includes(term)),
      ),
    })).filter((group) => group.items.length);
  if (!term) return build(false);
  const filtered = build(true);
  return filtered.length ? filtered : build(false);
});
</script>

<template>
  <Sidebar class="relative p-3 sm:h-full sm:w-60 sm:p-4 gap-3 flex flex-col">
    <!-- Mobile Header -->
    <div class="flex sm:hidden items-center justify-between px-1">
      <div class="flex items-center gap-2">
        <span class="text-lg font-bold ml-1">Settings</span>
        <template v-if="hasUnsavedChanges">
          <Button
            variant="soft"
            @click="emit('clear')"
            title="Clear changes"
            aria-label="Clear changes"
          >
            <template #icon>
              <IconUndo class="w-4 h-4" aria-hidden="true" />
            </template>
          </Button>
          <Button
            variant="primary"
            @click="emit('save')"
            title="Save changes"
            aria-label="Save changes"
          >
            <template #icon>
              <IconSave class="w-4 h-4" aria-hidden="true" />
            </template>
          </Button>
        </template>
      </div>
      <button
        type="button"
        @click="emit('close')"
        class="inline-flex h-9 w-9 items-center justify-center rounded-lg bg-transparent text-white/70 in-[.light-mode]:text-black/70 hover:text-white in-[.light-mode]:hover:text-black active:scale-[0.96] transition-all"
        aria-label="Close"
      >
        <IconClose class="h-6 w-6" aria-hidden="true" />
      </button>
    </div>

    <SidebarHeader class="hidden sm:flex">
      <Input
        v-model="search"
        type="text"
        placeholder="Search"
        aria-label="Search settings"
        class="hidden sm:flex"
      >
        <template #icon>
          <IconSearch class="h-5 w-5" aria-hidden="true" />
        </template>
      </Input>
    </SidebarHeader>

    <SidebarContent>
      <TabsList
        class="flex gap-1 overflow-x-auto pb-1 sm:flex-col sm:overflow-x-hidden sm:overflow-y-auto sm:pb-0"
        aria-label="Settings sections"
      >
        <SidebarGroup
          v-for="group in groups"
          :key="group.label"
          class="flex-row sm:flex-col w-auto sm:w-full gap-1"
        >
          <SidebarGroupLabel class="hidden sm:block sm:not-first:mt-3">
            {{ group.label }}
          </SidebarGroupLabel>
          <SidebarMenu class="flex-row sm:flex-col gap-1 w-auto sm:w-full">
            <SidebarMenuItem
              v-for="item in group.items"
              :key="item.key"
              class="flex-none sm:w-full"
            >
              <TabsTrigger
                as-child
                :value="item.key"
                @click="emit('select', item.key)"
              >
                <SidebarMenuButton
                  as="div"
                  class="cursor-pointer w-auto sm:w-full justify-center sm:justify-start px-3.5 sm:px-3.5 h-8 sm:h-10"
                >
                  <component
                    :is="item.icon"
                    class="hidden sm:block h-5 w-5 shrink-0 opacity-80 group-data-[state=active]:opacity-100"
                    aria-hidden="true"
                  />
                  <span class="whitespace-nowrap font-medium text-sm">{{
                    item.label
                  }}</span>
                </SidebarMenuButton>
              </TabsTrigger>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroup>
      </TabsList>
    </SidebarContent>
  </Sidebar>
</template>
