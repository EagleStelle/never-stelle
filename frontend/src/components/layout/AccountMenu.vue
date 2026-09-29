<script setup lang="ts">
import { ref } from "vue";
import IconChevronUp from "~icons/material-symbols/keyboard-arrow-up";

import ActionMenuItems from "@/components/task/ActionMenuItems.vue";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useAccount } from "@/composables/useAccount";
import { PAGE_ICONS } from "@/ui";

// The sidebar's account menu; phones open the account as a page instead.
const props = defineProps<{ collapsed: boolean }>();

const { actions, role, username } = useAccount();
const open = ref(false);
</script>

<template>
  <DropdownMenu v-model:open="open">
    <DropdownMenuTrigger as-child>
      <button
        type="button"
        class="group/account flex h-10 w-full min-w-0 items-center gap-3 overflow-hidden rounded-lg bg-transparent px-2.5 text-left text-white/80 transition-all duration-300 ease-glass hover:bg-white/10 hover:text-white active:scale-[0.98] in-[.light-mode]:text-black/80 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black"
        :title="props.collapsed ? username : undefined"
      >
        <component :is="PAGE_ICONS.account" class="shrink-0 h-5 w-5" aria-hidden="true" />
        <span
          class="min-w-0 flex-1 transition-opacity duration-300 ease-glass"
          :class="props.collapsed ? 'opacity-0' : 'opacity-100'"
        >
          <span class="block truncate text-sm font-semibold leading-tight">{{ username }}</span>
          <span class="block truncate text-xs leading-tight text-white/45 in-[.light-mode]:text-black/45">
            {{ role }}
          </span>
        </span>
        <IconChevronUp
          class="h-5 w-5 shrink-0 text-white/50 transition duration-300 ease-glass in-[.light-mode]:text-black/50"
          :class="[props.collapsed ? 'opacity-0' : 'opacity-100', { 'rotate-180': open }]"
          aria-hidden="true"
        />
      </button>
    </DropdownMenuTrigger>

    <!-- Above the trigger, or beside it while the sidebar is collapsed. -->
    <DropdownMenuContent
      :side="props.collapsed ? 'right' : 'top'"
      :align="props.collapsed ? 'end' : 'start'"
      :side-offset="8"
      :class="props.collapsed ? 'w-60' : 'w-(--reka-dropdown-menu-trigger-width)'"
    >
      <DropdownMenuLabel class="truncate text-white/60 in-[.light-mode]:text-black/60">
        {{ username }}
      </DropdownMenuLabel>
      <ActionMenuItems :actions="actions" />
    </DropdownMenuContent>
  </DropdownMenu>
</template>
