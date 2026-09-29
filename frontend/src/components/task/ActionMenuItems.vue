<script setup lang="ts">
import { DropdownMenuItem, DropdownMenuSeparator } from "@/components/ui/dropdown-menu";
import type { ItemAction } from "@/types";
import { isDestructive, opensDestructiveGroup } from "@/utils/task";

// A menu's items, destructive ones set apart below a divider.
defineProps<{ actions: ItemAction[] }>();
</script>

<template>
  <template v-for="(action, index) in actions" :key="action.key">
    <DropdownMenuSeparator v-if="opensDestructiveGroup(actions, index)" />
    <DropdownMenuItem
      :variant="isDestructive(action) ? 'destructive' : 'default'"
      :title="action.title"
      :disabled="action.disabled"
      @select="action.run?.()"
    >
      <component :is="action.icon" aria-hidden="true" :class="{ 'animate-sync': action.spinning }" />
      {{ action.label }}
    </DropdownMenuItem>
  </template>
</template>
