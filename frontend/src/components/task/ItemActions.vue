<script setup lang="ts">
import { computed } from "vue";

import { Button } from "@/components/ui/button";
import ActionButton from "@/components/task/ActionButton.vue";
import ActionMenuItems from "@/components/task/ActionMenuItems.vue";
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import type { ItemAction } from "@/types";
import { ACTION_ICONS } from "@/ui";

// An item's actions: the first shows as a button, the rest wait in its menu unless only one is left.
const props = defineProps<{ actions: ItemAction[]; disabled?: boolean }>();

const buttons = computed(() => (props.actions.length <= 2 ? props.actions : props.actions.slice(0, 1)));
const menu = computed(() => props.actions.slice(buttons.value.length));
</script>

<template>
  <div class="flex shrink-0 items-center gap-1.5">
    <ActionButton
      v-for="action in buttons"
      :key="action.key"
      :action="action"
      :disabled="props.disabled"
      size="sm"
      icon-only
    />

    <DropdownMenu v-if="menu.length">
      <DropdownMenuTrigger as-child :disabled="props.disabled">
        <Button type="button" variant="ghost" size="sm" title="More actions" aria-label="More actions">
          <template #icon>
            <component :is="ACTION_ICONS.more" aria-hidden="true" />
          </template>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <ActionMenuItems :actions="menu" />
      </DropdownMenuContent>
    </DropdownMenu>
  </div>
</template>
