<script setup lang="ts">
import { computed } from "vue";

import ActionButton from "@/components/task/ActionButton.vue";
import { Checkbox } from "@/components/ui/checkbox";
import { useDashboard } from "@/composables/useDashboard";
import type { SelectAll } from "@/composables/useSelection";
import type { ItemAction } from "@/types";

// What a footer shows while items are selected: the count and what can be done to them.
const props = defineProps<{ selection: SelectAll; actions: ItemAction[] }>();

const { selectAllPlace } = useDashboard();

// A destructive action alone in the bar shows filled.
const shown = computed(() => {
  const [only] = props.actions;
  if (props.actions.length !== 1 || only.variant !== "destructive-ghost") return props.actions;
  return [{ ...only, variant: "destructive" as const }];
});
</script>

<template>
  <Checkbox
    v-if="selectAllPlace === 'bar'"
    :checked="selection.state"
    aria-label="Select all"
    @update:checked="selection.setAll"
  />
  <span class="text-sm whitespace-nowrap" aria-live="polite">{{ selection.count }} selected</span>
  <ActionButton v-for="action in shown" :key="action.key" :action="action" size="sm" compact />
</template>
