<script setup lang="ts">
import { computed } from "vue";

import { Button } from "@/components/ui/button";
import type { ItemAction } from "@/types";
import { syncIconClass } from "@/ui";

// One action as a button in its own variant; a link action downloads. Size and layout come from the caller.
const props = defineProps<{ action: ItemAction; iconOnly?: boolean; disabled?: boolean }>();

const disabled = computed(() => props.disabled || props.action.disabled);
// A disabled link renders as a plain button.
const href = computed(() => (disabled.value ? undefined : props.action.href));
</script>

<template>
  <Button
    :as="href ? 'a' : 'button'"
    :type="href ? undefined : 'button'"
    :href="href"
    :download="href ? '' : undefined"
    :variant="props.action.variant"
    :title="props.action.title || props.action.label"
    :aria-label="props.iconOnly ? props.action.label : undefined"
    :disabled="disabled"
    @click="props.action.run?.()"
  >
    <template #icon>
      <component
        :is="props.action.icon"
        aria-hidden="true"
        :class="props.action.spinning !== undefined && syncIconClass(props.action.spinning)"
      />
    </template>
    <template v-if="!props.iconOnly">{{ props.action.label }}</template>
  </Button>
</template>
