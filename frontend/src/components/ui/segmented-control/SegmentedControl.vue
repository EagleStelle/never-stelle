<script setup lang="ts">
import type { ToggleGroupRootProps } from "reka-ui";
import { reactiveOmit } from "@vueuse/core";
import { ToggleGroupRoot, useForwardProps } from "reka-ui";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

const props = withDefaults(
  defineProps<ToggleGroupRootProps & { class?: string; label?: string }>(),
  {
    type: "single",
  },
);

const emit = defineEmits<{
  "update:modelValue": [value: any];
}>();

const delegatedProps = reactiveOmit(props, "class", "label");
const forwarded = useForwardProps(delegatedProps);
</script>

<template>
  <div class="inline-flex flex-col items-start gap-1 shrink-0">
    <Label v-if="props.label" class="select-none font-medium">
      {{ props.label }}
    </Label>
    <ToggleGroupRoot
      v-slot="slotProps"
      data-slot="toggle-group"
      v-bind="forwarded"
      @update:model-value="emit('update:modelValue', $event)"
      :class="
        cn(
          'inline-flex h-9 w-fit items-center gap-1 rounded-xl border border-(--glass-border) bg-black/25 px-1 py-1 text-sm shadow-inner backdrop-blur-md in-[.light-mode]:bg-white/40',
          props.class,
        )
      "
    >
      <slot v-bind="slotProps" />
    </ToggleGroupRoot>
  </div>
</template>
