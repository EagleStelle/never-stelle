<script setup lang="ts">
import { computed } from "vue";
import { ToggleGroupRoot, type ToggleGroupRootProps } from "reka-ui";

type Size = "xs" | "sm" | "default" | "lg" | "xl";

const props = withDefaults(
  defineProps<ToggleGroupRootProps & { class?: string; size?: Size }>(),
  {
    type: "single",
    size: "default",
  },
);

const emit = defineEmits<{
  "update:modelValue": [value: any];
}>();

const SIZE_CLASS: Record<Size, string> = {
  xs: "h-7 text-xs",
  sm: "h-8 text-xs",
  default: "h-9 text-sm",
  lg: "h-10 text-sm",
  xl: "h-11 text-base",
};

const sizeClass = computed(() => SIZE_CLASS[props.size]);
</script>

<template>
  <ToggleGroupRoot
    v-bind="props"
    @update:modelValue="emit('update:modelValue', $event)"
    :class="[
      'inline-flex items-center gap-1 px-1 py-1 bg-black/20 in-[.light-mode]:bg-white/40 backdrop-blur-md border border-(--glass-border) shadow-inner rounded-lg',
      sizeClass,
      props.class,
    ]"
  >
    <slot />
  </ToggleGroupRoot>
</template>
