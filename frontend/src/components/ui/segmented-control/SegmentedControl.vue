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
  xs: "h-8 text-xs",
  sm: "h-9 text-sm",
  default: "h-10 text-sm",
  lg: "h-11 text-base",
  xl: "h-12 text-lg",
};

const sizeClass = computed(() => SIZE_CLASS[props.size]);

const forwardedProps = computed(() => {
  const { class: _, size: __, ...rest } = props;
  return rest;
});
</script>

<template>
  <ToggleGroupRoot
    v-bind="forwardedProps"
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
