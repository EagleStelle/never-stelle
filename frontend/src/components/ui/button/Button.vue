<script setup lang="ts">
import { computed, useSlots } from "vue";
import { Primitive } from "reka-ui";

type Size = "xs" | "sm" | "default" | "lg" | "xl";

const props = withDefaults(
  defineProps<{
    variant?: string;
    size?: Size;
    as?: string;
    asChild?: boolean;
    class?: string;
  }>(),
  { size: "default", as: "button", asChild: false },
);

const BASE =
  "group inline-flex items-center justify-center gap-1.5 border border-[var(--glass-border)] bg-accent text-black font-sans font-medium leading-none whitespace-nowrap transition-all duration-300 ease-glass hover:bg-accent/45 active:scale-[0.96] disabled:cursor-not-allowed";

const SIZE_CLASS: Record<Size, { base: string; pad: string; square: string }> =
  {
    xs: { base: "h-7 text-xs rounded-lg", pad: "px-2.5", square: "w-7" },
    sm: { base: "h-8 text-xs rounded-lg", pad: "px-3", square: "w-8" },
    default: { base: "h-9 text-sm rounded-lg", pad: "px-4", square: "w-9" },
    lg: { base: "h-10 text-sm rounded-lg", pad: "px-5", square: "w-10" },
    xl: { base: "h-11 text-base rounded-lg", pad: "px-6", square: "w-11" },
  };

const slots = useSlots();
const iconOnly = computed(() => !!slots.icon && !slots.default);

const classes = computed(() => {
  const size = SIZE_CLASS[props.size];
  return [
    BASE,
    size.base,
    iconOnly.value ? size.square : size.pad,
    props.class,
  ];
});
</script>

<template>
  <Primitive :as="as" :as-child="asChild" :class="classes">
    <slot name="icon" />
    <slot />
  </Primitive>
</template>
