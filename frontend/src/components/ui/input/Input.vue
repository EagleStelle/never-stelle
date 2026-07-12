<script setup lang="ts">
import { computed, useAttrs } from "vue";

type Size = "xs" | "sm" | "default" | "lg" | "xl";

const props = withDefaults(
  defineProps<{
    modelValue?: string | number | null;
    inputClass?: string;
    size?: Size;
  }>(),
  { size: "default" },
);

const emit = defineEmits<{
  "update:modelValue": [value: string];
}>();

defineOptions({
  inheritAttrs: false,
});

const attrs = useAttrs();

const SIZE_CLASS: Record<
  Size,
  { wrapper: string; input: string; icon: string; gap: string }
> = {
  xs: {
    wrapper: "h-7 p-0.5 gap-0.5",
    input: "text-xs",
    icon: "w-6 h-6 ml-1",
    gap: "w-2",
  },
  sm: {
    wrapper: "h-8 p-0.5 gap-1",
    input: "text-xs",
    icon: "w-7 h-7 ml-1",
    gap: "w-2",
  },
  default: {
    wrapper: "h-9 p-1 gap-1",
    input: "text-sm",
    icon: "w-7 h-7 ml-1",
    gap: "w-2.5",
  },
  lg: {
    wrapper: "h-10 p-1 gap-1",
    input: "text-sm",
    icon: "w-8 h-8 ml-1.5",
    gap: "w-3",
  },
  xl: {
    wrapper: "h-11 p-1 gap-1.5",
    input: "text-base",
    icon: "w-9 h-9 ml-1.5",
    gap: "w-4",
  },
};

const sizes = computed(() => SIZE_CLASS[props.size]);
</script>

<template>
  <div
    :class="[
      'flex items-center min-w-0 rounded-lg bg-black/20 in-[.light-mode]:bg-white/40 backdrop-blur-md border border-(--glass-border) shadow-inner focus-within:ring-2 focus-within:ring-accent transition-all duration-300 ease-glass',
      sizes.wrapper,
      attrs.class,
    ]"
  >
    <div
      v-if="$slots.icon"
      :class="[
        'flex items-center justify-center shrink-0 text-white in-[.light-mode]:text-black',
        sizes.icon,
      ]"
    >
      <slot name="icon" />
    </div>
    <div v-else :class="sizes.gap" />

    <input
      v-bind="{ ...attrs, class: undefined }"
      :value="modelValue"
      @input="
        emit('update:modelValue', ($event.target as HTMLInputElement).value)
      "
      @click="
        ($event as MouseEvent).detail === 3 &&
          ($event.target as HTMLInputElement).select()
      "
      :class="[
        'flex-1 min-w-0 h-full bg-transparent outline-none text-white in-[.light-mode]:text-black pr-2 placeholder:text-white/50 in-[.light-mode]:placeholder:text-black/50',
        sizes.input,
        props.inputClass,
      ]"
    />

    <slot name="action" />
  </div>
</template>
