<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { useAttrs } from "vue";
import { useVModel } from "@vueuse/core";
import { cn } from "@/lib/utils";
import { cva, type VariantProps } from "class-variance-authority";

const inputVariants = cva(
  "flex items-center min-w-0 rounded-lg bg-black/20 in-[.light-mode]:bg-white/40 backdrop-blur-md border border-(--glass-border) shadow-inner focus-within:ring-2 focus-within:ring-accent transition-all duration-300 ease-glass",
  {
    variants: {
      size: {
        xs: "h-8 p-0.5 gap-1",
        sm: "h-9 p-1 gap-1",
        default: "h-10 p-1 gap-1",
        lg: "h-11 p-1 gap-1.5",
        xl: "h-12 p-1.5 gap-1.5",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

const iconVariants = cva(
  "flex items-center justify-center shrink-0 text-white in-[.light-mode]:text-black",
  {
    variants: {
      size: {
        xs: "w-7 h-7 ml-1",
        sm: "w-7 h-7 ml-1",
        default: "w-8 h-8 ml-1.5",
        lg: "w-9 h-9 ml-1.5",
        xl: "w-10 h-10 ml-2",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

const gapVariants = cva("", {
  variants: {
    size: {
      xs: "w-2",
      sm: "w-2.5",
      default: "w-3",
      lg: "w-4",
      xl: "w-4",
    },
  },
  defaultVariants: {
    size: "default",
  },
});

const textVariants = cva(
  "flex-1 min-w-0 h-full bg-transparent outline-none text-white in-[.light-mode]:text-black pr-2 placeholder:text-white/50 in-[.light-mode]:placeholder:text-black/50",
  {
    variants: {
      size: {
        xs: "text-xs",
        sm: "text-sm",
        default: "text-sm",
        lg: "text-base",
        xl: "text-lg",
      },
    },
    defaultVariants: {
      size: "default",
    },
  },
);

type InputVariants = VariantProps<typeof inputVariants>;

const props = withDefaults(
  defineProps<{
    defaultValue?: string | number;
    modelValue?: string | number | null;
    class?: HTMLAttributes["class"];
    inputClass?: HTMLAttributes["class"];
    size?: InputVariants["size"];
  }>(),
  { size: "default" },
);

const emits = defineEmits<{
  (e: "update:modelValue", payload: string | number): void;
}>();

const modelValue = useVModel(props, "modelValue", emits, {
  passive: true,
  defaultValue: props.defaultValue,
});

defineOptions({
  inheritAttrs: false,
});

const attrs = useAttrs();
</script>

<template>
  <div :class="cn(inputVariants({ size }), props.class, attrs.class as string)">
    <div v-if="$slots.icon" :class="iconVariants({ size })">
      <slot name="icon" />
    </div>
    <div v-else :class="gapVariants({ size })" />

    <input
      v-bind="{ ...attrs, class: undefined }"
      v-model="modelValue"
      @click="
        ($event as MouseEvent).detail === 3 &&
          ($event.target as HTMLInputElement).select()
      "
      :class="cn(textVariants({ size }), props.inputClass)"
    />

    <slot name="action" />
  </div>
</template>
