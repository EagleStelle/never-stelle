<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { computed, useAttrs, useSlots } from "vue";
import { useVModel } from "@vueuse/core";
import { cn } from "@/lib/utils";

const CONTAINER =
  "flex items-center min-w-0 h-9 p-1 gap-1 rounded-lg bg-black/20 in-[.light-mode]:bg-white/40 border border-(--glass-border) shadow-inner focus-within:ring-2 focus-within:ring-accent transition-all duration-300 ease-glass";
const ICON =
  "flex items-center justify-center shrink-0 w-7 h-7 ml-1 text-white in-[.light-mode]:text-black";
const GAP = "w-3";
const TEXT =
  "flex-1 min-w-0 h-full bg-transparent outline-none text-base md:text-sm text-white in-[.light-mode]:text-black pr-2 placeholder:text-white/50 in-[.light-mode]:placeholder:text-black/50";
const INPUT =
  "h-9 w-full min-w-0 rounded-lg border border-(--glass-border) bg-black/20 px-3 py-1 text-base text-white shadow-inner outline-none transition-all duration-300 ease-glass placeholder:text-white/50 focus-visible:ring-2 focus-visible:ring-accent disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 in-[.light-mode]:bg-white/40 in-[.light-mode]:text-black in-[.light-mode]:placeholder:text-black/50 md:text-sm";

const props = defineProps<{
  defaultValue?: string | number;
  modelValue?: string | number | null;
  class?: HTMLAttributes["class"];
  inputClass?: HTMLAttributes["class"];
}>();

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
const slots = useSlots();
const hasChrome = computed(() => !!slots.icon || !!slots.action);
</script>

<template>
  <div v-if="hasChrome" :class="cn(CONTAINER, props.class, attrs.class as string)">
    <div v-if="$slots.icon" :class="ICON">
      <slot name="icon" />
    </div>
    <div v-else :class="GAP" />

    <input
      v-bind="{ ...attrs, class: undefined }"
      v-model="modelValue"
      @click="
        ($event as MouseEvent).detail === 3 &&
          ($event.target as HTMLInputElement).select()
      "
      :class="cn(TEXT, props.inputClass)"
    />

    <slot name="action" />
  </div>
  <input
    v-else
    v-bind="{ ...attrs, class: undefined }"
    v-model="modelValue"
    data-slot="input"
    @click="
      ($event as MouseEvent).detail === 3 &&
        ($event.target as HTMLInputElement).select()
    "
    :class="cn(INPUT, props.class, attrs.class as string, props.inputClass)"
  />
</template>
