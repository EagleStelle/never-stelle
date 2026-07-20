<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { useVModel } from "@vueuse/core"
import { cn } from "@/lib/utils"

const props = defineProps<{
  class?: HTMLAttributes["class"]
  defaultValue?: string | number
  modelValue?: string | number
}>()

const emits = defineEmits<{
  (e: "update:modelValue", payload: string | number): void
}>()

const modelValue = useVModel(props, "modelValue", emits, {
  passive: true,
  defaultValue: props.defaultValue,
})
</script>

<template>
  <textarea
    v-model="modelValue"
    data-slot="textarea"
    :class="cn('flex field-sizing-content min-h-16 w-full rounded-lg border border-(--glass-border) bg-black/20 px-3 py-2 text-base text-white shadow-inner outline-none transition-all duration-300 ease-glass placeholder:text-white/50 focus-visible:ring-2 focus-visible:ring-accent disabled:cursor-not-allowed disabled:opacity-50 in-[.light-mode]:bg-white/40 in-[.light-mode]:text-black in-[.light-mode]:placeholder:text-black/50 md:text-sm', props.class)"
  />
</template>
