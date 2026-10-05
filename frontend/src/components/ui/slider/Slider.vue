<script setup lang="ts">
import type { SliderRootEmits, SliderRootProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { computed } from "vue"
import { reactiveOmit } from "@vueuse/core"
import { SliderRange, SliderRoot, SliderThumb, SliderTrack, useForwardPropsEmits } from "reka-ui"
import { cn } from "@/lib/utils"

const props = defineProps<
  SliderRootProps & {
    class?: HTMLAttributes["class"]
    // The thumb's name and spoken value, for a slider whose value is a step index.
    label?: string
    valueText?: string
    // A mark at every step.
    ticks?: boolean
  }
>()
const emits = defineEmits<SliderRootEmits>()

const delegatedProps = reactiveOmit(props, "class", "label", "valueText", "ticks")

const forwarded = useForwardPropsEmits(delegatedProps, emits)

// Thumb width in px (size-5); reka keeps the thumb inside the track, so a step's center moves in by it.
const THUMB = 20

const steps = computed(() => {
  if (!props.ticks) return []
  const min = props.min ?? 0
  const max = props.max ?? 100
  const step = props.step ?? 1
  return Array.from({ length: Math.floor((max - min) / step) + 1 }, (_, index) => {
    const percent = ((index * step) / (max - min)) * 100
    return { value: min + index * step, left: `calc(${percent}% + ${THUMB / 2 - (percent * THUMB) / 100}px)` }
  })
})
</script>

<template>
  <SliderRoot
    v-slot="{ modelValue }"
    data-slot="slider"
    :class="cn(
      'relative flex w-full cursor-pointer touch-none items-center select-none data-disabled:cursor-default data-disabled:opacity-50 data-[orientation=vertical]:h-full data-[orientation=vertical]:min-h-44 data-[orientation=vertical]:w-auto data-[orientation=vertical]:flex-col',
      props.class,
    )"
    v-bind="forwarded"
  >
    <SliderTrack
      data-slot="slider-track"
      class="relative grow overflow-hidden rounded-full border border-(--glass-border) bg-black/20 shadow-inner in-[.light-mode]:bg-white/40 data-[orientation=horizontal]:h-2 data-[orientation=horizontal]:w-full data-[orientation=vertical]:h-full data-[orientation=vertical]:w-2"
    >
      <SliderRange
        data-slot="slider-range"
        class="bg-accent absolute data-[orientation=horizontal]:h-full data-[orientation=vertical]:w-full"
      />
      <span
        v-for="mark in steps"
        :key="mark.value"
        aria-hidden="true"
        :style="{ left: mark.left }"
        :class="cn(
          'pointer-events-none absolute top-1/2 size-1 -translate-1/2 rounded-full',
          mark.value <= (modelValue?.[0] ?? 0)
            ? 'bg-black/35'
            : 'bg-white/30 in-[.light-mode]:bg-black/25',
        )"
      />
    </SliderTrack>

    <SliderThumb
      v-for="(_, key) in modelValue"
      :key="key"
      data-slot="slider-thumb"
      :aria-label="props.label"
      :aria-valuetext="props.valueText"
      class="group/thumb relative block size-5 shrink-0 cursor-grab rounded-full border-2 border-accent bg-white shadow-[0_1px_3px_rgb(0_0_0/0.35)] transition-[box-shadow,scale] duration-200 ease-glass before:absolute before:-inset-1.5 before:content-[''] hover:ring-4 hover:ring-accent/30 focus-visible:ring-4 focus-visible:ring-accent/50 focus-visible:outline-hidden active:scale-110 active:cursor-grabbing disabled:pointer-events-none disabled:opacity-50 motion-reduce:transition-none"
    >
      <!-- The value as a person reads it, shown while the thumb is pointed at, held or focused. -->
      <span
        v-if="props.valueText"
        aria-hidden="true"
        class="glass-chrome pointer-events-none absolute bottom-full left-1/2 mb-2.5 -translate-x-1/2 translate-y-1 whitespace-nowrap rounded-lg border border-(--glass-border) px-2 py-0.5 text-xs font-medium tabular-nums text-foreground opacity-0 shadow-xl transition-[opacity,translate] duration-200 ease-glass group-hover/thumb:translate-y-0 group-hover/thumb:opacity-100 group-focus/thumb:translate-y-0 group-focus/thumb:opacity-100 motion-reduce:transition-none"
      >
        {{ props.valueText }}
      </span>
    </SliderThumb>
  </SliderRoot>
</template>
