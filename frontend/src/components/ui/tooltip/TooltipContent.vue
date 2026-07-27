<script setup lang="ts">
import type { TooltipContentEmits, TooltipContentProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { reactiveOmit } from "@vueuse/core"
import { TooltipArrow, TooltipContent, TooltipPortal, useForwardPropsEmits } from "reka-ui"
import { cn } from "@/lib/utils"

defineOptions({
  inheritAttrs: false,
})

const props = withDefaults(defineProps<TooltipContentProps & { class?: HTMLAttributes["class"] }>(), {
  sideOffset: 4,
})

const emits = defineEmits<TooltipContentEmits>()

const delegatedProps = reactiveOmit(props, "class")
const forwarded = useForwardPropsEmits(delegatedProps, emits)
</script>

<template>
  <TooltipPortal>
    <TooltipContent
      data-slot="tooltip-content"
      v-bind="{ ...forwarded, ...$attrs }"
      :class="
        cn(
          'glass-chrome animate-in fade-in-0 zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95 data-[side=bottom]:slide-in-from-top-1.5 data-[side=left]:slide-in-from-right-1.5 data-[side=right]:slide-in-from-left-1.5 data-[side=top]:slide-in-from-bottom-1.5 z-100 max-w-64 rounded-lg border border-[var(--glass-border)] px-3 py-1.5 text-xs font-medium text-foreground text-pretty shadow-xl',
          props.class,
        )
      "
    >
      <slot />

      <TooltipArrow as-child>
        <svg
          width="12"
          height="6"
          viewBox="0 0 12 6"
          class="z-50 overflow-visible block"
        >
          <!-- Closed background fill extending -2px into container to cover rectangle border line -->
          <path
            d="M -1 -2 L 13 -2 L 6 6 Z"
            fill="var(--glass-strong)"
          />
          <!-- Seamless outer V stroke matching rectangle border -->
          <path
            d="M 0 0 L 6 6 L 12 0"
            fill="none"
            stroke="var(--glass-border)"
            stroke-width="1"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
      </TooltipArrow>
    </TooltipContent>
  </TooltipPortal>
</template>
