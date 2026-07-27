<script setup lang="ts">
import type { TooltipContentEmits, TooltipContentProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { reactiveOmit } from "@vueuse/core"
import { TooltipArrow, TooltipContent, TooltipPortal, useForwardPropsEmits } from "reka-ui"
import { cn } from "@/lib/utils"

defineOptions({
  inheritAttrs: false,
})

const props = withDefaults(
  defineProps<
    TooltipContentProps & {
      class?: HTMLAttributes["class"]
      to?: string | HTMLElement
    }
  >(),
  {
    sideOffset: 6,
    to: "body",
  },
)

const emits = defineEmits<TooltipContentEmits>()

const delegatedProps = reactiveOmit(props, "class", "to")
const forwarded = useForwardPropsEmits(delegatedProps, emits)
</script>

<template>
  <TooltipPortal :to="to">
    <TooltipContent
      data-slot="tooltip-content"
      v-bind="{ ...forwarded, ...$attrs }"
      :class="
        cn(
          'glass-chrome animate-in fade-in-0 zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95 data-[side=bottom]:slide-in-from-top-1.5 data-[side=left]:slide-in-from-right-1.5 data-[side=right]:slide-in-from-left-1.5 data-[side=top]:slide-in-from-bottom-1.5 z-[100] w-max max-w-xs rounded-lg border border-(--glass-border) px-3 py-1.5 text-xs font-medium leading-relaxed text-foreground text-pretty shadow-xl',
          props.class,
        )
      "
    >
      <slot />

      <TooltipArrow
        class="fill-(--glass-strong) stroke-(--glass-border)"
        :width="10"
        :height="5"
      />
    </TooltipContent>
  </TooltipPortal>
</template>

