<script setup lang="ts">
import type { ComboboxViewportProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { reactiveOmit } from "@vueuse/core"
import { ComboboxViewport, useForwardProps } from "reka-ui"
import { cn } from "@/lib/utils"
import { useIsMobile } from "@/composables/useIsMobile"

const props = defineProps<ComboboxViewportProps & { class?: HTMLAttributes["class"] }>()

const delegatedProps = reactiveOmit(props, "class")

const forwarded = useForwardProps(delegatedProps)

const isMobile = useIsMobile()
</script>

<template>
  <ComboboxViewport
    data-slot="combobox-viewport"
    v-bind="forwarded"
    :class="cn(
      'relative scroll-py-1 overflow-x-hidden overflow-y-auto scrollbar-none p-1',
      isMobile
        ? 'min-h-0 flex-1 overscroll-contain pb-[max(0.25rem,env(safe-area-inset-bottom))]'
        : 'max-h-75',
      props.class,
    )"
  >
    <slot />
  </ComboboxViewport>
</template>
