<script setup lang="ts">
import type { ComboboxItemEmits, ComboboxItemProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { reactiveOmit } from "@vueuse/core"
import { ComboboxItem, useForwardPropsEmits } from "reka-ui"
import { cn } from "@/lib/utils"
import { useIsMobile } from "@/composables/useIsMobile"

const props = defineProps<ComboboxItemProps & { class?: HTMLAttributes["class"] }>()
const emits = defineEmits<ComboboxItemEmits>()

const delegatedProps = reactiveOmit(props, "class")

const forwarded = useForwardPropsEmits(delegatedProps, emits)

const isMobile = useIsMobile()
</script>

<template>
  <ComboboxItem
    data-slot="combobox-item"
    v-bind="forwarded"
    :class="cn(
      'glass-option relative flex w-full cursor-pointer select-none items-center rounded-lg text-sm outline-none text-white in-[.light-mode]:text-black transition-all duration-300 ease-glass hover:bg-accent/20 hover:text-white data-[highlighted]:bg-accent/20 data-[highlighted]:text-white! data-[disabled]:pointer-events-none data-[disabled]:cursor-not-allowed data-[disabled]:opacity-40 in-[.light-mode]:hover:text-black in-[.light-mode]:data-[highlighted]:text-black!',
      isMobile ? 'py-2.5 pl-8 pr-3 active:scale-99' : 'py-2 pl-8 pr-2',
      props.class,
    )"
  >
    <slot />
  </ComboboxItem>
</template>
