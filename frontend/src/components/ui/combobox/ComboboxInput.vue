<script setup lang="ts">
import type { ComboboxInputEmits, ComboboxInputProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { SearchIcon } from "@lucide/vue"
import { reactiveOmit } from "@vueuse/core"
import { ComboboxInput, useForwardPropsEmits } from "reka-ui"
import { cn } from "@/lib/utils"
import { useIsMobile } from "@/composables/useBreakpoints"

defineOptions({
  inheritAttrs: false,
})

const props = defineProps<ComboboxInputProps & {
  class?: HTMLAttributes["class"]
  placeholder?: string
}>()

const emits = defineEmits<ComboboxInputEmits>()

const delegatedProps = reactiveOmit(props, "class", "placeholder")

const forwarded = useForwardPropsEmits(delegatedProps, emits)

// The sheet needs a taller search row, and 16px text keeps iOS Safari from
// zooming the page when the field takes focus.
const isMobile = useIsMobile()
</script>

<template>
  <div
    data-slot="combobox-input-wrapper"
    :class="cn('flex h-9 shrink-0 items-center gap-2 border-b border-(--glass-border) px-3', isMobile && 'h-10')"
  >
    <SearchIcon class="size-4 shrink-0 opacity-50" />
    <ComboboxInput
      data-slot="combobox-input"
      autocapitalize="off"
      autocorrect="off"
      spellcheck="false"
      enterkeyhint="done"
      v-bind="{ ...$attrs, ...forwarded }"
      :placeholder="props.placeholder || 'Search...'"
      :class="cn(
        'flex h-10 w-full rounded-md bg-transparent py-3 text-sm outline-hidden disabled:cursor-not-allowed disabled:opacity-50',
        'text-white in-[.light-mode]:text-black placeholder:text-white/50 in-[.light-mode]:placeholder:text-black/50',
        isMobile && 'text-base',
        props.class,
      )"
    >
      <slot />
    </ComboboxInput>
  </div>
</template>
