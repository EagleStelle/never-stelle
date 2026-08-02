<script setup lang="ts">
import type { ComboboxContentEmits, ComboboxContentProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { computed } from "vue"
import { reactiveOmit } from "@vueuse/core"
import { ComboboxContent, ComboboxPortal, useForwardPropsEmits } from "reka-ui"
import { cn } from "@/lib/utils"
import { useIsMobile } from "@/composables/useIsMobile"
import { useKeyboardInset } from "@/composables/useKeyboardInset"

defineOptions({
  inheritAttrs: false,
})

const props = withDefaults(
  defineProps<ComboboxContentProps & {
    class?: HTMLAttributes["class"]
    /** Drives the sheet backdrop, which lives outside the content's own presence. */
    open?: boolean
    /** Heading shown above the search field in the sheet. */
    label?: string
  }>(),
  {
    position: "popper",
    align: "start",
    sideOffset: 6,
    open: false,
  },
)
const emits = defineEmits<ComboboxContentEmits>()

const delegatedProps = reactiveOmit(props, "class", "open", "label", "position", "bodyLock")
const forwarded = useForwardPropsEmits(delegatedProps, emits)

const isMobile = useIsMobile()

const { keyboardOpen, viewportTop, viewportHeight, viewportBottom } = useKeyboardInset(
  () => props.open && isMobile.value,
)

const SHEET_MARGIN = 8

const sheetStyle = computed(() => {
  const height = viewportHeight.value
  if (keyboardOpen.value && height) {
    const sheetHeight = Math.max(0, height - SHEET_MARGIN * 2)
    return {
      top: `${Math.round(Math.max(viewportTop.value + SHEET_MARGIN, viewportBottom.value - sheetHeight - SHEET_MARGIN))}px`,
      bottom: "auto",
      height: `${Math.round(sheetHeight)}px`,
      maxHeight: `${Math.round(sheetHeight)}px`,
    }
  }

  return {
    top: "auto",
    bottom: "0px",
    height: "auto",
    maxHeight: height ? `${Math.round(Math.max(200, height * 0.6))}px` : "60dvh",
  }
})
</script>

<template>
  <ComboboxPortal>
    <template v-if="isMobile">
      <Transition
        enter-active-class="transition-opacity duration-200 ease-glass"
        enter-from-class="opacity-0"
        leave-active-class="transition-opacity duration-150 ease-glass"
        leave-to-class="opacity-0"
      >
        <div
          v-if="open"
          data-slot="combobox-list-overlay"
          aria-hidden="true"
          class="fixed inset-0 z-90 bg-black/50 backdrop-blur-sm"
        />
      </Transition>

      <ComboboxContent
        data-slot="combobox-list"
        v-bind="{ ...$attrs, ...forwarded }"
        position="inline"
        :body-lock="props.bodyLock ?? true"
        :style="sheetStyle"
        :class="cn('glass-chrome glass-border-top fixed inset-x-0 z-100 flex flex-col overflow-hidden rounded-t-xl text-white outline-none in-[.light-mode]:text-black ease-glass motion-reduce:animate-none data-[state=open]:animate-in data-[state=open]:slide-in-from-bottom data-[state=open]:duration-260 data-[state=closed]:animate-out data-[state=closed]:slide-out-to-bottom data-[state=closed]:duration-200', props.class)"
      >
        <div class="shrink-0 pb-1 pt-1.5" aria-hidden="true">
          <div class="mx-auto h-0.75 w-8 rounded-full bg-white/20 in-[.light-mode]:bg-black/15" />
        </div>
        <p
          v-if="label"
          class="shrink-0 px-3 pb-1.5 text-center text-xs font-medium text-white/50 in-[.light-mode]:text-black/50"
        >
          {{ label }}
        </p>

        <slot />
      </ComboboxContent>
    </template>

    <ComboboxContent
      v-else
      data-slot="combobox-list"
      v-bind="{ ...$attrs, ...forwarded }"
      :position="props.position"
      :body-lock="props.bodyLock ?? false"
      :style="{
        width: 'var(--reka-combobox-trigger-width)',
        minWidth: 'var(--reka-combobox-trigger-width)',
      }"
      :class="cn('z-100 min-w-(--reka-combobox-trigger-width) glass-chrome text-white in-[.light-mode]:text-black origin-(--reka-combobox-content-transform-origin) rounded-lg border border-(--glass-border) overflow-hidden shadow-none outline-none data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2', props.class)"
    >
      <slot />
    </ComboboxContent>
  </ComboboxPortal>
</template>
