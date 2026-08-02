<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { computed } from "vue"
import { ChevronsUpDownIcon } from "@lucide/vue"
import type { ComboboxItemOption } from "@/components/ui/combobox/types"

defineOptions({
  inheritAttrs: false,
})

const props = withDefaults(
  defineProps<{
    /** Selected option shown in the trigger face. */
    item?: ComboboxItemOption | null
    /** Every option, rendered invisibly so the button never resizes on selection. */
    items?: ComboboxItemOption[]
    placeholder?: string
    class?: HTMLAttributes["class"]
    layout?: "fit" | "fill"
  }>(),
  { layout: "fit" },
)

const placeholderText = computed(() => props.placeholder || "Search...")

const sizerLabels = computed(() => [
  ...(props.items ?? []).map((item) => item.label),
  placeholderText.value,
])

const hasItemVisual = computed(() =>
  (props.items ?? []).some((item) => item.icon || item.iconUrl || item.initials),
)

const activeLabel = computed(() => props.item?.label || "")
</script>

<template>
  <button
    type="button"
    v-bind="$attrs"
    :class="[
      'h-9 items-center gap-2 overflow-hidden rounded-lg bg-black/20 in-[.light-mode]:bg-white/40 border border-(--glass-border) px-3 py-2 text-sm font-medium text-white in-[.light-mode]:text-black shadow-inner transition-all duration-300 ease-glass focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent',
      props.layout === 'fill'
        ? 'grid w-full grid-cols-[minmax(0,1fr)_auto]'
        : 'inline-grid max-w-[calc(100vw-2rem)] grid-cols-[minmax(0,max-content)_auto]',
      props.class,
    ]"
  >
    <span
      v-for="(label, index) in sizerLabels"
      :key="index"
      aria-hidden="true"
      :class="[
        'pointer-events-none invisible col-start-1 row-start-1 block max-w-[min(28rem,calc(100vw-5rem))] overflow-hidden whitespace-nowrap',
        hasItemVisual && 'pr-4',
      ]"
    >
      {{ label }}
    </span>
    <div
      :class="[
        'col-start-1 row-start-1 min-w-0 max-w-[min(28rem,calc(100vw-5rem))] flex items-center gap-2 text-left',
        !activeLabel && 'opacity-55',
      ]"
    >
      <template v-if="item">
        <img
          v-if="item.iconUrl"
          :src="item.iconUrl"
          class="w-4 h-4 shrink-0 rounded-lg"
          alt=""
          aria-hidden="true"
        />
        <component
          :is="item.icon"
          v-else-if="item.icon"
          class="w-4 h-4 shrink-0"
          aria-hidden="true"
        />
        <span
          v-else-if="item.initials"
          class="inline-flex h-5 min-w-5 items-center justify-center rounded glass-soft px-1 text-[0.65rem] font-semibold"
        >
          {{ item.initials }}
        </span>
      </template>
      <span class="truncate">{{ activeLabel || placeholderText }}</span>
    </div>
    <ChevronsUpDownIcon
      class="col-start-2 row-start-1 size-4 shrink-0 opacity-50"
      aria-hidden="true"
    />
  </button>
</template>
