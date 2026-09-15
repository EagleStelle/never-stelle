<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { computed } from "vue"
import { ChevronsUpDownIcon } from "@lucide/vue"
import ComboboxItemIcon from "@/components/ui/combobox/ComboboxItemIcon.vue"
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
    /** Reserves the icon slot on the face and sizers. */
    hasIcons?: boolean
  }>(),
  { layout: "fit" },
)

const placeholderText = computed(() => props.placeholder || "Search...")

const activeLabel = computed(() => props.item?.label || "")

interface SizerItem {
  key: string
  label: string
  icon?: any
  iconUrl?: string
}

const sizerItems = computed<SizerItem[]>(() => {
  const list: SizerItem[] = []
  const existingKeys = new Set<string>()

  if (props.items) {
    for (const item of props.items) {
      list.push({
        key: item.key,
        label: item.label,
        icon: item.icon,
        iconUrl: item.iconUrl,
      })
      existingKeys.add(item.key)
    }
  }

  if (props.item && !existingKeys.has(props.item.key)) {
    list.push({
      key: props.item.key,
      label: props.item.label,
      icon: props.item.icon,
      iconUrl: props.item.iconUrl,
    })
  }

  // Include placeholder text as a candidate sizer item
  list.push({
    key: "__placeholder__",
    label: placeholderText.value,
  })

  return list
})
</script>

<template>
  <button
    type="button"
    v-bind="$attrs"
    :class="[
      'h-9 items-center gap-2 overflow-hidden rounded-lg bg-black/20 in-[.light-mode]:bg-white/40 border border-(--glass-border) px-3 py-2 text-sm font-medium text-white in-[.light-mode]:text-black shadow-inner transition-all duration-300 ease-glass focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent disabled:pointer-events-none disabled:opacity-50',
      props.layout === 'fill'
        ? 'grid w-full grid-cols-[minmax(0,1fr)_auto]'
        : 'inline-grid max-w-[calc(100vw-2rem)] grid-cols-[minmax(0,max-content)_auto]',
      props.class,
    ]"
  >
    <!-- Invisible sizers for every item so button width always equals the longest item -->
    <div
      v-for="sizer in sizerItems"
      :key="sizer.key"
      aria-hidden="true"
      class="pointer-events-none invisible col-start-1 row-start-1 min-w-0 max-w-[min(28rem,calc(100vw-5rem))] flex items-center gap-2 text-left whitespace-nowrap"
    >
      <ComboboxItemIcon v-if="props.hasIcons" :item="sizer" />
      <span>{{ sizer.label }}</span>
    </div>

    <!-- Active selected item face -->
    <div
      :class="[
        'col-start-1 row-start-1 min-w-0 max-w-[min(28rem,calc(100vw-5rem))] flex items-center gap-2 text-left',
        !activeLabel && 'opacity-55',
      ]"
    >
      <template v-if="item">
        <ComboboxItemIcon v-if="props.hasIcons" :item="item" />
      </template>
      <span class="truncate">{{ activeLabel || placeholderText }}</span>
    </div>
    <ChevronsUpDownIcon
      class="col-start-2 row-start-1 size-4 shrink-0 opacity-50"
      aria-hidden="true"
    />
  </button>
</template>
