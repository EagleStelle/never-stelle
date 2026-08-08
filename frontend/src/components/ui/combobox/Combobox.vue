<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { computed, nextTick, onMounted, ref, watch } from "vue"
import { ComboboxRoot } from "reka-ui"
import IconCheck from "~icons/material-symbols/check"
import { Label } from "@/components/ui/label"
import ComboboxAnchor from "@/components/ui/combobox/ComboboxAnchor.vue"
import ComboboxEmpty from "@/components/ui/combobox/ComboboxEmpty.vue"
import ComboboxGroup from "@/components/ui/combobox/ComboboxGroup.vue"
import ComboboxInput from "@/components/ui/combobox/ComboboxInput.vue"
import ComboboxItem from "@/components/ui/combobox/ComboboxItem.vue"
import ComboboxItemIndicator from "@/components/ui/combobox/ComboboxItemIndicator.vue"
import ComboboxList from "@/components/ui/combobox/ComboboxList.vue"
import ComboboxSelect from "@/components/ui/combobox/ComboboxSelect.vue"
import ComboboxTrigger from "@/components/ui/combobox/ComboboxTrigger.vue"
import ComboboxViewport from "@/components/ui/combobox/ComboboxViewport.vue"
import type { ComboboxItemOption } from "@/components/ui/combobox/types"

defineOptions({
  inheritAttrs: false,
})

const props = withDefaults(
  defineProps<{
    modelValue: string
    items: ComboboxItemOption[]
    placeholder?: string
    emptyText?: string
    class?: HTMLAttributes["class"]
    layout?: "fit" | "fill"
    /** Heading shown above the search field in the mobile sheet. */
    label?: string
  }>(),
  { layout: "fit" },
)

const emit = defineEmits<{
  "update:modelValue": [value: string]
}>()

const open = ref(false)

const activeItem = computed(
  () => props.items.find((item) => item.key === props.modelValue) || null,
)

const activeLabel = computed(() => activeItem.value?.label || "")

const searchTerm = ref("")

const syncSearchTerm = () => {
  searchTerm.value = activeLabel.value
}

watch(() => props.modelValue, syncSearchTerm, { immediate: true })

onMounted(() => {
  // Ensure reka-ui doesn't overwrite our search term on mount.
  nextTick(syncSearchTerm)
})

const isComboboxItemOption = (value: unknown): value is ComboboxItemOption =>
  typeof value === "object" && value !== null && "key" in value && "label" in value

const displayValue = (value: unknown) =>
  isComboboxItemOption(value) ? value.label : ""

const handleModelValue = (value: unknown) => {
  if (isComboboxItemOption(value)) {
    emit("update:modelValue", value.key)
  }
}

const handleOpenChange = (isOpen: boolean) => {
  open.value = isOpen
  if (!isOpen) {
    syncSearchTerm()
  }
}
</script>

<template>
  <div
    :class="[
      props.layout === 'fill' ? 'w-full flex' : 'inline-flex',
      'flex-col items-start gap-1 shrink-0',
    ]"
  >
    <Label v-if="props.label" class="select-none font-medium">
      {{ props.label }}
    </Label>

    <ComboboxRoot
      data-slot="combobox"
      by="label"
      v-model:search-term="searchTerm"
      :model-value="activeItem ?? undefined"
      :display-value="displayValue"
      :open="open"
      @update:open="handleOpenChange"
      @update:model-value="handleModelValue"
      :class="[props.class, props.layout === 'fill' ? 'w-full' : 'inline-block']"
    >
      <ComboboxAnchor as-child>
        <ComboboxTrigger as-child>
          <ComboboxSelect
            v-bind="$attrs"
            :item="activeItem"
            :items="items"
            :placeholder="placeholder"
            :layout="props.layout"
          />
        </ComboboxTrigger>
      </ComboboxAnchor>

      <ComboboxList :open="open" :label="label">
        <ComboboxInput :placeholder="props.placeholder || 'Search...'" />
        <ComboboxViewport>
          <ComboboxEmpty>
            {{ props.emptyText || "No items found." }}
          </ComboboxEmpty>

          <ComboboxGroup class="p-0">
            <ComboboxItem
              v-for="item in items"
              :key="item.key"
              :value="item"
              :disabled="item.disabled"
            >
              <ComboboxItemIndicator
                class="absolute left-2 ml-0 flex h-4 w-4 items-center justify-center"
              >
                <IconCheck class="w-4 h-4" aria-hidden="true" />
              </ComboboxItemIndicator>

              <div class="flex items-center gap-2">
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
                <span>{{ item.label }}</span>
              </div>
            </ComboboxItem>
          </ComboboxGroup>
        </ComboboxViewport>
      </ComboboxList>
    </ComboboxRoot>
  </div>
</template>
