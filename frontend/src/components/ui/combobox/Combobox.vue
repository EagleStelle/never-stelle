<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { computed, nextTick, onMounted, ref, useAttrs, useId, watch } from "vue"
import { ComboboxRoot } from "reka-ui"
import IconCheck from "~icons/material-symbols/check"
import type { FieldVariants } from "@/components/ui/field"
import { Field, FieldContent, FieldLabel } from "@/components/ui/field"
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
    /**
     * Names the control: rendered as the visible label, read as the accessible name,
     * and shown as the heading above the search field in the mobile sheet.
     */
    label?: string
    /** Above the control (toolbars) or beside it in a label column (settings rows). */
    labelPlacement?: "top" | "start"
    /** Width of the label column; only applies when the label sits beside the control. */
    labelWidth?: FieldVariants["labelWidth"]
  }>(),
  { labelPlacement: "top" },
)

const emit = defineEmits<{
  "update:modelValue": [value: string]
}>()

const attrs = useAttrs()
const fallbackId = useId()

// The trigger is a button, so a plain `for` gives the label a real association.
const controlId = computed(() => (attrs.id as string) || fallbackId)

// A label beside the control shares its row, so the control takes the rest of it.
const layout = computed(
  () => props.layout ?? (props.labelPlacement === "start" ? "fill" : "fit"),
)

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
  <Field
    :label-width="props.labelWidth"
    :class="layout === 'fill' ? 'w-full' : 'w-fit shrink-0'"
  >
    <FieldLabel v-if="props.label" :for="controlId">
      {{ props.label }}
    </FieldLabel>

    <FieldContent>
      <ComboboxRoot
        data-slot="combobox"
        by="label"
        v-model:search-term="searchTerm"
        :model-value="activeItem ?? undefined"
        :display-value="displayValue"
        :open="open"
        @update:open="handleOpenChange"
        @update:model-value="handleModelValue"
        :class="[props.class, layout === 'fill' ? 'w-full' : 'inline-block']"
      >
        <ComboboxAnchor as-child>
          <ComboboxTrigger as-child>
            <ComboboxSelect
              v-bind="$attrs"
              :id="controlId"
              :item="activeItem"
              :items="items"
              :placeholder="placeholder"
              :layout="layout"
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
    </FieldContent>
  </Field>
</template>
