<script setup lang="ts" generic="ModelValue extends string | string[]">
import type { HTMLAttributes } from "vue"
import { computed, ref, useAttrs, useId } from "vue"
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
import ComboboxItemIcon from "@/components/ui/combobox/ComboboxItemIcon.vue"
import type { ComboboxItemOption } from "@/components/ui/combobox/types"

defineOptions({
  inheritAttrs: false,
})

const props = withDefaults(
  defineProps<{
    /** One key, or every picked key when `multiple`. */
    modelValue: ModelValue
    items: ComboboxItemOption[]
    /** Keeps the list open and toggles each picked key in the model. */
    multiple?: boolean
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
  "update:modelValue": [value: ModelValue]
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

// Any option with an icon reserves the icon slot on every row.
const hasIcons = computed(() => props.items.some((item) => item.icon || item.iconUrl))

const selectedItems = computed(() => {
  const keys = new Set<string>([props.modelValue].flat())
  return props.items.filter((item) => keys.has(item.key))
})

// The trigger face: the selected item, or every selected label joined.
const activeItem = computed<ComboboxItemOption | null>(() => {
  const [first, ...rest] = selectedItems.value
  if (!first || !rest.length) return first ?? null
  return {
    ...first,
    key: selectedItems.value.map((item) => item.key).join(","),
    label: selectedItems.value.map((item) => item.label).join(", "),
  }
})

const isComboboxItemOption = (value: unknown): value is ComboboxItemOption =>
  typeof value === "object" && value !== null && "key" in value && "label" in value

const handleModelValue = (value: unknown) => {
  if (props.multiple && Array.isArray(value)) {
    emit("update:modelValue", value.filter(isComboboxItemOption).map((item) => item.key) as ModelValue)
  } else if (isComboboxItemOption(value)) {
    emit("update:modelValue", value.key as ModelValue)
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
        v-model:open="open"
        :multiple="props.multiple"
        :model-value="props.multiple ? selectedItems : selectedItems[0]"
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
              :has-icons="hasIcons"
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
                  <ComboboxItemIcon v-if="hasIcons" :item="item" />
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
