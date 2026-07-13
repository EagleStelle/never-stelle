<script setup lang="ts">
import { computed } from "vue";
import { CheckboxRoot, CheckboxIndicator, type CheckboxRootProps } from "reka-ui";
import IconCheck from "~icons/material-symbols/check";

type CheckedValue = boolean | "indeterminate";

const props = defineProps<CheckboxRootProps & { class?: string; checked?: CheckedValue }>();
const emit = defineEmits<{
  "update:checked": [value: boolean];
  "update:modelValue": [value: CheckedValue];
}>();

const forwardedProps = computed(() => {
  const { class: _, checked: __, modelValue: ___, ...rest } = props;
  return rest;
});

const modelValue = computed<CheckedValue>(() => {
  const value = props.checked ?? props.modelValue;
  return value === "indeterminate" ? "indeterminate" : Boolean(value);
});

function update(value: CheckedValue): void {
  emit("update:modelValue", value);
  emit("update:checked", value === true);
}
</script>

<template>
  <CheckboxRoot
    v-bind="forwardedProps"
    :model-value="modelValue"
    @update:model-value="update"
    :class="[
      'peer h-5 w-5 shrink-0 rounded border border-(--glass-border) bg-black/20 in-[.light-mode]:bg-white/40 backdrop-blur-sm transition-all duration-200 focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:bg-accent data-[state=checked]:border-accent text-white in-[.light-mode]:text-black',
      props.class,
    ]"
  >
    <CheckboxIndicator class="flex h-full w-full items-center justify-center">
      <slot>
        <IconCheck class="h-3.5 w-3.5 text-[#1B1931]" />
      </slot>
    </CheckboxIndicator>
  </CheckboxRoot>
</template>
