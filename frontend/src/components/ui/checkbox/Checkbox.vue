<script setup lang="ts">
import { computed } from "vue";
import { CheckboxRoot, CheckboxIndicator, type CheckboxRootProps } from "reka-ui";
import { Check } from "@lucide/vue";
import { cn } from "@/lib/utils";

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
    :class="
      cn(
        'peer size-4 shrink-0 rounded-[4px] border border-(--glass-border) bg-black/20 text-black shadow-xs transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:border-accent data-[state=checked]:bg-accent in-[.light-mode]:bg-white/40',
        props.class,
      )
    "
  >
    <CheckboxIndicator
      data-slot="checkbox-indicator"
      class="grid h-full w-full place-content-center text-current transition-none"
    >
      <slot>
        <Check class="size-3.5" />
      </slot>
    </CheckboxIndicator>
  </CheckboxRoot>
</template>
