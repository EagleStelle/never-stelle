<script setup lang="ts">
import type { ToggleGroupRootProps } from "reka-ui";
import { reactiveOmit } from "@vueuse/core";
import { useId } from "vue";
import { ToggleGroupRoot, useForwardProps } from "reka-ui";
import type { FieldVariants } from "@/components/ui/field";
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";
import { cn } from "@/lib/utils";

defineOptions({
  inheritAttrs: false,
});

const props = withDefaults(
  defineProps<
    ToggleGroupRootProps & {
      class?: string;
      /** Names the control: rendered as the visible label and read as the group's name. */
      label?: string;
      /** Above the control (toolbars) or beside it in a label column (settings rows). */
      labelPlacement?: "top" | "start";
      /** Width of the label column; only applies when the label sits beside the control. */
      labelWidth?: FieldVariants["labelWidth"];
    }
  >(),
  {
    type: "single",
    labelPlacement: "top",
  },
);

const emit = defineEmits<{
  "update:modelValue": [value: any];
}>();

// The group is a div rather than a labelable element, so it borrows the label by id.
const labelId = useId();

const delegatedProps = reactiveOmit(
  props,
  "class",
  "label",
  "labelPlacement",
  "labelWidth",
);
const forwarded = useForwardProps(delegatedProps);
</script>

<template>
  <Field
    :label-width="props.labelWidth"
    :class="props.labelPlacement === 'start' ? 'w-full' : 'w-fit shrink-0'"
  >
    <FieldLabel v-if="props.label" :id="labelId" as="span">
      {{ props.label }}
    </FieldLabel>

    <FieldContent>
      <ToggleGroupRoot
        v-slot="slotProps"
        data-slot="toggle-group"
        v-bind="{ ...$attrs, ...forwarded }"
        :aria-labelledby="props.label ? labelId : undefined"
        @update:model-value="emit('update:modelValue', $event)"
        :class="
          cn(
            'inline-flex h-9 w-fit items-center gap-1 rounded-lg border border-(--glass-border) bg-black/20 p-1 text-sm shadow-inner transition-all duration-300 ease-glass in-[.light-mode]:bg-white/40',
            props.class,
          )
        "
      >
        <slot v-bind="slotProps" />
      </ToggleGroupRoot>
    </FieldContent>
  </Field>
</template>
