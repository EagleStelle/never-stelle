<script setup lang="ts">
import type { PrimitiveProps } from "reka-ui";
import type { HTMLAttributes } from "vue";
import type { ButtonVariants } from ".";
import { computed, useSlots } from "vue";
import { Primitive } from "reka-ui";
import { cn } from "@/lib/utils";
import { buttonVariants } from ".";

interface Props extends PrimitiveProps {
  variant?: ButtonVariants["variant"];
  size?: ButtonVariants["size"];
  class?: HTMLAttributes["class"];
}

const props = withDefaults(defineProps<Props>(), {
  as: "button",
});
const slots = useSlots();
const iconOnly = computed(() => !!slots.icon && !slots.default);
const resolvedSize = computed<ButtonVariants["size"]>(() =>
  props.size || (iconOnly.value ? "icon" : "default"),
);
</script>

<template>
  <Primitive
    data-slot="button"
    :data-variant="variant"
    :data-size="resolvedSize"
    :as="as"
    :as-child="asChild"
    :class="cn(buttonVariants({ variant, size: resolvedSize }), props.class)"
  >
    <slot name="icon" />
    <slot />
  </Primitive>
</template>
