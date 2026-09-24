<script setup lang="ts">
import type { PrimitiveProps } from "reka-ui";
import type { HTMLAttributes, VNode } from "vue";
import type { ButtonVariants } from "@/components/ui/button";
import { Comment, Fragment, Text, computed, useSlots } from "vue";
import { Primitive } from "reka-ui";
import { cn } from "@/lib/utils";
import { buttonVariants } from "@/components/ui/button";

interface Props extends PrimitiveProps {
  variant?: ButtonVariants["variant"];
  size?: ButtonVariants["size"];
  /** Shows only the icon on phones; the label stays for screen readers. */
  compact?: boolean;
  class?: HTMLAttributes["class"];
}

// A `v-if`ed label still leaves a slot function behind, so the label's presence has to
// be read off what the slot renders. Otherwise a hidden label keeps the button oblong.
function hasContent(nodes?: VNode[]): boolean {
  return (nodes || []).some((node) => {
    if (node.type === Comment) return false;
    if (node.type === Fragment) return hasContent(node.children as VNode[]);
    if (node.type === Text) return String(node.children || "").trim() !== "";
    return true;
  });
}

const props = withDefaults(defineProps<Props>(), {
  as: "button",
  compact: false,
});
const slots = useSlots();
const iconOnly = computed(() => !!slots.icon && !hasContent(slots.default?.()));
const compactLabel = computed(() => props.compact && !!slots.icon && !iconOnly.value);
const resolvedSize = computed<ButtonVariants["size"]>(() => {
  if (!iconOnly.value) return props.size || "default";
  if (props.size === "sm") return "icon-sm";
  return props.size || "icon";
});
</script>

<template>
  <Primitive
    data-slot="button"
    :data-variant="variant"
    :data-size="resolvedSize"
    :as="as"
    :as-child="asChild"
    :class="
      cn(
        buttonVariants({ variant, size: resolvedSize, compact: compactLabel }),
        props.class,
      )
    "
  >
    <slot name="icon" />
    <span v-if="compactLabel" class="max-sm:sr-only"><slot /></span>
    <slot v-else />
  </Primitive>
</template>
