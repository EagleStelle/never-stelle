<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { cn } from "@/lib/utils"

const props = withDefaults(
  defineProps<{
    class?: HTMLAttributes["class"]
    /** `divider` labels a subgroup with a rule running to the edge. */
    variant?: "legend" | "label" | "divider"
    /** The element to render; a divider outside a fieldset renders as a plain block. */
    as?: string
  }>(),
  {
    variant: "legend",
    as: "legend",
  },
)
</script>

<template>
  <component
    :is="props.as"
    data-slot="field-legend"
    :data-variant="variant"
    :class="cn(
      'mb-1.5 font-medium',
      'data-[variant=legend]:text-base',
      'data-[variant=label]:text-sm',
      'data-[variant=divider]:flex data-[variant=divider]:w-full data-[variant=divider]:items-center data-[variant=divider]:gap-3 data-[variant=divider]:text-sm data-[variant=divider]:text-muted-foreground',
      props.class,
    )"
  >
    <slot />
    <template v-if="variant === 'divider'">
      <span aria-hidden="true" class="h-px min-w-0 flex-1 bg-(--glass-border)" />
      <!-- An action at the far end of the rule, such as an add button. -->
      <slot name="end" />
    </template>
  </component>
</template>
