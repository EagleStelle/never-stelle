<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { computed } from "vue"
import type { FieldVariants } from "@/components/ui/field"
import { cn } from "@/lib/utils"
import { fieldVariants } from "@/components/ui/field"

const props = defineProps<{
  class?: HTMLAttributes["class"]
  orientation?: FieldVariants["orientation"]
  labelWidth?: FieldVariants["labelWidth"]
}>()

// Only the row orientation lays out a label column, so only it takes a default width.
const labelWidth = computed(
  () => props.labelWidth ?? (props.orientation === "start" ? "md" : undefined),
)
</script>

<template>
  <div
    role="group"
    data-slot="field"
    :data-orientation="orientation"
    :class="cn(
      fieldVariants({ orientation, labelWidth }),
      props.class,
    )"
  >
    <slot />
  </div>
</template>
