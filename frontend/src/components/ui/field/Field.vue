<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import type { FieldVariants } from "@/components/ui/field"
import { cn } from "@/lib/utils"
import { fieldVariants } from "@/components/ui/field"

const props = defineProps<{
  class?: HTMLAttributes["class"]
  orientation?: FieldVariants["orientation"]
}>()
</script>

<template>
  <div
    role="group"
    data-slot="field"
    :data-orientation="orientation"
    :class="cn(
      fieldVariants({ orientation }),
      props.class,
    )"
  >
    <slot />
  </div>
</template>
