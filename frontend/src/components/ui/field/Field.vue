<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { computed } from "vue"
import type { FieldVariants } from "@/components/ui/field"
import { cn } from "@/lib/utils"
import { fieldVariants } from "@/components/ui/field"

const props = defineProps<{
  class?: HTMLAttributes["class"]
  labelWidth?: FieldVariants["labelWidth"]
}>()

const labelWidth = computed(() => props.labelWidth ?? "md")
</script>

<template>
  <div
    role="group"
    data-slot="field"
    :class="cn(
      fieldVariants({ labelWidth }),
      props.class,
    )"
  >
    <slot />
  </div>
</template>
