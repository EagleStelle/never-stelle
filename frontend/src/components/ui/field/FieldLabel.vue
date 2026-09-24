<script setup lang="ts">
import type { HTMLAttributes } from "vue"
import { cn } from "@/lib/utils"
import { Label } from '@/components/ui/label'

const props = defineProps<{
  class?: HTMLAttributes["class"]
  /** Marks the field as required; the control itself carries `required` for assistive tech. */
  required?: boolean
}>()
</script>

<template>
  <Label
    data-slot="field-label"
    :class="cn(
      'group/field-label peer/field-label flex w-fit gap-2 leading-snug group-data-[disabled=true]/field:opacity-50',
      'has-[>[data-slot=field]]:w-full has-[>[data-slot=field]]:flex-col has-[>[data-slot=field]]:rounded-md has-[>[data-slot=field]]:border [&>*]:data-[slot=field]:p-4',
      'has-data-[state=checked]:bg-primary/5 has-data-[state=checked]:border-primary dark:has-data-[state=checked]:bg-primary/10',
      props.class,
    )"
  >
    <slot />
    <span v-if="props.required" class="-ml-1.5 text-accent-ink" aria-hidden="true">*</span>
  </Label>
</template>
