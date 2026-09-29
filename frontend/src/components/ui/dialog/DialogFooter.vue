<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "@/lib/utils";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <div
    data-slot="dialog-footer"
    :class="
      cn(
        'flex shrink-0 flex-col-reverse gap-2 border-t border-(--glass-border) bg-primary/45 px-5 py-4 backdrop-blur-md sm:flex-row sm:items-center sm:justify-end sm:px-6',
        props.class,
      )
    "
  >
    <slot />
  </div>
</template>
