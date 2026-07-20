<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "@/lib/utils";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <caption
    data-slot="table-caption"
    :class="
      cn('mt-4 text-sm text-white/70 in-[.light-mode]:text-black/70', props.class)
    "
  >
    <slot />
  </caption>
</template>
