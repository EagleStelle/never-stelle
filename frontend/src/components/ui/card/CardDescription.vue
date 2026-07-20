<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "@/lib/utils";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <div
    data-slot="card-description"
    :class="
      cn('text-sm text-white/70 in-[.light-mode]:text-black/70', props.class)
    "
  >
    <slot />
  </div>
</template>
