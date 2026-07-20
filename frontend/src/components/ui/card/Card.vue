<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "@/lib/utils";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <div
    data-slot="card"
    :class="
      cn(
        'glass text-card-foreground flex flex-col gap-6 rounded-lg py-6',
        props.class,
      )
    "
  >
    <slot />
  </div>
</template>
