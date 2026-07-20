<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "@/lib/utils";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <tr
    data-slot="table-row"
    :class="
      cn(
        'border-b border-(--glass-border) transition-colors duration-200 ease-glass hover:bg-(--glass-hover) data-[state=selected]:bg-(--glass-hover)',
        props.class,
      )
    "
  >
    <slot />
  </tr>
</template>
