<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "@/lib/utils";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <div data-slot="card-title" :class="cn('min-w-0 font-semibold wrap-anywhere font-sans leading-snug tracking-tight', props.class)">
    <slot />
  </div>
</template>
