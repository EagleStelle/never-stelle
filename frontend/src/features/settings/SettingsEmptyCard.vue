<script setup lang="ts">
import type { HTMLAttributes } from "vue";

import { Card } from "../../components/ui/card";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <Card :class="['px-6', props.class]">
    <p class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55">
      <slot />
    </p>
  </Card>
</template>
