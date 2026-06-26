<script setup lang="ts">
import type { Component } from "vue";

defineProps<{
  countCards: Array<{ label: string; value: number; icon: Component }>;
}>();
</script>

<template>
  <footer class="z-40 border-t border-accent-subtle bg-panel py-2 px-4 flex items-center justify-between text-xs text-text-muted mb-[4.2rem] lg:mb-0" aria-label="Task counts">
    <div class="flex items-center gap-6 overflow-x-auto scrollbar-hide w-full max-w-full">
      <div v-for="item in countCards" :key="item.label" class="flex items-center gap-2 whitespace-nowrap">
        <component :is="item.icon" class="w-[1.1rem] h-[1.1rem]" aria-hidden="true" />
        <span class="font-bold tracking-wider uppercase text-[0.65rem]">{{ item.label }}:</span>
        <strong class="text-text text-sm">{{ item.value }}</strong>
      </div>
    </div>
  </footer>
</template>

<style scoped>
.scrollbar-hide::-webkit-scrollbar {
    display: none;
}
.scrollbar-hide {
    -ms-overflow-style: none;
    scrollbar-width: none;
}
</style>
