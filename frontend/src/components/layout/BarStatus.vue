<script setup lang="ts">
import type { Component } from "vue";
import IconTrash from "~icons/material-symbols/delete";

defineProps<{
  countCards: Array<{ label: string; value: number; icon: Component }>;
}>();

const emit = defineEmits<{
  clearQueue: [];
}>();
</script>

<template>
  <footer class="z-40 bg-secondary py-1.5 px-3 flex items-center justify-between text-text-muted mb-14 lg:mb-0 gap-3" aria-label="Task counts">
    <button
      type="button"
      class="inline-flex items-center justify-center gap-1.5 h-7 px-2.5 rounded bg-secondary-muted border border-primary-muted text-text-muted transition-all duration-200 hover:text-text hover:border-primary active:scale-95 shrink-0"
      title="Clear Queue"
      aria-label="Clear Queue"
      @click="emit('clearQueue')"
    >
      <IconTrash class="w-4 h-4" aria-hidden="true" />
      <span class="hidden sm:inline text-xs font-medium uppercase tracking-wider">Clear Queue</span>
    </button>
    <div class="flex items-center justify-end gap-3 sm:gap-4 overflow-x-auto scrollbar-hide w-full max-w-full">
      <div v-for="item in countCards" :key="item.label" class="flex items-center gap-1.5 whitespace-nowrap text-sm" :title="item.label">
        <component :is="item.icon" class="w-4 h-4" aria-hidden="true" />
        <span class="hidden sm:inline tracking-wider uppercase text-xs">{{ item.label }}:</span>
        <strong class="text-text">{{ item.value }}</strong>
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
