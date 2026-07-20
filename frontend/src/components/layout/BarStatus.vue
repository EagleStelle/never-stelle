<script setup lang="ts">
import type { Component } from "vue";
import IconTrash from "~icons/material-symbols/delete";
import { Button } from "../ui/button";

defineProps<{
  countCards: Array<{ label: string; value: number; icon: Component }>;
}>();

const emit = defineEmits<{
  clearQueue: [];
}>();
</script>

<template>
  <footer
    class="z-40 glass glass-border-top py-2 px-3 flex items-center justify-between text-white in-[.light-mode]:text-black mb-(--nav-bottom-height) lg:mb-0 gap-3"
    aria-label="Task counts"
  >
    <Button
      type="button"
      class="shrink-0"
      title="Clear Queue"
      aria-label="Clear Queue"
      @click="emit('clearQueue')"
    >
      <template #icon>
        <IconTrash class="w-4 h-4" aria-hidden="true" />
      </template>
      <span
        class="hidden sm:inline text-xs font-medium uppercase tracking-wider"
        >Clear Queue</span
      >
    </Button>
    <div
      class="flex items-center justify-end gap-3 sm:gap-4 overflow-x-auto scrollbar-hide w-full max-w-full"
    >
      <div
        v-for="item in countCards"
        :key="item.label"
        class="flex items-center gap-1.5 whitespace-nowrap text-sm"
        :title="item.label"
      >
        <component :is="item.icon" class="w-4 h-4" aria-hidden="true" />
        <span class="hidden sm:inline tracking-wider uppercase text-xs"
          >{{ item.label }}:</span
        >
        <strong class="text-white in-[.light-mode]:text-black">{{
          item.value
        }}</strong>
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
