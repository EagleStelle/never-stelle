<script setup lang="ts">
import type { ToastMessage } from "../types";

defineProps<{
  toasts: ToastMessage[];
}>();
</script>

<template>
  <div class="fixed z-90 right-[0.75rem] bottom-[5.7rem] grid w-[min(22rem,calc(100vw-1.5rem))] gap-[0.5rem] sm:bottom-[0.75rem]">
    <TransitionGroup name="toast">
      <div v-for="toast in toasts" :key="toast.id" class="border border-accent-subtle rounded-lg bg-panel text-text px-[0.85rem] py-[0.75rem] text-[0.9rem] font-[800]" :class="toast.type === 'error' ? 'border-accent text-text' : 'border-accent'">
        {{ toast.message }}
      </div>
    </TransitionGroup>
  </div>
</template>

<style scoped>
.toast-enter-active,
.toast-leave-active {
  transition:
    opacity 180ms ease,
    transform 180ms ease;
}

.toast-enter-from,
.toast-leave-to {
  opacity: 0;
  transform: translateY(6px);
}
</style>
