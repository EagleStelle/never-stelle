<script setup lang="ts">
import type { ToastMessage } from "../types";

defineProps<{
  toasts: ToastMessage[];
}>();
</script>

<template>
  <div class="fixed bottom-4 right-4 z-[60] grid max-w-[360px] gap-3">
    <TransitionGroup name="toast">
      <div
        v-for="toast in toasts"
        :key="toast.id"
        class="rounded-2xl border px-4 py-3 text-sm font-medium shadow-soft transition"
        :class="
          toast.type === 'error'
            ? 'border-[color-mix(in_srgb,var(--ns-danger)_30%,transparent)] bg-[var(--ns-panel)] text-[var(--ns-text)]'
            : 'border-[color-mix(in_srgb,var(--ns-success)_30%,transparent)] bg-[var(--ns-panel)] text-[var(--ns-text)]'
        "
      >
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
