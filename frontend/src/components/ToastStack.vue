<script setup lang="ts">
import type { ToastMessage } from"../types";

defineProps<{
 toasts: ToastMessage[];
}>();
</script>

<template>
 <div class="fixed z-50 left-2 right-2 bottom-16 grid gap-1.5 sm:left-auto sm:bottom-2 sm:w-80">
 <TransitionGroup name="toast">
 <div v-for="toast in toasts" :key="toast.id" class="rounded-lg bg-secondary text-text px-3 py-2" :class="toast.type ==='error'?'text-text':''">
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
