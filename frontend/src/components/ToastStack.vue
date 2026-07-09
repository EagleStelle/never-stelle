<script setup lang="ts">
import type { ToastMessage } from"../types";

defineProps<{
 toasts: ToastMessage[];
}>();
</script>

<template>
 <div class="fixed z-50 left-2 right-2 bottom-16 grid gap-1.5 sm:left-auto sm:bottom-2 sm:w-80">
 <TransitionGroup name="toast">
 <div v-for="toast in toasts" :key="toast.id" class="rounded-lg glass-chrome text-white in-[.light-mode]:text-black px-4 py-3">
 {{ toast.message }}
 </div>
 </TransitionGroup>
 </div>
</template>

<style scoped>
.toast-enter-active,
.toast-leave-active {
 transition: transform 180ms var(--ease-glass);
}

.toast-enter-from,
.toast-leave-to {
 transform: translateY(6px);
}
</style>
