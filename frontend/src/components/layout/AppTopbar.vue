<script setup lang="ts">
import type { Component } from "vue";
import type { MenuKey } from "../../types";

defineProps<{
  activeMenu: MenuKey;
  navigationItems: Array<{ key: MenuKey; label: string; icon: Component }>;
}>();

const emit = defineEmits<{
  selectMenu: [menu: MenuKey];
}>();
</script>

<template>
  <div class="w-full overflow-x-auto pb-2 mb-[0.65rem] scrollbar-hide">
    <nav class="inline-flex items-center p-1 bg-panel border border-accent-subtle rounded-xl min-w-max">
      <button
        v-for="item in navigationItems"
        :key="item.key"
        type="button"
        class="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-[0.85rem] font-[600] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:text-text focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
        :class="activeMenu === item.key ? 'bg-accent text-bg shadow-sm' : 'text-text-muted hover:bg-panel-subtle'"
        :aria-pressed="activeMenu === item.key"
        @click="emit('selectMenu', item.key)"
      >
        <component :is="item.icon" class="w-[1.1rem] h-[1.1rem] shrink-0" aria-hidden="true" />
        <span>{{ item.label }}</span>
      </button>
    </nav>
  </div>
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
