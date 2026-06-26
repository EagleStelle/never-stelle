<script setup lang="ts">
import type { Component } from "vue";
import type { MenuKey } from "../../types";
import SegmentedControl from "../ui/SegmentedControl.vue";
import SegmentedControlItem from "../ui/SegmentedControlItem.vue";

defineProps<{
  activeMenu: MenuKey;
  navigationItems: Array<{ key: MenuKey; label: string; icon: Component }>;
}>();

const emit = defineEmits<{
  selectMenu: [menu: MenuKey];
}>();
</script>

<template>
  <div class="w-full overflow-x-auto pb-2 scrollbar-hide">
    <SegmentedControl
      :model-value="activeMenu"
      @update:model-value="(val) => emit('selectMenu', val)"
      class="bg-panel min-w-max"
    >
      <SegmentedControlItem
        v-for="item in navigationItems"
        :key="item.key"
        :value="item.key"
        class="px-4 text-[0.85rem] font-[600]"
      >
        <component :is="item.icon" class="w-[1.1rem] h-[1.1rem] shrink-0" aria-hidden="true" />
        <span>{{ item.label }}</span>
      </SegmentedControlItem>
    </SegmentedControl>
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
