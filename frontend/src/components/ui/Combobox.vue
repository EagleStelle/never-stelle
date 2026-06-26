<script setup lang="ts">
import { ref, computed, watch, nextTick, onMounted } from"vue";
import type { Component } from"vue";
import {
 ComboboxRoot,
 ComboboxAnchor,
 ComboboxInput,
 ComboboxTrigger,
 ComboboxPortal,
 ComboboxContent,
 ComboboxViewport,
 ComboboxEmpty,
 ComboboxItem,
 ComboboxItemIndicator,
} from'reka-ui'
import IconChevronDown from"~icons/material-symbols/keyboard-arrow-down-rounded";
import IconCheck from"~icons/material-symbols/check";

const props = defineProps<{
 modelValue: string;
 items: Array<{ key: string; label: string; icon?: Component }>;
 placeholder?: string;
 emptyText?: string;
 class?: string;
}>();

const emit = defineEmits<{
'update:modelValue': [value: string];
}>();

const activeLabel = computed(() => {
 return props.items.find(i => i.key === props.modelValue)?.label ||"";
});

const searchTerm = ref("");

const syncSearchTerm = () => {
 searchTerm.value = activeLabel.value;
};

watch(() => props.modelValue, () => {
 syncSearchTerm();
}, { immediate: true });

onMounted(() => {
 // Ensure reka-ui doesn't overwrite our search term on mount
 nextTick(() => {
 syncSearchTerm();
 });
});

const filteredItems = computed(() => {
 if (searchTerm.value ==="" || searchTerm.value === activeLabel.value) {
 return props.items;
 }
 return props.items.filter((item) =>
 item.label.toLowerCase().includes(searchTerm.value.toLowerCase())
 );
});

const handleOpenChange = (isOpen: boolean) => {
 if (!isOpen) {
 syncSearchTerm();
 }
};
</script>

<template>
 <ComboboxRoot
 :model-value="items.find(i => i.key === modelValue)"
 @update:model-value="(val: any) => { if (val) emit('update:modelValue', (val as any).key) }"
 v-model:searchTerm="searchTerm"
 @update:open="handleOpenChange"
 :display-value="(val: any) => (val as any)?.label ||''"
 >
 <ComboboxAnchor :class="['relative inline-flex items-center rounded-xl glass-soft focus-within:ring-2 focus-within:ring-primary/70 focus-within:border-primary/50 h-10 transition-all duration-300 ease-glass', props.class]">
 <div class="pl-3 pr-2 flex items-center justify-center text-text-muted">
 <component 
 v-if="items.find(i => i.key === modelValue)?.icon"
 :is="items.find(i => i.key === modelValue)!.icon" 
 class="w-4 h-4 shrink-0 text-text" 
 aria-hidden="true" 
 />
 </div>
 <ComboboxInput 
 class="flex-1 bg-transparent outline-none min-w-0 text-text placeholder:text-text-muted" 
 :placeholder="activeLabel || props.placeholder ||'Search...'"
 />
 <ComboboxTrigger class="pr-3 pl-1 flex items-center justify-center text-text-muted hover:text-text cursor-pointer outline-none">
 <IconChevronDown class="w-5 h-5 shrink-0" aria-hidden="true" />
 </ComboboxTrigger>
 </ComboboxAnchor>

 <ComboboxPortal>
 <ComboboxContent
 class="z-50 min-w-[160px] glass-chrome rounded-xl shadow-[0_18px_50px_-18px_rgba(0,0,0,0.75)] overflow-hidden"
 position="popper"
 :side-offset="6"
 >
 <ComboboxViewport class="p-1">
 <ComboboxEmpty class="py-3 text-center text-sm text-text-muted">
 {{ props.emptyText ||'No items found.'}}
 </ComboboxEmpty>
 
 <ComboboxItem 
 v-for="item in filteredItems" 
 :key="item.key" 
 :value="item"
 class="relative flex w-full cursor-pointer select-none items-center rounded-md py-2 pl-8 pr-2 text-sm outline-none focus:bg-primary focus:text-text-on-primary text-text-muted data-[highlighted]:bg-primary data-[highlighted]:text-text-on-primary transition-colors"
 >
 <ComboboxItemIndicator class="absolute left-2 flex h-4 w-4 items-center justify-center">
 <IconCheck class="w-4 h-4" aria-hidden="true" />
 </ComboboxItemIndicator>

 <div class="flex items-center gap-2">
 <component :is="item.icon" v-if="item.icon" class="w-4 h-4 shrink-0" aria-hidden="true" />
 <span>{{ item.label }}</span>
 </div>
 </ComboboxItem>
 </ComboboxViewport>
 </ComboboxContent>
 </ComboboxPortal>
 </ComboboxRoot>
</template>
