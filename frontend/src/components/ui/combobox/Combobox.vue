<script setup lang="ts">
import { ref, computed, watch, nextTick, onMounted } from "vue";
import type { Component } from "vue";
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
} from "reka-ui";
import IconChevronDown from "~icons/material-symbols/keyboard-arrow-down-rounded";
import IconCheck from "~icons/material-symbols/check";

type ComboboxItemOption = {
  key: string;
  label: string;
  icon?: Component;
  iconUrl?: string;
  initials?: string;
  disabled?: boolean;
};

type Size = "xs" | "sm" | "default" | "lg" | "xl";

const props = withDefaults(
  defineProps<{
    modelValue: string;
    items: ComboboxItemOption[];
    placeholder?: string;
    emptyText?: string;
    class?: string;
    size?: Size;
  }>(),
  { size: "default" },
);

const emit = defineEmits<{
  "update:modelValue": [value: string];
}>();

const activeLabel = computed(() => {
  return props.items.find((i) => i.key === props.modelValue)?.label || "";
});
const activeItem = computed(() =>
  props.items.find((i) => i.key === props.modelValue),
);

const searchTerm = ref("");

const syncSearchTerm = () => {
  searchTerm.value = activeLabel.value;
};

watch(
  () => props.modelValue,
  () => {
    syncSearchTerm();
  },
  { immediate: true },
);

onMounted(() => {
  // Ensure reka-ui doesn't overwrite our search term on mount
  nextTick(() => {
    syncSearchTerm();
  });
});

const filteredItems = computed(() => {
  if (searchTerm.value === "" || searchTerm.value === activeLabel.value) {
    return props.items;
  }
  return props.items.filter((item) =>
    item.label.toLowerCase().includes(searchTerm.value.toLowerCase()),
  );
});

const handleOpenChange = (isOpen: boolean) => {
  if (!isOpen) {
    syncSearchTerm();
  }
};

const SIZE_CLASS: Record<Size, { anchor: string; input: string }> = {
  xs: { anchor: "h-7", input: "text-xs" },
  sm: { anchor: "h-8", input: "text-xs" },
  default: { anchor: "h-9", input: "text-sm" },
  lg: { anchor: "h-10", input: "text-sm" },
  xl: { anchor: "h-11", input: "text-base" },
};

const sizes = computed(() => SIZE_CLASS[props.size]);
</script>

<template>
  <ComboboxRoot
    :model-value="items.find((i) => i.key === modelValue)"
    @update:model-value="
      (val: any) => {
        if (val) emit('update:modelValue', (val as any).key);
      }
    "
    v-model:searchTerm="searchTerm"
    @update:open="handleOpenChange"
    :display-value="(val: any) => (val as any)?.label || ''"
    :class="props.class"
  >
    <ComboboxAnchor
      :class="[
        'relative inline-flex w-full items-center rounded-lg glass-soft focus-within:ring-2 focus-within:ring-accent transition-all duration-300 ease-glass',
        sizes.anchor,
      ]"
    >
      <div
        class="pl-3 pr-2 flex items-center justify-center text-white in-[.light-mode]:text-black"
      >
        <img
          v-if="activeItem?.iconUrl"
          :src="activeItem.iconUrl"
          class="w-4 h-4 shrink-0 rounded-lg"
          alt=""
          aria-hidden="true"
        />
        <component
          v-else-if="activeItem?.icon"
          :is="activeItem.icon"
          class="w-4 h-4 shrink-0 text-white in-[.light-mode]:text-black"
          aria-hidden="true"
        />
        <span
          v-else-if="activeItem?.initials"
          class="inline-flex h-5 min-w-5 items-center justify-center rounded-lg glass-soft px-1 text-[0.65rem] font-semibold text-white in-[.light-mode]:text-black"
        >
          {{ activeItem.initials }}
        </span>
      </div>
      <ComboboxInput
        :class="[
          'flex-1 bg-transparent outline-none min-w-0 text-white in-[.light-mode]:text-black placeholder:text-white in-[.light-mode]:placeholder:text-black',
          sizes.input,
        ]"
        :placeholder="activeLabel || props.placeholder || 'Search...'"
      />
      <ComboboxTrigger
        class="pr-3 pl-1 flex items-center justify-center text-white in-[.light-mode]:text-black hover:text-white in-[.light-mode]:hover:text-black cursor-pointer outline-none"
      >
        <IconChevronDown class="w-5 h-5 shrink-0" aria-hidden="true" />
      </ComboboxTrigger>
    </ComboboxAnchor>

    <ComboboxPortal>
      <ComboboxContent
        class="z-[100] glass-chrome rounded-lg overflow-hidden"
        :style="{ width: 'var(--reka-combobox-trigger-width)', minWidth: 'var(--reka-combobox-trigger-width)' }"
        position="popper"
        :side-offset="6"
      >
        <ComboboxViewport class="p-1">
          <ComboboxEmpty
            class="py-3 text-center text-sm text-white in-[.light-mode]:text-black"
          >
            {{ props.emptyText || "No items found." }}
          </ComboboxEmpty>

          <ComboboxItem
            v-for="item in filteredItems"
            :key="item.key"
            :value="item"
            :disabled="item.disabled"
            class="glass-option relative flex w-full cursor-pointer select-none items-center rounded-lg py-2 pl-8 pr-2 text-sm outline-none text-white in-[.light-mode]:text-black transition-all duration-300 ease-glass data-disabled:cursor-not-allowed data-disabled:opacity-40"
          >
            <ComboboxItemIndicator
              class="absolute left-2 flex h-4 w-4 items-center justify-center"
            >
              <IconCheck class="w-4 h-4" aria-hidden="true" />
            </ComboboxItemIndicator>

            <div class="flex items-center gap-2">
              <img
                v-if="item.iconUrl"
                :src="item.iconUrl"
                class="w-4 h-4 shrink-0 rounded-lg"
                alt=""
                aria-hidden="true"
              />
              <component
                :is="item.icon"
                v-else-if="item.icon"
                class="w-4 h-4 shrink-0"
                aria-hidden="true"
              />
              <span
                v-else-if="item.initials"
                class="inline-flex h-5 min-w-5 items-center justify-center rounded glass-soft px-1 text-[0.65rem] font-semibold"
              >
                {{ item.initials }}
              </span>
              <span>{{ item.label }}</span>
            </div>
          </ComboboxItem>
        </ComboboxViewport>
      </ComboboxContent>
    </ComboboxPortal>
  </ComboboxRoot>
</template>
