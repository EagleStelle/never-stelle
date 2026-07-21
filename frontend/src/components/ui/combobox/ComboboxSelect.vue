<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import type { Component } from "vue";
import ShadcnCombobox from "./Combobox.vue";
import ComboboxAnchor from "./ComboboxAnchor.vue";
import ComboboxEmpty from "./ComboboxEmpty.vue";
import ComboboxGroup from "./ComboboxGroup.vue";
import ComboboxInput from "./ComboboxInput.vue";
import ComboboxItem from "./ComboboxItem.vue";
import ComboboxItemIndicator from "./ComboboxItemIndicator.vue";
import ComboboxList from "./ComboboxList.vue";
import ComboboxTrigger from "./ComboboxTrigger.vue";
import ComboboxViewport from "./ComboboxViewport.vue";
import { ChevronsUpDownIcon } from "@lucide/vue";
import IconCheck from "~icons/material-symbols/check";

type ComboboxItemOption = {
  key: string;
  label: string;
  icon?: Component;
  iconUrl?: string;
  initials?: string;
  disabled?: boolean;
};

const props = withDefaults(
  defineProps<{
    modelValue: string;
    items: ComboboxItemOption[];
    placeholder?: string;
    emptyText?: string;
    class?: string;
    layout?: "fit" | "fill";
  }>(),
  { layout: "fit" },
);

const emit = defineEmits<{
  "update:modelValue": [value: string];
}>();

const activeItem = computed(() => {
  return props.items.find((i) => i.key === props.modelValue) || null;
});

const activeLabel = computed(() => {
  return activeItem.value?.label || "";
});

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

const isComboboxItemOption = (value: unknown): value is ComboboxItemOption =>
  typeof value === "object" &&
  value !== null &&
  "key" in value &&
  "label" in value;

const displayValue = (value: unknown) =>
  isComboboxItemOption(value) ? value.label : "";

const handleModelValue = (value: unknown) => {
  if (isComboboxItemOption(value)) {
    emit("update:modelValue", value.key);
  }
};

const handleOpenChange = (isOpen: boolean) => {
  if (!isOpen) {
    syncSearchTerm();
  }
};

const sizerLabels = computed(() => [
  ...props.items.map((item) => item.label),
  props.placeholder || "Search...",
]);

const hasItemVisual = computed(() =>
  props.items.some((item) => item.icon || item.iconUrl || item.initials),
);
</script>

<template>
  <ShadcnCombobox
    :model-value="items.find((i) => i.key === modelValue)"
    by="label"
    v-model:search-term="searchTerm"
    :display-value="displayValue"
    @update:open="handleOpenChange"
    @update:model-value="handleModelValue"
    :class="[props.class, props.layout === 'fill' ? 'w-full' : 'inline-block']"
  >
    <ComboboxAnchor as-child>
      <ComboboxTrigger as-child>
        <button
          type="button"
          :class="[
            'h-9 items-center gap-2 overflow-hidden rounded-lg bg-black/20 in-[.light-mode]:bg-white/40 border border-(--glass-border) px-3 py-2 text-sm font-medium text-white in-[.light-mode]:text-black shadow-inner transition-all duration-300 ease-glass focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent',
            props.layout === 'fill'
              ? 'grid w-full grid-cols-[minmax(0,1fr)_auto]'
              : 'inline-grid max-w-[calc(100vw-2rem)] grid-cols-[minmax(0,max-content)_auto]',
          ]"
        >
          <span
            v-for="(label, index) in sizerLabels"
            :key="index"
            aria-hidden="true"
            :class="[
              'pointer-events-none invisible col-start-1 row-start-1 block max-w-[min(28rem,calc(100vw-5rem))] overflow-hidden whitespace-nowrap',
              hasItemVisual && 'pr-4',
            ]"
          >
            {{ label }}
          </span>
          <div
            :class="[
              'col-start-1 row-start-1 min-w-0 max-w-[min(28rem,calc(100vw-5rem))] flex items-center gap-2 text-left',
              !activeLabel && 'opacity-55',
            ]"
          >
            <template v-if="activeItem">
              <img
                v-if="activeItem.iconUrl"
                :src="activeItem.iconUrl"
                class="w-4 h-4 shrink-0 rounded-lg"
                alt=""
                aria-hidden="true"
              />
              <component
                :is="activeItem.icon"
                v-else-if="activeItem.icon"
                class="w-4 h-4 shrink-0"
                aria-hidden="true"
              />
              <span
                v-else-if="activeItem.initials"
                class="inline-flex h-5 min-w-5 items-center justify-center rounded glass-soft px-1 text-[0.65rem] font-semibold"
              >
                {{ activeItem.initials }}
              </span>
            </template>
            <span class="truncate">{{ activeLabel || props.placeholder || "Search..." }}</span>
          </div>
          <ChevronsUpDownIcon
            class="col-start-2 row-start-1 size-4 shrink-0 opacity-50"
            aria-hidden="true"
          />
        </button>
      </ComboboxTrigger>
    </ComboboxAnchor>

    <ComboboxList
      class="z-100 w-auto min-w-(--reka-combobox-trigger-width) glass-chrome rounded-lg overflow-hidden border border-(--glass-border) text-white in-[.light-mode]:text-black shadow-none"
      :style="{
        width: 'var(--reka-combobox-trigger-width)',
        minWidth: 'var(--reka-combobox-trigger-width)',
      }"
      position="popper"
      align="start"
      :side-offset="6"
    >
      <ComboboxInput
        class="text-white in-[.light-mode]:text-black placeholder:text-white/50 in-[.light-mode]:placeholder:text-black/50"
        :placeholder="props.placeholder || 'Search...'"
      />
      <ComboboxViewport class="p-1">
        <ComboboxEmpty
          class="py-3 text-center text-sm text-white in-[.light-mode]:text-black"
        >
          {{ props.emptyText || "No items found." }}
        </ComboboxEmpty>

        <ComboboxGroup class="p-0 text-white in-[.light-mode]:text-black">
          <ComboboxItem
            v-for="item in items"
            :key="item.key"
            :value="item"
            :disabled="item.disabled"
            class="glass-option relative flex w-full cursor-pointer select-none items-center rounded-lg py-2 pl-8 pr-2 text-sm outline-none text-white in-[.light-mode]:text-black transition-all duration-300 ease-glass data-[disabled]:cursor-not-allowed data-[disabled]:opacity-40 data-[highlighted]:!bg-transparent data-[highlighted]:!text-white in-[.light-mode]:data-[highlighted]:!text-black"
          >
            <ComboboxItemIndicator
              class="absolute left-2 ml-0 flex h-4 w-4 items-center justify-center"
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
        </ComboboxGroup>
      </ComboboxViewport>
    </ComboboxList>
  </ShadcnCombobox>
</template>
