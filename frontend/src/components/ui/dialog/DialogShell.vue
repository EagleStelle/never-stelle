<script setup lang="ts">
import { computed } from "vue";
import {
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogOverlay,
  DialogPortal,
  DialogTitle,
} from "reka-ui";
import { X } from "@lucide/vue";
import DialogRoot from "@/components/ui/dialog/Dialog.vue";

const props = withDefaults(
  defineProps<{
    open: boolean;
    title: string;
    description?: string;
    hideTitle?: boolean;
    showClose?: boolean;
    overlayClass?: string;
    contentClass?: string;
  }>(),
  {
    hideTitle: false,
    showClose: true,
    overlayClass: "fixed inset-0 z-60 bg-black/50 backdrop-blur-sm",
    contentClass:
      "fixed left-1/2 top-1/2 z-70 flex max-h-[92vh] w-[min(720px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none",
  },
);

const emit = defineEmits<{ "update:open": [open: boolean] }>();

const openModel = computed({
  get: () => props.open,
  set: (value) => emit("update:open", value),
});
</script>

<template>
  <DialogRoot v-model:open="openModel">
    <DialogPortal>
      <DialogOverlay :class="overlayClass" />
      <DialogContent :class="contentClass">
        <DialogTitle
          :class="
            hideTitle
              ? 'sr-only'
              : 'shrink-0 px-5 pt-5 text-lg font-semibold sm:px-6'
          "
        >
          {{ title }}
        </DialogTitle>
        <DialogDescription
          v-if="description"
          :class="
            hideTitle
              ? 'sr-only'
              : 'shrink-0 px-5 pb-1 text-sm text-white/60 in-[.light-mode]:text-black/60 sm:px-6'
          "
        >
          {{ description }}
        </DialogDescription>

        <DialogClose v-if="showClose" as-child>
          <button
            type="button"
            class="absolute right-4 top-4 z-10 inline-flex h-9 w-9 items-center justify-center rounded-lg bg-transparent text-white/70 transition-all duration-300 ease-glass hover:text-white active:scale-[0.96] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent in-[.light-mode]:text-black/70 in-[.light-mode]:hover:text-black"
            aria-label="Close"
            title="Close"
          >
            <X class="size-5" aria-hidden="true" />
          </button>
        </DialogClose>

        <slot />
      </DialogContent>
    </DialogPortal>
  </DialogRoot>
</template>
