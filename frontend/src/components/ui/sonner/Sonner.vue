<script lang="ts" setup>
import {
  CircleCheckIcon,
  InfoIcon,
  TriangleAlertIcon,
  OctagonXIcon,
  Loader2Icon,
  XIcon,
} from "@lucide/vue";

import { computed } from "vue";
import { useMediaQuery } from "@vueuse/core";

import type { ToasterProps } from "vue-sonner";
import { Toaster as Sonner } from "vue-sonner";

import { cn } from "@/lib/utils";

// statusBarClearance: measured height (px) of the docked status bar, so the desktop
// toast clears it instead of hardcoding a margin.
const props = withDefaults(
  defineProps<ToasterProps & { statusBarClearance?: number }>(),
  { statusBarClearance: 0 },
);

// Forward only real ToasterProps to <Sonner>; keep our custom prop out of it.
const forwarded = computed(() => {
  const { statusBarClearance: _ignored, ...rest } = props;
  return rest;
});

// Desktop docks bottom-right; below lg the status bar + bottom nav own that corner, so anchor top.
const isDesktop = useMediaQuery("(min-width: 1024px)");
const position = computed<ToasterProps["position"]>(() =>
  isDesktop.value ? "bottom-right" : "top-center",
);

// Lift the bottom edge above the status bar; mobile anchors top so bottom is unused there.
const offset = computed(() => ({
  top: "0.75rem",
  right: "0.5rem",
  bottom: `calc(${props.statusBarClearance}px + 0.5rem)`,
  left: "0.5rem",
}));
</script>

<template>
  <Teleport to="body">
    <Sonner
      v-bind="forwarded"
    :class="cn('toaster group z-[100]', props.class)"
    :position="position"
    :duration="3200"
    :offset="offset"
    :mobile-offset="{ top: '0.75rem', left: '0.5rem', right: '0.5rem' }"
    :style="{
      '--normal-bg': 'transparent',
      '--normal-border': 'transparent',
      '--normal-text': 'inherit',
    }"
    :toast-options="{
      classes: {
        toast:
          'group toast w-full gap-2.5! rounded-lg glass-chrome px-4! py-3! text-sm text-white! in-[.light-mode]:text-black!',
        title: 'font-medium leading-snug',
        description: 'text-white/70 in-[.light-mode]:text-black/70',
        actionButton: 'rounded-lg bg-accent! px-2.5! text-black!',
        cancelButton: 'rounded-lg glass px-2.5! text-white! in-[.light-mode]:text-black!',
        closeButton: 'glass text-white! in-[.light-mode]:text-black!',
        icon: 'text-accent',
      },
    }"
  >
    <template #success-icon>
      <CircleCheckIcon class="size-4 text-accent" />
    </template>
    <template #info-icon>
      <InfoIcon class="size-4 text-accent" />
    </template>
    <template #warning-icon>
      <TriangleAlertIcon class="size-4 text-accent" />
    </template>
    <template #error-icon>
      <OctagonXIcon class="size-4 text-accent" />
    </template>
    <template #loading-icon>
      <div>
        <Loader2Icon class="size-4 animate-spin text-accent" />
      </div>
    </template>
    <template #close-icon>
      <XIcon class="size-4" />
    </template>
    </Sonner>
  </Teleport>
</template>

<style>
[data-sonner-toaster] {
  z-index: 9999 !important;
}

/* Force sonner toasts to use glass chrome backgrounds instead of default state colors */
.toaster [data-sonner-toast] {
  background: linear-gradient(
    135deg,
    var(--glass-strong),
    var(--glass-strong-2)
  ) !important;
  border: var(--glass-border-rule) !important;
}
</style>
