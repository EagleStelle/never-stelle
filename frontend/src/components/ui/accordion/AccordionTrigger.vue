<script setup lang="ts">
import type { AccordionTriggerProps } from "reka-ui";
import type { Component, HTMLAttributes } from "vue";
import { ChevronDown } from "@lucide/vue";
import { reactiveOmit } from "@vueuse/core";
import { AccordionHeader, AccordionTrigger } from "reka-ui";
import IconSource from "~icons/material-symbols/language";
import { IconImage } from "@/components/ui/icon-image";
import { cn } from "@/lib/utils";

const props = defineProps<
  AccordionTriggerProps & {
    class?: HTMLAttributes["class"];
    image?: string;
    /** A glyph in the image slot, drawn like the settings sidebar icons. */
    icon?: Component;
  }
>();

const delegatedProps = reactiveOmit(props, "class", "image", "icon");
</script>

<template>
  <AccordionHeader class="flex">
    <AccordionTrigger
      data-slot="accordion-trigger"
      v-bind="delegatedProps"
      :class="
        cn(
          'group/trigger flex min-h-12 flex-1 items-center gap-3 px-2 py-2.5 text-left font-sans text-[0.9375rem] font-semibold tracking-normal outline-none transition-colors duration-200 hover:bg-white/5 in-[.light-mode]:hover:bg-black/4 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent disabled:pointer-events-none disabled:opacity-50',
          props.class,
        )
      "
    >
      <span
        v-if="props.image !== undefined"
        class="flex size-5 shrink-0 items-center justify-center"
      >
        <IconImage :src="props.image" class="size-5 rounded-sm object-contain">
          <IconSource class="size-5 text-muted-foreground" aria-hidden="true" />
        </IconImage>
      </span>
      <component
        :is="props.icon"
        v-else-if="props.icon"
        class="size-5 shrink-0 opacity-80"
        aria-hidden="true"
      />
      <span class="min-w-0 flex-1 wrap-anywhere">
        <slot />
      </span>
      <slot name="icon">
        <ChevronDown
          class="pointer-events-none size-4 shrink-0 text-muted-foreground transition-transform duration-200 group-hover/trigger:text-foreground group-data-[state=open]/trigger:rotate-180"
        />
      </slot>
    </AccordionTrigger>
  </AccordionHeader>
</template>
