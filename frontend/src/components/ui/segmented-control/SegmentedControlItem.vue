<script setup lang="ts">
import type { ToggleGroupItemProps } from "reka-ui";
import { reactiveOmit } from "@vueuse/core";
import { ToggleGroupItem, useForwardProps } from "reka-ui";
import { cn } from "@/lib/utils";

const props = defineProps<ToggleGroupItemProps & { class?: string }>();
const delegatedProps = reactiveOmit(props, "class");
const forwarded = useForwardProps(delegatedProps);
</script>

<template>
  <ToggleGroupItem
    v-slot="slotProps"
    data-slot="toggle-group-item"
    v-bind="forwarded"
    :class="
      cn(
        'inline-flex h-full items-center justify-center gap-1.5 rounded-md bg-transparent px-2.5 text-sm font-medium leading-none whitespace-nowrap text-white/80 transition-all duration-300 ease-glass hover:text-white hover:bg-white/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent active:scale-[0.97] data-[state=on]:bg-accent data-[state=on]:text-accent-foreground data-[state=on]:font-semibold in-[.light-mode]:text-black/80 in-[.light-mode]:hover:text-black in-[.light-mode]:hover:bg-black/10',
        props.class,
      )
    "
  >
    <slot v-bind="slotProps" />
  </ToggleGroupItem>
</template>
