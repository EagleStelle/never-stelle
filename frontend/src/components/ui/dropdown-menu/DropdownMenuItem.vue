<script setup lang="ts">
import type { DropdownMenuItemProps } from "reka-ui"
import type { HTMLAttributes } from "vue"
import { reactiveOmit } from "@vueuse/core"
import { DropdownMenuItem, useForwardProps } from "reka-ui"
import { cn } from "@/lib/utils"

const props = withDefaults(defineProps<DropdownMenuItemProps & {
  class?: HTMLAttributes["class"]
  inset?: boolean
  variant?: "default" | "destructive"
}>(), {
  variant: "default",
})

const delegatedProps = reactiveOmit(props, "inset", "variant", "class")

const forwardedProps = useForwardProps(delegatedProps)
</script>

<template>
  <DropdownMenuItem
    data-slot="dropdown-menu-item"
    :data-inset="inset ? '' : undefined"
    :data-variant="variant"
    v-bind="forwardedProps"
    :class="cn(`relative flex cursor-pointer items-center gap-2 rounded-lg px-2.5 py-2 text-sm outline-hidden select-none transition-colors duration-200 ease-glass hover:bg-accent/20 focus:bg-accent/20 data-[disabled]:pointer-events-none data-[disabled]:opacity-40 data-[inset]:pl-8 data-[variant=destructive]:text-destructive data-[variant=destructive]:hover:bg-destructive/15 data-[variant=destructive]:focus:bg-destructive/15 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4 [&_svg:not([class*='text-'])]:text-muted-foreground data-[variant=destructive]:*:[svg]:text-destructive!`, props.class)"
  >
    <slot />
  </DropdownMenuItem>
</template>
