<script setup lang="ts">
import type { ComboboxItemOption } from "@/components/ui/combobox/types"
import { IconImage } from "@/components/ui/icon-image"

defineProps<{
  item: Pick<ComboboxItemOption, "icon" | "iconUrl">
}>()
</script>

<template>
  <!-- Fixed 16px slot, filled or empty. -->
  <span class="flex size-4 shrink-0 items-center justify-center" aria-hidden="true">
    <IconImage :src="item.iconUrl" class="size-4">
      <component :is="item.icon" v-if="item.icon" class="size-4" />
    </IconImage>
  </span>
</template>
