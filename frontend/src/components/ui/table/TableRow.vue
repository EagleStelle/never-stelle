<script setup lang="ts">
import { Primitive } from 'reka-ui'
</script>

<template>
  <Primitive
    as="tr"
    class="transition-[background] duration-200 ease-glass hover:bg-(--glass-hover) [&:last-child>td]:border-b-0"
    v-bind="$attrs"
  >
    <slot />
  </Primitive>
</template>
