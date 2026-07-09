<script setup lang="ts">
import { Primitive } from 'reka-ui'
</script>

<template>
  <Primitive
    as="th"
    class="px-3 py-2 font-semibold text-inherit border-b border-b-(--glass-border) text-left align-middle"
    v-bind="$attrs"
  >
    <slot />
  </Primitive>
</template>
