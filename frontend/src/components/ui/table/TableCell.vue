<script setup lang="ts">
import { Primitive } from 'reka-ui'
</script>

<template>
  <Primitive
    as="td"
    class="px-3 py-2 border-b border-b-[var(--glass-border)] text-inherit align-middle"
    v-bind="$attrs"
  >
    <slot />
  </Primitive>
</template>
