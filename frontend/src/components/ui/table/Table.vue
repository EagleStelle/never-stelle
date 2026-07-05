<script setup lang="ts">
import { Primitive } from 'reka-ui'
</script>

<template>
  <div class="w-full overflow-auto rounded-lg glass">
    <Primitive
      as="table"
      class="w-full min-w-full border-separate border-spacing-0 overflow-hidden text-left rounded-lg"
      v-bind="$attrs"
    >
      <slot />
    </Primitive>
  </div>
</template>
