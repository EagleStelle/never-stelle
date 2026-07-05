<script setup lang="ts">
import { Primitive } from 'reka-ui'
</script>

<template>
  <Primitive as="tbody" v-bind="$attrs">
    <slot />
  </Primitive>
</template>
