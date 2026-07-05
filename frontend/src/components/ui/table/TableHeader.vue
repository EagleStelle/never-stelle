<script setup lang="ts">
import { Primitive } from 'reka-ui'
</script>

<template>
  <Primitive as="thead" v-bind="$attrs">
    <slot />
  </Primitive>
</template>
