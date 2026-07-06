<script setup lang="ts">
import { Primitive } from 'reka-ui'

const props = withDefaults(
  defineProps<{
    as?: string
    class?: string
  }>(),
  { as: 'div' }
)
</script>

<template>
  <Primitive
    :as="as"
    :class="['flex min-h-0 flex-1 flex-col', props.class]"
  >
    <slot />
  </Primitive>
</template>
