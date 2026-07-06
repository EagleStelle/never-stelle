<script setup lang="ts">
import { Primitive } from 'reka-ui'

const props = withDefaults(
  defineProps<{
    as?: string
    class?: string
  }>(),
  { as: 'li' }
)
</script>

<template>
  <Primitive
    :as="as"
    :class="['relative', props.class]"
  >
    <slot />
  </Primitive>
</template>
