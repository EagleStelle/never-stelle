<script setup lang="ts">
import { Primitive } from 'reka-ui'

const props = withDefaults(
  defineProps<{
    as?: string
    class?: string
  }>(),
  { as: 'aside' }
)
</script>

<template>
  <Primitive
    :as="as"
    :class="['flex flex-col glass-chrome', props.class]"
  >
    <slot />
  </Primitive>
</template>
