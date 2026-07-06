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
    :class="['flex flex-col gap-1 w-full', props.class]"
  >
    <slot />
  </Primitive>
</template>
