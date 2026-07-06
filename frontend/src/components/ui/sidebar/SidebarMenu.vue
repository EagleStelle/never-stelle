<script setup lang="ts">
import { Primitive } from 'reka-ui'

const props = withDefaults(
  defineProps<{
    as?: string
    class?: string
  }>(),
  { as: 'ul' }
)
</script>

<template>
  <Primitive
    :as="as"
    :class="['flex flex-col gap-1 w-full m-0 p-0 list-none', props.class]"
  >
    <slot />
  </Primitive>
</template>
