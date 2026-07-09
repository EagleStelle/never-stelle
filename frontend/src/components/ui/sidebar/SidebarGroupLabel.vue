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
    :class="['px-2 pb-0.5 text-[0.7rem] font-semibold uppercase tracking-wider text-white/50 in-[.light-mode]:text-black/50', props.class]"
  >
    <slot />
  </Primitive>
</template>
