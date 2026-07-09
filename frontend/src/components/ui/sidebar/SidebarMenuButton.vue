<script setup lang="ts">
import { Primitive } from 'reka-ui'

const props = withDefaults(
  defineProps<{
    as?: string
    asChild?: boolean
    class?: string
  }>(),
  { as: 'button', asChild: false }
)
</script>

<template>
  <Primitive
    :as="as"
    :as-child="asChild"
    :class="[
      'group inline-flex items-center gap-3 rounded-lg px-3.5 h-10 w-full font-medium leading-none transition-all duration-300 ease-glass active:scale-[0.97]',
      'bg-transparent text-white/65 in-[.light-mode]:text-black/65 hover:text-white in-[.light-mode]:hover:text-black',
      'aria-pressed:bg-accent aria-pressed:text-black in-[.light-mode]:aria-pressed:text-black',
      'data-[state=active]:bg-accent data-[state=active]:text-black in-[.light-mode]:data-[state=active]:text-black',
      props.class
    ]"
  >
    <slot />
  </Primitive>
</template>
