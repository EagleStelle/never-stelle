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
      'bg-transparent text-white/65 [.light-mode_&]:text-black/65 hover:text-white [.light-mode_&]:hover:text-black',
      'aria-pressed:bg-accent aria-pressed:text-black [.light-mode_&]:aria-pressed:text-black',
      'data-[state=active]:bg-accent data-[state=active]:text-black [.light-mode_&]:data-[state=active]:text-black',
      props.class
    ]"
  >
    <slot />
  </Primitive>
</template>
