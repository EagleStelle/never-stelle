<script setup lang="ts">
import { Label } from'reka-ui'
import { computed, useAttrs } from'vue'

const props = defineProps<{
 modelValue?: string | number | null
}>()

const emit = defineEmits<{
'update:modelValue': [value: string]
}>()

defineOptions({
 inheritAttrs: false
})

const attrs = useAttrs()

// Ensure we don't pass class to the input directly from parent if it was intended for wrapper,
// or we can allow passing wrapper-class vs input-class.
// But standard Vue behavior: $attrs goes to root unless inheritAttrs: false.
</script>

<template>
 <Label
 class="flex items-center min-w-0 h-10 p-1 rounded-lg glass-soft focus-within:ring-2 focus-within:ring-accent transition-all duration-300 ease-glass gap-1"
 :class="attrs.class"
 >
 <div v-if="$slots.icon" class="flex items-center justify-center w-8 h-8 shrink-0 text-accent ml-1.5">
 <slot name="icon" />
 </div>
 <div v-else class="w-3" />
 
 <input
 v-bind="{ ...attrs, class: undefined }"
 :value="modelValue"
 @input="emit('update:modelValue', ($event.target as HTMLInputElement).value)"
 class="flex-1 min-w-0 h-full bg-transparent outline-none text-white [.light-mode_&]:text-black pr-2 placeholder:text-white [.light-mode_&]:placeholder:text-black"
 />
 
 <slot name="action" />
 </Label>
</template>
