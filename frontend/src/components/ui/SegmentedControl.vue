<script setup lang="ts">
import { ToggleGroupRoot, type ToggleGroupRootProps } from'reka-ui'

const props = withDefaults(defineProps<ToggleGroupRootProps & { class?: string }>(), {
 type:'single'
})

const emit = defineEmits<{
'update:modelValue': [value: any]
}>()
</script>

<template>
 <ToggleGroupRoot
 v-bind="props"
 @update:modelValue="emit('update:modelValue', $event)"
 :class="['inline-flex items-center gap-1 px-1 py-1 h-10 glass-soft rounded-xl', props.class]"
 >
 <slot />
 </ToggleGroupRoot>
</template>
