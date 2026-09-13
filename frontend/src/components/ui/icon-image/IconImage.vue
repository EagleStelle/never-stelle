<script lang="ts">
import { reactive } from "vue"

const RETRY_BASE_MS = 5_000
const RETRY_MAX_MS = 10 * 60_000

// Shared by every instance: a failed src renders nothing and is retried with backoff.
const brokenIcons = reactive(new Set<string>())
const failures = new Map<string, number>()

function markBroken(src: string): void {
  if (brokenIcons.has(src)) return
  const count = failures.get(src) ?? 0
  failures.set(src, count + 1)
  brokenIcons.add(src)
  setTimeout(() => brokenIcons.delete(src), Math.min(RETRY_BASE_MS * 3 ** count, RETRY_MAX_MS))
}
</script>

<script setup lang="ts">
defineOptions({
  inheritAttrs: false,
})

const props = withDefaults(defineProps<{ src?: string }>(), { src: "" })
</script>

<template>
  <img
    v-if="props.src && !brokenIcons.has(props.src)"
    v-bind="$attrs"
    :src="props.src"
    alt=""
    aria-hidden="true"
    @error="markBroken(props.src)"
  />
  <slot v-else />
</template>
