<script setup lang="ts">
import type { Component } from "vue";

defineProps<{
  countCards: Array<{ label: string; value: number; icon: Component }>;
}>();
</script>

<template>
  <section class="grid grid-cols-[repeat(2,minmax(0,1fr))] gap-[0.5rem] my-[0.55rem] md:grid-cols-[repeat(4,minmax(0,1fr))]" aria-label="Task counts">
    <article v-for="item in countCards" :key="item.label" class="border border-accent-subtle rounded-lg bg-panel grid grid-cols-[auto_minmax(0,1fr)] items-center gap-y-[0.1rem] gap-x-[0.55rem] p-[0.8rem]">
      <component :is="item.icon" aria-hidden="true" />
      <span>{{ item.label }}</span>
      <strong>{{ item.value }}</strong>
    </article>
  </section>
</template>
