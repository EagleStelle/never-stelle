<script setup lang="ts">
// Outermost stack of a settings pane: separates groups with the one section rhythm.
// Panes compose Section > Group > Row and never write spacing classes themselves, so
// a pane cannot drift from the others or depend on a child's own margin.
</script>

<template>
  <div class="flex flex-col gap-6">
    <slot />
  </div>
</template>
