<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconLink from "~icons/material-symbols/link";

import type { SavedSettings } from "../../types";

defineProps<{
  savedSettings: SavedSettings;
  url: string;
}>();

const emit = defineEmits<{
  addDownload: [];
  "update:url": [url: string];
}>();
</script>

<template>
  <section aria-label="Add download">
    <form class="grid grid-cols-[minmax(0,1fr)_2.5rem] items-center gap-[0.5rem]" @submit.prevent="emit('addDownload')">
      <label class="flex items-center min-w-0 h-10 overflow-hidden border border-accent-subtle rounded-lg bg-panel-subtle focus-within:border-accent pl-3">
        <IconLink class="text-text-muted shrink-0 w-5 h-5 mr-2" aria-hidden="true" />
        <span class="sr-only">Download URL</span>
        <input
          class="flex-1 min-w-0 bg-transparent outline-none"
          :value="url"
          name="url"
          type="url"
          autocomplete="off"
          placeholder="Paste a supported URL"
          required
          @input="emit('update:url', ($event.target as HTMLInputElement).value)"
        />
      </label>

      <button type="submit" class="inline-flex items-center justify-center gap-[0.42rem] border border-text rounded-lg bg-accent text-bg font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] w-10 h-10 hover:border-accent hover:bg-text hover:text-bg" aria-label="Download">
        <IconDownload aria-hidden="true" />
      </button>
    </form>
  </section>
</template>
