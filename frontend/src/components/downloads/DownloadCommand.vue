<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconHardDrives from "~icons/material-symbols/dns";
import IconDesktop from "~icons/material-symbols/desktop-windows";

import type { SavedSettings } from "../../types";

defineProps<{
  savedSettings: SavedSettings;
  url: string;
}>();

const emit = defineEmits<{
  addDownload: [];
  updateSaveMode: [mode: "nas" | "device"];
  "update:url": [url: string];
}>();
</script>

<template>
  <section class="border border-accent-subtle rounded-lg bg-panel p-[0.55rem]" aria-label="Add download">
    <form class="grid grid-cols-[minmax(0,1fr)_3rem] items-center gap-[0.5rem] md:grid-cols-[minmax(0,1fr)_auto_3rem]" @submit.prevent="emit('addDownload')">
      <label class="flex min-w-0 h-[3rem] overflow-hidden border border-accent-subtle rounded-lg bg-panel-subtle focus-within:border-accent">
        <span class="sr-only">Download URL</span>
        <input
          :value="url"
          name="url"
          type="url"
          autocomplete="off"
          placeholder="Paste a supported URL"
          required
          @input="emit('update:url', ($event.target as HTMLInputElement).value)"
        />
      </label>

      <div class="inline-flex items-center min-w-0 gap-[0.25rem] border border-accent-subtle rounded-lg bg-panel-subtle p-[0.25rem] col-span-full order-3 md:col-auto md:row-auto md:order-0" aria-label="Save mode">
        <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.35rem] px-[0.7rem] text-[0.88rem] border border-transparent rounded-lg bg-transparent text-text-muted font-[800] leading-none whitespace-nowrap transition-all duration-[180ms] ease-out active:scale-[0.98] aria-pressed:bg-accent aria-pressed:text-bg" :aria-pressed="savedSettings.save_mode === 'nas'" @click="emit('updateSaveMode', 'nas')">
          <IconHardDrives aria-hidden="true" />
          <span>NAS</span>
        </button>
        <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.35rem] px-[0.7rem] text-[0.88rem] border border-transparent rounded-lg bg-transparent text-text-muted font-[800] leading-none whitespace-nowrap transition-all duration-[180ms] ease-out active:scale-[0.98] aria-pressed:bg-accent aria-pressed:text-bg" :aria-pressed="savedSettings.save_mode === 'device'" @click="emit('updateSaveMode', 'device')">
          <IconDesktop aria-hidden="true" />
          <span>Device</span>
        </button>
      </div>

      <button type="submit" class="inline-flex items-center justify-center gap-[0.42rem] border border-text rounded-lg bg-accent text-bg font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] col-start-2 row-start-1 w-[3rem] h-[3rem] hover:border-accent hover:bg-text hover:text-bg md:col-auto md:row-auto" aria-label="Download">
        <IconDownload aria-hidden="true" />
      </button>
    </form>
  </section>
</template>
