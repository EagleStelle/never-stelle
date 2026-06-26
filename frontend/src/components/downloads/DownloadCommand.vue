<script setup lang="ts">
import IconDownload from"~icons/material-symbols/download";
import IconLink from"~icons/material-symbols/link";
import IconPaste from"~icons/material-symbols/content-paste-rounded";
import InputBar from"../ui/InputBar.vue";

import type { SavedSettings } from"../../types";

defineProps<{
 savedSettings: SavedSettings;
 url: string;
}>();

const emit = defineEmits<{
 addDownload: [];
"update:url": [url: string];
}>();

const pasteFromClipboard = async () => {
 try {
 const text = await navigator.clipboard.readText();
 emit('update:url', text);
 } catch (err) {
 console.error('Failed to read clipboard contents:', err);
 }
};
</script>

<template>
 <section aria-label="Add download">
 <form class="flex items-center gap-2" @submit.prevent="emit('addDownload')">
 <InputBar
 class="flex-1 min-w-0"
 :model-value="url"
 @update:model-value="(val) => emit('update:url', val)"
 name="url"
 type="url"
 autocomplete="off"
 placeholder="Paste a supported URL"
 required
 >
 <template #icon>
 <IconLink class="w-5 h-5" aria-hidden="true" />
 </template>
 <template #action>
 <button 
 type="button" 
 class="flex items-center justify-center w-8 h-8 text-text-muted hover:text-text transition-all duration-300 ease-glass rounded-lg hover:bg-white/10 mr-0.5"
 aria-label="Paste from clipboard"
 @click="pasteFromClipboard"
 >
 <IconPaste class="w-5 h-5" aria-hidden="true" />
 </button>
 </template>
 </InputBar>
 <button type="submit" class="group inline-flex items-center justify-center rounded-xl bg-primary text-text-on-primary leading-none transition-all duration-300 ease-glass active:scale-[0.96] w-10 h-10 shrink-0 shadow-[0_10px_28px_-10px_rgba(237,158,89,0.7)] hover:shadow-[0_14px_34px_-8px_rgba(237,158,89,0.85)]" aria-label="Download">
 <IconDownload aria-hidden="true" class="w-6 h-6 transition-transform duration-300 ease-glass group-hover:-translate-y-px group-hover:scale-110" />
 </button>
 </form>
 </section>
</template>
