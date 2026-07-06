<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconLink from "~icons/material-symbols/link";
import IconPaste from "~icons/material-symbols/content-paste-rounded";
import { Input } from "../../components/ui/input";
import { Button } from "../../components/ui/button";

import type { SavedSettings } from "../../types";

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
    emit("update:url", text);
  } catch (err) {
    console.error("Failed to read clipboard contents:", err);
  }
};
</script>

<template>
  <section aria-label="Add download">
    <form class="flex items-center gap-2" @submit.prevent="emit('addDownload')">
      <Input
        size="lg"
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
          <Button
            variant="ghost"
            size="sm"
            type="button"
            class="mr-0.5"
            aria-label="Paste from clipboard"
            @click="pasteFromClipboard"
          >
            <template #icon>
              <IconPaste class="w-5 h-5" aria-hidden="true" />
            </template>
          </Button>
        </template>
      </Input>
      <Button
        variant="primary"
        size="lg"
        type="submit"
        class="shrink-0"
        aria-label="Download"
      >
        <template #icon>
          <IconDownload
            aria-hidden="true"
            class="w-6 h-6 transition-transform duration-300 ease-glass group-hover:-translate-y-px group-hover:scale-110"
          />
        </template>
      </Button>
    </form>
  </section>
</template>
