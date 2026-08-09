<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconLink from "~icons/material-symbols/link";
import IconPaste from "~icons/material-symbols/content-paste-rounded";

import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useDashboard } from "@/composables/useDashboard";
import { extractUrl } from "@/utils/dashboard";

const { addDownloadTask, url } = useDashboard();

// A bare URL pastes natively so the caret and selection replacement behave normally.
function onPaste(event: ClipboardEvent): void {
  const raw = event.clipboardData?.getData("text") || "";
  const cleaned = extractUrl(raw);
  if (cleaned === raw.trim()) return;
  event.preventDefault();
  url.value = cleaned;
}

// The button never fires a paste event, so it cleans its own clipboard read.
async function pasteFromClipboard(): Promise<void> {
  try {
    url.value = extractUrl(await navigator.clipboard.readText());
  } catch (err) {
    console.error("Failed to read clipboard contents:", err);
  }
}
</script>

<template>
  <form
    class="flex items-center gap-2 w-full"
    aria-label="Add download"
    @submit.prevent="addDownloadTask"
  >
    <Input
      v-model="url"
      class="flex-1 min-w-0"
      @paste="onPaste"
      name="url"
      type="url"
      autocomplete="off"
      placeholder="Paste a supported URL"
      required
    >
      <template #icon>
        <IconLink
          class="w-5 h-5 text-white in-[.light-mode]:text-black"
          aria-hidden="true"
        />
      </template>
      <template #action>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          class="mr-0.5"
          aria-label="Paste from clipboard"
          @click="pasteFromClipboard"
        >
          <template #icon>
            <IconPaste aria-hidden="true" class="group-hover:scale-110" />
          </template>
        </Button>
      </template>
    </Input>

    <Button variant="primary" size="icon" type="submit" aria-label="Download">
      <template #icon>
        <IconDownload
          aria-hidden="true"
          class="group-hover:-translate-y-px group-hover:scale-110"
        />
      </template>
    </Button>
  </form>
</template>
