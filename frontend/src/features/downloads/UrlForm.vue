<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconLink from "~icons/material-symbols/link";

import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useDashboard } from "@/composables/useDashboard";
import { extractUrl } from "@/utils/dashboard";

const { addDownloadTask, url } = useDashboard();
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
      :paste="extractUrl"
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
