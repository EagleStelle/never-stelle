<script setup lang="ts">
import { computed } from "vue";
import IconAdd from "~icons/material-symbols/add";
import IconDownload from "~icons/material-symbols/download";
import IconLink from "~icons/material-symbols/link";

import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useDashboard } from "@/composables/useDashboard";
import { extractUrl } from "@/utils/dashboard";

const { activePage, addDownloadTask, addTracker, url } = useDashboard();

// The trackers page takes a collection link and follows it instead of downloading once.
const tracking = computed(() => activePage.value === "trackers");

function submit(): void {
  if (tracking.value) addTracker();
  else addDownloadTask();
}
</script>

<template>
  <form
    class="flex flex-wrap lg:flex-nowrap items-center gap-x-2 gap-y-3 w-full"
    :aria-label="tracking ? 'Add tracker' : 'Add download'"
    @submit.prevent="submit"
  >
    <Input
      v-model="url"
      class="flex-1 min-w-0"
      :paste="extractUrl"
      name="url"
      type="url"
      autocomplete="off"
      :placeholder="tracking ? 'Paste a creator, channel or playlist URL' : 'Paste a supported URL'"
      required
    >
      <template #icon>
        <IconLink
          class="w-5 h-5 text-white in-[.light-mode]:text-black"
          aria-hidden="true"
        />
      </template>
    </Input>

    <!-- Link options: own row above the field below lg, inline from lg. -->
    <div
      v-if="$slots.default"
      class="order-first lg:order-0 flex items-center gap-2 w-full lg:w-auto empty:hidden"
    >
      <slot />
    </div>

    <Button
      variant="primary"
      size="icon"
      type="submit"
      :aria-label="tracking ? 'Add tracker' : 'Download'"
      :title="tracking ? 'Add tracker' : undefined"
    >
      <template #icon>
        <IconAdd
          v-if="tracking"
          aria-hidden="true"
          class="group-hover:scale-110"
        />
        <IconDownload
          v-else
          aria-hidden="true"
          class="group-hover:-translate-y-px group-hover:scale-110"
        />
      </template>
    </Button>
  </form>
</template>
