<script setup lang="ts">
import { reactive } from "vue";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";

import { Button } from "../../../../components/ui/button";
import { Input } from "../../../../components/ui/input";
import { useSettingsContext } from "../../context";
import SettingsRow from "../../SettingsRow.vue";

const { editableSourceProfiles, cookieStatuses, connectCookies, connectCookiesSource, removeCookies } =
  useSettingsContext();

const cookieFiles = reactive<Record<string, File | null>>({});
const newCookie = reactive<{ source: string; file: File | null }>({ source: "", file: null });

function onCookieFile(site: string, event: Event): void {
  const file = (event.target as HTMLInputElement).files?.[0] || null;
  cookieFiles[site] = file;
  if (file) connectCookies(site, file);
}

function openPicker(site: string): void {
  document.getElementById(`${site}CookiesInput`)?.click();
}

function onNewCookieFile(event: Event): void {
  const file = (event.target as HTMLInputElement).files?.[0] || null;
  newCookie.file = file;
  if (file) connectNew();
}

function openNewPicker(): void {
  document.getElementById("newSourceCookiesInput")?.click();
}

function connectNew(): void {
  connectCookiesSource(newCookie.source, newCookie.file || undefined);
  newCookie.source = "";
  newCookie.file = null;
}
</script>

<template>
  <div class="py-2">
    <div class="flex w-full items-center gap-2">
      <Input
        v-model="newCookie.source"
        type="text"
        inputmode="url"
        placeholder="Paste a link or domain"
        class="flex-1"
      />
      <input
        id="newSourceCookiesInput"
        type="file"
        accept=".txt,.cookies,text/plain"
        class="hidden"
        @change="onNewCookieFile"
      />
      <Button
        variant="primary"
        type="button"
        class="shrink-0"
        title="Upload cookies for link"
        aria-label="Upload cookies for link"
        @click="openNewPicker"
      >
        <template #icon>
          <IconUpload class="w-5 h-5" aria-hidden="true" />
        </template>
      </Button>
    </div>
  </div>

  <p
    v-if="editableSourceProfiles.length === 0"
    class="mt-3 rounded-lg glass p-4 text-white in-[.light-mode]:text-black"
  >
    No sources yet.
  </p>

  <SettingsRow
    v-for="site in editableSourceProfiles"
    :key="site.key"
    :label="site.label"
  >
    <div class="flex items-center gap-2">
      <input
        :id="`${site.key}CookiesInput`"
        type="file"
        accept=".txt,.cookies,text/plain"
        class="hidden"
        @change="onCookieFile(site.key, $event)"
      />
      <template v-if="!cookieStatuses[site.key]?.configured">
        <div
          class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft px-3 text-sm"
        >
          <span class="truncate text-white/60 in-[.light-mode]:text-black/60">{{
            cookieFiles[site.key]?.name || "No cookies file"
          }}</span>
        </div>
        <Button
          variant="primary"
          type="button"
          class="shrink-0"
          :title="`Upload ${site.label} cookies`"
          :aria-label="`Upload ${site.label} cookies`"
          @click="openPicker(site.key)"
        >
          <template #icon>
            <IconUpload class="w-5 h-5" aria-hidden="true" />
          </template>
        </Button>
      </template>
      <template v-else>
        <div
          class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft px-3 text-sm"
        >
          <span class="truncate text-white in-[.light-mode]:text-black">{{
            cookieStatuses[site.key].filename || "cookies.txt"
          }}</span>
        </div>
        <Button
          variant="primary"
          type="button"
          class="shrink-0"
          :title="`Delete ${site.label} cookies`"
          :aria-label="`Delete ${site.label} cookies`"
          @click="removeCookies(site.key)"
        >
          <template #icon>
            <IconTrash class="w-5 h-5" aria-hidden="true" />
          </template>
        </Button>
      </template>
    </div>
  </SettingsRow>
</template>
