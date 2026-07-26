<script setup lang="ts">
import { reactive } from "vue";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";

import { Button } from "../../../../components/ui/button";
import { Input } from "../../../../components/ui/input";
import { useSettingsContext } from "../../context";
import SettingsEmptyCard from "../../SettingsEmptyCard.vue";
import SettingsGroup from "../../SettingsGroup.vue";
import SettingsRow from "../../SettingsRow.vue";
import SettingsSection from "../../SettingsSection.vue";

const {
  editableSourceProfiles,
  cookieStatuses,
  connectCookies,
  connectCookiesSource,
  removeCookies,
} = useSettingsContext();

const newCookie = reactive<{ source: string; file: File | null }>({
  source: "",
  file: null,
});

function hasCookies(siteKey: string): boolean {
  return Boolean(cookieStatuses.value[siteKey]?.configured);
}

function cookieLabel(siteKey: string): string {
  return cookieStatuses.value[siteKey]?.filename || "cookies.txt";
}

function onCookieFile(site: string, event: Event): void {
  const file = (event.target as HTMLInputElement).files?.[0] || null;
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
  <SettingsSection>
    <SettingsGroup>
      <div class="flex w-full items-center gap-2">
        <Input
          v-model="newCookie.source"
          data-settings-system
          type="text"
          inputmode="url"
          placeholder="Paste a link"
          class="flex-1"
        />
        <input
          id="newSourceCookiesInput"
          data-settings-system
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

      <SettingsEmptyCard v-if="editableSourceProfiles.length === 0">
        No sources yet.
      </SettingsEmptyCard>
    </SettingsGroup>

    <SettingsGroup v-if="editableSourceProfiles.length">
      <SettingsRow
        v-for="site in editableSourceProfiles"
        :key="site.key"
        :label="site.label"
      >
        <div class="flex items-center gap-2">
          <input
            :id="`${site.key}CookiesInput`"
            data-settings-system
            type="file"
            accept=".txt,.cookies,text/plain"
            class="hidden"
            @change="onCookieFile(site.key, $event)"
          />
          <template v-if="!hasCookies(site.key)">
            <Input model-value="No cookies file" disabled class="flex-1" />
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
            <Input
              :model-value="cookieLabel(site.key)"
              disabled
              class="flex-1"
              input-class="font-mono"
            />
            <Button
              variant="danger"
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
    </SettingsGroup>
  </SettingsSection>
</template>
