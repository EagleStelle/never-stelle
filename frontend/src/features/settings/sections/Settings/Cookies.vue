<script setup lang="ts">
import { reactive } from "vue";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { FieldGroup, FieldSet } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useSettingsContext } from "@/features/settings/context";

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
  <div class="flex flex-col gap-6">
    <div class="flex flex-col gap-4">
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

      <Card v-if="editableSourceProfiles.length === 0" class="px-6 py-4">
        <p class="text-[0.8125rem] text-muted-foreground">
          No sources yet.
        </p>
      </Card>
    </div>

    <FieldSet v-if="editableSourceProfiles.length">
      <FieldGroup class="gap-4">
        <div
          v-for="site in editableSourceProfiles"
          :key="site.key"
          class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3"
        >
          <Label class="sm:w-40 sm:shrink-0">{{ site.label }}</Label>
          <div class="w-full sm:flex-auto">
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
          </div>
        </div>
      </FieldGroup>
    </FieldSet>
  </div>
</template>
