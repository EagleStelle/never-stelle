<script setup lang="ts">
import { useTemplateRef } from "vue";
import IconApi from "~icons/material-symbols/api";
import IconCopy from "~icons/material-symbols/content-copy-rounded";
import IconLogin from "~icons/material-symbols/password";
import IconRegenerate from "~icons/material-symbols/autorenew";

import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Button } from "@/components/ui/button";
import {
  Field,
  FieldContent,
  FieldGroup,
  FieldLabel,
  FieldSeparator,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { useSonner } from "@/composables/useSonner";
import { useSettingsContext } from "@/features/settings/context";
import { copyText } from "@/utils/clipboard";

const { settings, settingsDraft, saveSettingsDraft } = useSettingsContext();
const { toast } = useSonner();
const form = useTemplateRef<HTMLFormElement>("form");

// Enter in any field saves, same as the footer button.
function submit() {
  void saveSettingsDraft().catch(() => {});
}

// Same shape as the keys the server generates.
function regenerateKey(): void {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  settingsDraft.security.api_key = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function copyKey(): Promise<void> {
  if (form.value && (await copyText(settingsDraft.security.api_key, form.value))) {
    toast("Key copied.");
  } else {
    toast("Could not copy.", "error");
  }
}
</script>

<template>
  <!-- Credential inputs need a form ancestor so browsers offer to save them. -->
  <form ref="form" aria-label="Security" @submit.prevent="submit">
    <Accordion type="multiple" :default-value="['login', 'api']" class="w-full">
      <AccordionItem value="login">
        <AccordionTrigger :icon="IconLogin">Login</AccordionTrigger>
        <AccordionContent>
          <FieldGroup>
            <Field>
              <FieldLabel for="securityUsernameInput">Username</FieldLabel>
              <FieldContent>
                <Input
                  id="securityUsernameInput"
                  v-model="settingsDraft.security.username"
                  data-settings-system
                  type="text"
                  autocomplete="username"
                  placeholder="Username"
                />
              </FieldContent>
            </Field>

            <Field>
              <FieldLabel for="securityNewPasswordInput">New Password</FieldLabel>
              <FieldContent>
                <Input
                  id="securityNewPasswordInput"
                  v-model="settingsDraft.security.new_password"
                  data-settings-system
                  type="password"
                  autocomplete="new-password"
                  placeholder="New password"
                />
              </FieldContent>
            </Field>

            <Field>
              <FieldLabel for="securityConfirmPasswordInput">Confirm Password</FieldLabel>
              <FieldContent>
                <Input
                  id="securityConfirmPasswordInput"
                  v-model="settingsDraft.security.confirm_password"
                  data-settings-system
                  type="password"
                  autocomplete="new-password"
                  placeholder="Confirm password"
                />
              </FieldContent>
            </Field>

            <FieldSeparator />

            <Field>
              <FieldLabel for="securityCurrentPasswordInput">Current Password</FieldLabel>
              <FieldContent>
                <Input
                  id="securityCurrentPasswordInput"
                  v-model="settingsDraft.security.current_password"
                  data-settings-system
                  type="password"
                  autocomplete="current-password"
                  placeholder="Current password"
                />
              </FieldContent>
            </Field>
          </FieldGroup>
        </AccordionContent>
      </AccordionItem>

      <AccordionItem value="api">
        <AccordionTrigger :icon="IconApi">API</AccordionTrigger>
        <AccordionContent>
          <FieldGroup>
            <Field>
              <FieldLabel for="securityApiKeyInput">API Key</FieldLabel>
              <!-- Phones give the key its own row so all of it stays visible. -->
              <FieldContent class="flex-row flex-wrap items-center justify-end gap-2 sm:flex-nowrap">
                <Input
                  id="securityApiKeyInput"
                  v-model="settingsDraft.security.api_key"
                  data-settings-system
                  class="min-w-0 basis-full font-mono sm:basis-0 sm:flex-1"
                  type="text"
                  autocomplete="off"
                  spellcheck="false"
                  :disabled="settings.auth.api_key_from_env"
                />
                <Button
                  variant="ghost"
                  size="icon"
                  type="button"
                  title="Copy key"
                  aria-label="Copy key"
                  :disabled="!settingsDraft.security.api_key"
                  @click="copyKey"
                >
                  <template #icon>
                    <IconCopy aria-hidden="true" />
                  </template>
                </Button>
                <Button
                  variant="secondary"
                  type="button"
                  title="Make a new key"
                  aria-label="Regenerate key"
                  :disabled="settings.auth.api_key_from_env"
                  @click="regenerateKey"
                >
                  <template #icon>
                    <IconRegenerate aria-hidden="true" />
                  </template>
                  Regenerate
                </Button>
              </FieldContent>
            </Field>
          </FieldGroup>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </form>
</template>
