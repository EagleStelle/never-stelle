<script setup lang="ts">
import IconSave from "~icons/material-symbols/save";

import { Button } from "../../../../components/ui/button";
import { Input } from "../../../../components/ui/input";
import { useCredentials } from "../../composables/useCredentials";
import { useSettingsContext } from "../../context";
import SettingsRow from "../../SettingsRow.vue";

const { open, settings } = useSettingsContext();
const { auth, form, saving, isChanged, save } = useCredentials(open, settings);
</script>

<template>
  <form class="flex flex-col" @submit.prevent="save">
    <SettingsRow label="Username">
      <Input
        id="accountUsernameInput"
        v-model="form.username"
        type="text"
        autocomplete="username"
        placeholder="Username"
      />
    </SettingsRow>
    <SettingsRow label="Current Password">
      <Input
        v-model="form.current_password"
        type="password"
        autocomplete="current-password"
        placeholder="Current password"
      />
    </SettingsRow>
    <SettingsRow label="New Password">
      <Input
        v-model="form.new_password"
        type="password"
        autocomplete="new-password"
        placeholder="New password"
      />
    </SettingsRow>
    <SettingsRow label="Confirm Password">
      <Input
        v-model="form.confirm_password"
        type="password"
        autocomplete="new-password"
        placeholder="Confirm password"
      />
    </SettingsRow>

    <div class="mt-4 flex flex-wrap gap-2">
      <Button
        v-if="isChanged"
        variant="primary"
        type="submit"
        :disabled="saving || auth.loading.value"
      >
        <template #icon>
          <IconSave class="h-5 w-5" aria-hidden="true" />
        </template>
        {{ saving ? "Saving..." : "Save" }}
      </Button>
    </div>
  </form>
</template>
