<script setup lang="ts">
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { useSettingsContext } from "@/features/settings/context";

const { settingsDraft, saveSettingsDraft } = useSettingsContext();
</script>

<template>
  <form class="flex flex-col gap-4 py-2" @submit.prevent="saveSettingsDraft">
    <Field orientation="start">
      <FieldLabel for="accountUsernameInput">Username</FieldLabel>
      <FieldContent>
        <Input
          id="accountUsernameInput"
          v-model="settingsDraft.account.username"
          data-settings-system
          type="text"
          autocomplete="username"
          placeholder="Username"
        />
      </FieldContent>
    </Field>

    <Field orientation="start">
      <FieldLabel for="accountCurrentPasswordInput">Current Password</FieldLabel>
      <FieldContent>
        <Input
          id="accountCurrentPasswordInput"
          v-model="settingsDraft.account.current_password"
          data-settings-system
          type="password"
          autocomplete="current-password"
          placeholder="Current password"
        />
      </FieldContent>
    </Field>

    <Field orientation="start">
      <FieldLabel for="accountNewPasswordInput">New Password</FieldLabel>
      <FieldContent>
        <Input
          id="accountNewPasswordInput"
          v-model="settingsDraft.account.new_password"
          data-settings-system
          type="password"
          autocomplete="new-password"
          placeholder="New password"
        />
      </FieldContent>
    </Field>

    <Separator class="my-1" />

    <Field orientation="start">
      <FieldLabel for="accountConfirmPasswordInput">Confirm Password</FieldLabel>
      <FieldContent>
        <Input
          id="accountConfirmPasswordInput"
          v-model="settingsDraft.account.confirm_password"
          data-settings-system
          type="password"
          autocomplete="new-password"
          placeholder="Confirm password"
        />
      </FieldContent>
    </Field>
  </form>
</template>
