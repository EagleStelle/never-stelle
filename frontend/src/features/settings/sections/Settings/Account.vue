<script setup lang="ts">
import {
  Field,
  FieldContent,
  FieldGroup,
  FieldLabel,
  FieldSeparator,
  FieldSet,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { useSettingsContext } from "@/features/settings/context";

const { settingsDraft } = useSettingsContext();
</script>

<template>
  <FieldSet>
    <FieldGroup>
      <Field>
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

      <Field>
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

      <Field>
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

      <FieldSeparator />

      <Field>
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
    </FieldGroup>
  </FieldSet>
</template>
