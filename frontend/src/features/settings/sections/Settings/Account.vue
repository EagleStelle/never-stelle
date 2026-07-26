<script setup lang="ts">
import { FieldGroup, FieldSet } from "../../../../components/ui/field";
import { Input } from "../../../../components/ui/input";
import { Label } from "../../../../components/ui/label";
import { useSettingsContext } from "../../context";

const { settingsDraft, saveSettingsDraft } = useSettingsContext();
</script>

<template>
  <form @submit.prevent="saveSettingsDraft">
    <FieldSet>
      <FieldGroup class="gap-4">
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label for="accountUsernameInput" class="sm:w-40 sm:shrink-0">
            Username
          </Label>
          <div class="w-full sm:flex-auto">
            <Input
              id="accountUsernameInput"
              v-model="settingsDraft.account.username"
              type="text"
              autocomplete="username"
              placeholder="Username"
            />
          </div>
        </div>
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label class="sm:w-40 sm:shrink-0">Current Password</Label>
          <div class="w-full sm:flex-auto">
            <Input
              v-model="settingsDraft.account.current_password"
              type="password"
              autocomplete="current-password"
              placeholder="Current password"
            />
          </div>
        </div>
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label class="sm:w-40 sm:shrink-0">New Password</Label>
          <div class="w-full sm:flex-auto">
            <Input
              v-model="settingsDraft.account.new_password"
              type="password"
              autocomplete="new-password"
              placeholder="New password"
            />
          </div>
        </div>
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label class="sm:w-40 sm:shrink-0">Confirm Password</Label>
          <div class="w-full sm:flex-auto">
            <Input
              v-model="settingsDraft.account.confirm_password"
              type="password"
              autocomplete="new-password"
              placeholder="Confirm password"
            />
          </div>
        </div>
      </FieldGroup>
    </FieldSet>
  </form>
</template>
