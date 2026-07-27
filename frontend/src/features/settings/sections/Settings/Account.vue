<script setup lang="ts">
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { useSettingsContext } from "@/features/settings/context";

const { settingsDraft, saveSettingsDraft } = useSettingsContext();
</script>

<template>
  <form class="flex flex-col gap-4 py-2" @submit.prevent="saveSettingsDraft">
    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
      <Label for="accountUsernameInput" class="sm:w-40 sm:shrink-0">Username</Label>
      <div class="w-full sm:flex-auto">
        <Input
          id="accountUsernameInput"
          v-model="settingsDraft.account.username"
          data-settings-system
          type="text"
          autocomplete="username"
          placeholder="Username"
        />
      </div>
    </div>

    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
      <Label for="accountCurrentPasswordInput" class="sm:w-40 sm:shrink-0">Current Password</Label>
      <div class="w-full sm:flex-auto">
        <Input
          id="accountCurrentPasswordInput"
          v-model="settingsDraft.account.current_password"
          data-settings-system
          type="password"
          autocomplete="current-password"
          placeholder="Current password"
        />
      </div>
    </div>

    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
      <Label for="accountNewPasswordInput" class="sm:w-40 sm:shrink-0">New Password</Label>
      <div class="w-full sm:flex-auto">
        <Input
          id="accountNewPasswordInput"
          v-model="settingsDraft.account.new_password"
          data-settings-system
          type="password"
          autocomplete="new-password"
          placeholder="New password"
        />
      </div>
    </div>

    <Separator class="my-1" />

    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
      <Label for="accountConfirmPasswordInput" class="sm:w-40 sm:shrink-0">Confirm Password</Label>
      <div class="w-full sm:flex-auto">
        <Input
          id="accountConfirmPasswordInput"
          v-model="settingsDraft.account.confirm_password"
          data-settings-system
          type="password"
          autocomplete="new-password"
          placeholder="Confirm password"
        />
      </div>
    </div>
  </form>
</template>
