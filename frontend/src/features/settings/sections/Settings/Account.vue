<script setup lang="ts">
import { Input } from "../../../../components/ui/input";
import { useSettingsContext } from "../../context";
import SettingsGroup from "../../SettingsGroup.vue";
import SettingsRow from "../../SettingsRow.vue";

const { settingsDraft, saveSettingsDraft } = useSettingsContext();
</script>

<template>
  <form @submit.prevent="saveSettingsDraft">
    <SettingsGroup>
      <SettingsRow label="Username">
        <Input
          id="accountUsernameInput"
          v-model="settingsDraft.account.username"
          type="text"
          autocomplete="username"
          placeholder="Username"
        />
      </SettingsRow>
      <SettingsRow label="Current Password">
        <Input
          v-model="settingsDraft.account.current_password"
          type="password"
          autocomplete="current-password"
          placeholder="Current password"
        />
      </SettingsRow>
      <SettingsRow label="New Password">
        <Input
          v-model="settingsDraft.account.new_password"
          type="password"
          autocomplete="new-password"
          placeholder="New password"
        />
      </SettingsRow>
      <SettingsRow label="Confirm Password">
        <Input
          v-model="settingsDraft.account.confirm_password"
          type="password"
          autocomplete="new-password"
          placeholder="Confirm password"
        />
      </SettingsRow>
    </SettingsGroup>
  </form>
</template>
