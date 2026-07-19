<script setup lang="ts">
import { reactive } from "vue";
import IconAdd from "~icons/material-symbols/add";
import IconDown from "~icons/material-symbols/arrow-downward";
import IconUp from "~icons/material-symbols/arrow-upward";
import IconClose from "~icons/material-symbols/close";
import IconSearch from "~icons/material-symbols/search";
import IconSpinner from "~icons/material-symbols/sync";

import { Button } from "../../../../components/ui/button";
import { Checkbox } from "../../../../components/ui/checkbox";
import { Input } from "../../../../components/ui/input";
import {
  useFieldsSettings,
  type CreatorRole,
} from "../../composables/useFieldsSettings";
import { useSettingsContext } from "../../context";

const ROLES: { key: CreatorRole; label: string }[] = [
  { key: "username", label: "Username" },
  { key: "nickname", label: "Nickname" },
];

const { settings, settingsDraft, editableSourceProfiles } =
  useSettingsContext();
const {
  probes,
  rules,
  suggestions,
  fieldList,
  addField,
  removeField,
  moveField,
  ruleEnabled,
  setRule,
  maxChars,
  setMaxChars,
  runProbe,
} = useFieldsSettings(settingsDraft, settings, editableSourceProfiles);

// Per (source, role) draft text for the manual add box.
const newField = reactive<Record<string, string>>({});
function draftKey(key: string, role: CreatorRole): string {
  return `${key}:${role}`;
}
function submitField(key: string, role: CreatorRole): void {
  addField(key, role, newField[draftKey(key, role)] || "");
  newField[draftKey(key, role)] = "";
}
</script>

<template>
  <div
    v-for="site in editableSourceProfiles"
    :key="site.key"
    class="flex flex-col gap-[0.85rem] p-[0.85rem] mb-3 rounded-xl border border-(--glass-border)"
  >
    <span class="text-sm font-medium">{{ site.label }}</span>

    <!-- Field probe -->
    <div class="flex flex-col gap-2 mb-2">
      <div class="flex items-center gap-2">
        <Input
          :id="`${site.key}FieldsProbeInput`"
          v-model="probes[site.key].url"
          type="text"
          inputmode="url"
          placeholder="Paste a link to see which field is which"
          class="flex-1"
          @keydown.enter.prevent="runProbe(site.key)"
        />
        <Button
          class="shrink-0"
          variant="primary"
          type="button"
          aria-label="Test"
          title="Test"
          :disabled="probes[site.key].loading"
          :aria-busy="probes[site.key].loading"
          @click="runProbe(site.key)"
        >
          <template #icon>
            <IconSpinner
              v-if="probes[site.key].loading"
              class="w-4 h-4 animate-spin"
              aria-hidden="true"
            />
            <IconSearch v-else class="w-4 h-4" aria-hidden="true" />
          </template>
        </Button>
      </div>

      <p
        v-if="probes[site.key].message"
        class="text-xs text-white/60 in-[.light-mode]:text-black/60"
      >
        {{ probes[site.key].message }}
      </p>

      <ul
        v-if="probes[site.key].fields.length"
        class="flex flex-col gap-1 text-[0.8125rem]"
      >
          <li
            v-for="result in probes[site.key].fields"
            :key="result.field"
            class="flex items-center gap-2 min-w-0"
          >
            <span
              class="font-mono shrink-0 w-40 truncate"
              :title="result.field"
              >{{ result.field }}</span
            >
            <span class="min-w-0 flex-1 wrap-anywhere">{{ result.value }}</span>
            <Button
              size="xs"
              type="button"
              title="Use as username"
              @click="addField(site.key, 'username', result.field)"
            >
              → user
            </Button>
            <Button
              size="xs"
              type="button"
              title="Use as nickname"
              @click="addField(site.key, 'nickname', result.field)"
            >
              → nick
            </Button>
          </li>
      </ul>
    </div>

    <!-- Username and nickname field lists -->
    <div
      v-for="role in ROLES"
      :key="role.key"
      class="flex flex-col gap-2"
    >
      <span class="text-xs font-medium">{{ role.label }}</span>
      <div
        v-for="(field, index) in fieldList(site.key, role.key)"
        :key="field"
        class="flex items-center gap-2"
      >
        <span class="font-mono text-[0.8125rem] flex-1 min-w-0 truncate"
          >{{ index + 1 }}. {{ field }}</span
        >
        <Button
          size="xs"
          variant="soft"
          type="button"
          title="Move up"
          :disabled="index === 0"
          @click="moveField(site.key, role.key, index, -1)"
        >
          <template #icon
            ><IconUp class="w-4 h-4" aria-hidden="true"
          /></template>
        </Button>
        <Button
          size="xs"
          variant="soft"
          type="button"
          title="Move down"
          :disabled="index === fieldList(site.key, role.key).length - 1"
          @click="moveField(site.key, role.key, index, 1)"
        >
          <template #icon
            ><IconDown class="w-4 h-4" aria-hidden="true"
          /></template>
        </Button>
        <Button
          size="xs"
          variant="soft"
          type="button"
          title="Remove"
          @click="removeField(site.key, role.key, index)"
        >
          <template #icon
            ><IconClose class="w-4 h-4" aria-hidden="true"
          /></template>
        </Button>
      </div>
      <div class="flex items-center gap-2">
        <Input
          v-model="newField[draftKey(site.key, role.key)]"
          type="text"
          placeholder="Add a field (e.g. uploader_id)"
          input-class="font-mono"
          class="flex-1"
          @keydown.enter.prevent="submitField(site.key, role.key)"
        />
        <Button
          variant="soft"
          type="button"
          title="Add field"
          @click="submitField(site.key, role.key)"
        >
          <template #icon
            ><IconAdd class="w-4 h-4" aria-hidden="true"
          /></template>
        </Button>
      </div>
      <div v-if="suggestions.length" class="flex flex-wrap gap-1.5">
        <Button
          v-for="item in suggestions"
          :key="item.field"
          size="xs"
          type="button"
          class="font-mono"
          :title="item.label"
          @click="addField(site.key, role.key, item.field)"
        >
          {{ item.field }}
        </Button>
      </div>
    </div>

    <!-- Title cleaning -->
    <div
      class="flex flex-col gap-2"
    >
      <span class="text-xs font-medium">Title cleaning</span>
      <label
        v-for="rule in rules"
        :key="rule.key"
        class="flex items-center gap-2 cursor-pointer select-none text-sm"
      >
        <Checkbox
          :checked="ruleEnabled(site.key, rule)"
          @update:checked="(v: boolean) => setRule(site.key, rule, Boolean(v))"
        />
        <span>{{ rule.label }}</span>
      </label>
      <label
        v-if="
          ruleEnabled(site.key, { key: 'shorten', label: '', default: true })
        "
        class="flex items-center gap-2 text-sm mt-1"
      >
        <span class="shrink-0">Max title length</span>
        <Input
          type="number"
          :model-value="String(maxChars(site.key))"
          class="w-24"
          @update:model-value="(v: string) => setMaxChars(site.key, Number(v))"
        />
      </label>
    </div>
  </div>
</template>
