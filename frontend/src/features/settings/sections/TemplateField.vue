<script setup lang="ts">
import { computed, nextTick, ref } from "vue";

import { Button } from "../../../components/ui/button";
import { Input } from "../../../components/ui/input";
import type { SourceProfile } from "../../../types";
import { normalizeTokenName } from "../../../utils/dashboard";
import { useSettingsContext } from "../context";

const props = defineProps<{
  site: SourceProfile;
  field: "folder_template" | "filename_template";
  idSuffix: string;
}>();

const { settings, settingsDraft } = useSettingsContext();

const inputId = computed(() => `${props.site.key}${props.idSuffix}`);

// Reveal the palette only while focus is within this field (input or its chips).
const focused = ref(false);
function onFocusOut(event: FocusEvent): void {
  const root = event.currentTarget as HTMLElement;
  if (!root.contains(event.relatedTarget as Node | null)) focused.value = false;
}

// Base tokens come from the server. Scraper rules set to "None" keep the old
// custom-token behavior, while role-backed rules are represented by public tokens.
const baseTokens = computed(() => settings.template_tokens || []);
const customTokens = computed(() => {
  const seen = new Set(baseTokens.value.map((token) => token.key));
  const roles = settingsDraft.source_token_roles[props.site.key] || {};
  const out: string[] = [];
  for (const rule of settingsDraft.source_scrape_rules[props.site.key]?.rules || []) {
    const key = normalizeTokenName(rule.token);
    const role = key ? roles[key] : "";
    if (key && (!role || role === "ignore") && !seen.has(key)) {
      seen.add(key);
      out.push(key);
    }
  }
  return out;
});

function braces(key: string): string {
  return `{{${key}}}`;
}

// Splice the token at the caret so users build templates by clicking, not typing braces.
function insert(token: string): void {
  const el = document.getElementById(inputId.value) as HTMLInputElement | null;
  const draft = settingsDraft.source_templates[props.site.key];
  const current = draft[props.field] || "";
  const start = el?.selectionStart ?? current.length;
  const end = el?.selectionEnd ?? current.length;
  const text = braces(token);
  draft[props.field] = current.slice(0, start) + text + current.slice(end);
  void nextTick(() => {
    const caret = start + text.length;
    el?.focus();
    el?.setSelectionRange(caret, caret);
  });
}
</script>

<template>
  <div class="flex flex-col gap-2" @focusin="focused = true" @focusout="onFocusOut">
    <Input
      :id="inputId"
      v-model="settingsDraft.source_templates[site.key][field]"
      input-class="font-mono"
    />
    <div v-if="focused" class="flex flex-wrap gap-1.5">
      <Button
        v-for="token in baseTokens"
        :key="token.key"
        type="button"
        class="font-mono"
        :title="token.description"
        @mousedown.prevent
        @click="insert(token.key)"
      >
        {{ token.key }}
      </Button>
      <Button
        v-for="token in customTokens"
        :key="token"
        type="button"
        class="font-mono"
        title="Custom scrape token"
        @mousedown.prevent
        @click="insert(token)"
      >
        {{ token }}
      </Button>
    </div>
  </div>
</template>
