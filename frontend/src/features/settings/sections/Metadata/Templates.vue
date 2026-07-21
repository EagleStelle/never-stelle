<script setup lang="ts">
import { computed, ref, nextTick } from "vue";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "../../../../components/ui/accordion";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardFooter,
} from "../../../../components/ui/card";
import { Input } from "../../../../components/ui/input";
import { Button } from "../../../../components/ui/button";
import SettingsLabel from "../../SettingsLabel.vue";
import { useSettingsContext } from "../../context";
import { createTemplateSettings, normalizeTokenName } from "../../../../utils/dashboard";

const { settings, settingsDraft, editableSourceProfiles } = useSettingsContext();

function formatsFor(key: string): string[] {
  return settings.learned_formats?.[key]?.templates || [];
}

// Track the last focused input element ID so token clicks insert into it
const lastFocusedInputId = ref<string | null>(null);

function recordFocus(id: string): void {
  lastFocusedInputId.value = id;
}

// Clean input IDs for template inputs
function getFolderInputId(siteKey: string, format: string): string {
  const cleanFmt = format ? "-" + format.replace(/[^a-zA-Z0-9]/g, "") : "";
  return `${siteKey}-folder-template${cleanFmt}`;
}

function getFilenameInputId(siteKey: string, format: string): string {
  const cleanFmt = format ? "-" + format.replace(/[^a-zA-Z0-9]/g, "") : "";
  return `${siteKey}-filename-template${cleanFmt}`;
}

// Ref getters/setters for folder and filename templates
function getFolderTemplate(siteKey: string, format: string): string {
  return settingsDraft.source_templates[siteKey]?.[format]?.folder_template ?? "{{username}}";
}

function setFolderTemplate(siteKey: string, format: string, val: string): void {
  ensureFormatInitialized(siteKey, format);
  settingsDraft.source_templates[siteKey][format].folder_template = val;
}

function getFilenameTemplate(siteKey: string, format: string): string {
  return settingsDraft.source_templates[siteKey]?.[format]?.filename_template ?? "{{username}} - {{title}} [{{id}}]";
}

// Track if we need to initialize when filename is typed
function setFilenameTemplate(siteKey: string, format: string, val: string): void {
  ensureFormatInitialized(siteKey, format);
  settingsDraft.source_templates[siteKey][format].filename_template = val;
}

function ensureFormatInitialized(siteKey: string, format: string): void {
  if (!settingsDraft.source_templates[siteKey]) {
    settingsDraft.source_templates[siteKey] = {};
  }
  if (!settingsDraft.source_templates[siteKey][format]) {
    settingsDraft.source_templates[siteKey][format] = {
      folder_template: "{{username}}",
      filename_template: "{{username}} - {{title}} [{{id}}]"
    };
  }
}

// Token lists
const baseTokens = computed(() => settings.template_tokens || []);
function customTokensFor(siteKey: string) {
  const seen = new Set(baseTokens.value.map((token) => token.key));
  const roles = settingsDraft.source_token_roles[siteKey] || {};
  const out: string[] = [];
  for (const rule of settingsDraft.source_scrape_rules[siteKey]?.rules || []) {
    const key = normalizeTokenName(rule.token);
    const role = key ? roles[key] : "";
    if (key && (!role || role === "ignore") && !seen.has(key)) {
      seen.add(key);
      out.push(key);
    }
  }
  return out;
}

function braces(key: string): string {
  return `{{${key}}}`;
}

function insert(siteKey: string, format: string, token: string): void {
  const targetId = lastFocusedInputId.value;
  if (!targetId) return;

  // Determine if it's folder or filename template for this siteKey and format
  const isFolder = targetId === getFolderInputId(siteKey, format);
  const isFilename = targetId === getFilenameInputId(siteKey, format);
  if (!isFolder && !isFilename) return;

  const current = isFolder ? getFolderTemplate(siteKey, format) : getFilenameTemplate(siteKey, format);
  const el = document.getElementById(targetId) as HTMLInputElement | null;
  const start = el?.selectionStart ?? current.length;
  const end = el?.selectionEnd ?? current.length;
  const text = braces(token);
  const nextVal = current.slice(0, start) + text + current.slice(end);

  if (isFolder) {
    setFolderTemplate(siteKey, format, nextVal);
  } else {
    setFilenameTemplate(siteKey, format, nextVal);
  }

  void nextTick(() => {
    const caret = start + text.length;
    el?.focus();
    el?.setSelectionRange(caret, caret);
  });
}
</script>

<template>
  <Accordion type="multiple" class="w-full">
    <AccordionItem
      v-for="site in editableSourceProfiles"
      :key="site.key"
      :value="site.key"
    >
      <AccordionTrigger>
        {{ site.label }}
      </AccordionTrigger>

      <AccordionContent>
        <div
          v-if="!formatsFor(site.key).length"
          class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55"
        >
          Download once from this source to learn its URL format, then customize its templates.
        </div>

        <div v-else class="flex flex-col gap-[0.85rem]">
          <Card
            v-for="template in formatsFor(site.key)"
            :key="template"
          >
            <CardHeader>
              <CardTitle class="font-mono text-sm leading-snug wrap-anywhere">
                {{ template }}
              </CardTitle>
            </CardHeader>
            <CardContent class="flex flex-col gap-3">
              <label class="flex flex-col gap-1.5">
                <SettingsLabel>Folder</SettingsLabel>
                <Input
                  :id="getFolderInputId(site.key, template)"
                  :model-value="getFolderTemplate(site.key, template)"
                  input-class="font-mono"
                  placeholder="{{username}}"
                  @focus="recordFocus(getFolderInputId(site.key, template))"
                  @update:model-value="(v) => setFolderTemplate(site.key, template, String(v))"
                />
              </label>

              <label class="flex flex-col gap-1.5">
                <SettingsLabel>Filename</SettingsLabel>
                <Input
                  :id="getFilenameInputId(site.key, template)"
                  :model-value="getFilenameTemplate(site.key, template)"
                  input-class="font-mono"
                  placeholder="{{username}} - {{title}} [{{id}}]"
                  @focus="recordFocus(getFilenameInputId(site.key, template))"
                  @update:model-value="(v) => setFilenameTemplate(site.key, template, String(v))"
                />
              </label>
            </CardContent>
            <CardFooter v-if="baseTokens.length || customTokensFor(site.key).length" class="flex flex-wrap gap-1.5 pt-0 pb-1">
              <Button
                v-for="token in baseTokens"
                :key="token.key"
                variant="secondary"
                size="sm"
                type="button"
                class="font-mono text-[0.8125rem]"
                :title="token.description"
                @mousedown.prevent
                @click="insert(site.key, template, token.key)"
              >
                {{ token.key }}
              </Button>
              <Button
                v-for="token in customTokensFor(site.key)"
                :key="token"
                variant="secondary"
                size="sm"
                type="button"
                class="font-mono text-[0.8125rem]"
                title="Custom scrape token"
                @mousedown.prevent
                @click="insert(site.key, template, token)"
              >
                {{ token }}
              </Button>
            </CardFooter>
          </Card>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
