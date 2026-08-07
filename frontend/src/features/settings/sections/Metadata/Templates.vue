<script setup lang="ts">
import { computed, ref, nextTick } from "vue";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardFooter,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { useSettingsContext } from "@/features/settings/context";
import {
  displayUrlTemplate,
  normalizeTokenName,
} from "@/utils/dashboard";

const { settings, settingsDraft, learnedFormatsDraft, editableSourceProfiles } =
  useSettingsContext();

function formatsFor(key: string): string[] {
  return (
    learnedFormatsDraft?.[key]?.templates ||
    settings.learned_formats?.[key]?.templates ||
    []
  );
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

// Unset, a format follows the global template from the Defaults pane.
function defaultFolderTemplate(): string {
  return settingsDraft.template_settings.folder_template || "{{username}}";
}

function defaultFilenameTemplate(): string {
  return (
    settingsDraft.template_settings.filename_template ||
    "{{username}} - {{title}} [{{id}}]"
  );
}

// Ref getters/setters for folder and filename templates
function getFolderTemplate(siteKey: string, format: string): string {
  return (
    settingsDraft.source_templates[siteKey]?.[format]?.folder_template ??
    defaultFolderTemplate()
  );
}

function setFolderTemplate(siteKey: string, format: string, val: string): void {
  ensureFormatInitialized(siteKey, format);
  settingsDraft.source_templates[siteKey][format].folder_template = val;
  pruneUnsetFormat(siteKey, format);
}

function getFilenameTemplate(siteKey: string, format: string): string {
  return (
    settingsDraft.source_templates[siteKey]?.[format]?.filename_template ??
    defaultFilenameTemplate()
  );
}

// Track if we need to initialize when filename is typed
function setFilenameTemplate(
  siteKey: string,
  format: string,
  val: string,
): void {
  ensureFormatInitialized(siteKey, format);
  settingsDraft.source_templates[siteKey][format].filename_template = val;
  pruneUnsetFormat(siteKey, format);
}

function pruneUnsetFormat(siteKey: string, format: string): void {
  if (settings.source_templates[siteKey]?.[format]) return;
  const entry = settingsDraft.source_templates[siteKey]?.[format];
  if (!entry) return;
  const folderUnset =
    !entry.folder_template || entry.folder_template === defaultFolderTemplate();
  const filenameUnset =
    !entry.filename_template ||
    entry.filename_template === defaultFilenameTemplate();
  if (folderUnset && filenameUnset) {
    delete settingsDraft.source_templates[siteKey][format];
  }
}

function ensureFormatInitialized(siteKey: string, format: string): void {
  if (!settingsDraft.source_templates[siteKey]) {
    settingsDraft.source_templates[siteKey] = {};
  }
  if (!settingsDraft.source_templates[siteKey][format]) {
    settingsDraft.source_templates[siteKey][format] = {
      folder_template: defaultFolderTemplate(),
      filename_template: defaultFilenameTemplate(),
    };
  }
}

// Token lists
const baseTokens = computed(() => settings.template_tokens || []);
function customTokensFor(siteKey: string) {
  const seen = new Set(baseTokens.value.map((token) => token.key));
  const roles = settingsDraft.source_token_roles[siteKey] || {};
  const out: string[] = [];

  function addNoneRoleToken(token: unknown): void {
    const key = normalizeTokenName(token);
    const role = key ? roles[key] : "";
    if (key && (!role || role === "ignore") && !seen.has(key)) {
      seen.add(key);
      out.push(key);
    }
  }

  for (const rule of settingsDraft.source_scrape_rules[siteKey]?.rules || []) {
    addNoneRoleToken(rule.token);
  }

  const configuredByPart = new Map(
    (settingsDraft.source_slug_tokens[siteKey] || []).map((entry) => [
      entry.part,
      normalizeTokenName(entry.token),
    ]),
  );
  const segments = (
    learnedFormatsDraft?.[siteKey]?.segments ||
    settings.learned_formats?.[siteKey]?.segments ||
    []
  ).filter((segment) => !segment.reserved);
  segments.forEach((segment, index) => {
    const token = configuredByPart.has(segment.part)
      ? configuredByPart.get(segment.part) || ""
      : `var${index}`;
    addNoneRoleToken(token);
  });

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

  const current = isFolder
    ? getFolderTemplate(siteKey, format)
    : getFilenameTemplate(siteKey, format);
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
        <Card v-if="!formatsFor(site.key).length" class="px-6">
          <p class="text-[0.8125rem] text-muted-foreground">
            Download once from this source to learn its URL format, then customize
            its templates.
          </p>
        </Card>

        <div v-else class="flex flex-col gap-[0.85rem]">
          <Card v-for="template in formatsFor(site.key)" :key="template">
            <CardHeader>
              <CardTitle class="font-mono text-sm leading-snug">
                {{ displayUrlTemplate(template) }}
              </CardTitle>
            </CardHeader>
            <CardContent class="flex flex-col gap-3">
              <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                <Label class="sm:w-40 sm:shrink-0">Folder</Label>
                <div class="w-full sm:flex-auto">
                  <Input
                    :id="getFolderInputId(site.key, template)"
                    :model-value="getFolderTemplate(site.key, template)"
                    :placeholder="defaultFolderTemplate()"
                    @focus="recordFocus(getFolderInputId(site.key, template))"
                    @update:model-value="
                      (v) => setFolderTemplate(site.key, template, String(v))
                    "
                  />
                </div>
              </div>

              <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                <Label class="sm:w-40 sm:shrink-0">Filename</Label>
                <div class="w-full sm:flex-auto">
                  <Input
                    :id="getFilenameInputId(site.key, template)"
                    :model-value="getFilenameTemplate(site.key, template)"
                    :placeholder="defaultFilenameTemplate()"
                    @focus="recordFocus(getFilenameInputId(site.key, template))"
                    @update:model-value="
                      (v) => setFilenameTemplate(site.key, template, String(v))
                    "
                  />
                </div>
              </div>
            </CardContent>
            <CardFooter
              v-if="baseTokens.length || customTokensFor(site.key).length"
              class="flex flex-wrap gap-1.5 pt-0 pb-1"
            >
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
                title="Custom token"
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
