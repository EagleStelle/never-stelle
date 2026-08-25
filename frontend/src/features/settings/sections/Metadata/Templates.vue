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
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useSettingsContext } from "@/features/settings/context";
import {
  TEMPLATE_FIELDS,
  templateFieldSlug,
  type TemplateFieldDef,
} from "@/features/settings/templateFields";
import type { TemplateSettings } from "@/types";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
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

function inputId(siteKey: string, format: string, field: TemplateFieldDef): string {
  const cleanFmt = format ? "-" + format.replace(/[^a-zA-Z0-9]/g, "") : "";
  return `${siteKey}-${templateFieldSlug(field)}-template${cleanFmt}`;
}

// Unset, a format follows the global template from the Defaults pane.
function defaultTemplate(field: TemplateFieldDef): string {
  return settingsDraft.template_settings[field.key] || field.builtin;
}

function getTemplate(
  siteKey: string,
  format: string,
  field: TemplateFieldDef,
): string {
  return (
    settingsDraft.source_templates[siteKey]?.[format]?.[field.key] ??
    defaultTemplate(field)
  );
}

function setTemplate(
  siteKey: string,
  format: string,
  field: TemplateFieldDef,
  val: string,
): void {
  ensureFormatInitialized(siteKey, format);
  settingsDraft.source_templates[siteKey][format][field.key] = val;
  pruneUnsetFormat(siteKey, format);
}

function pruneUnsetFormat(siteKey: string, format: string): void {
  if (settings.source_templates[siteKey]?.[format]) return;
  const entry = settingsDraft.source_templates[siteKey]?.[format];
  if (!entry) return;
  const unset = TEMPLATE_FIELDS.every(
    (field) =>
      !entry[field.key] || entry[field.key] === defaultTemplate(field),
  );
  if (unset) delete settingsDraft.source_templates[siteKey][format];
}

function ensureFormatInitialized(siteKey: string, format: string): void {
  if (!settingsDraft.source_templates[siteKey]) {
    settingsDraft.source_templates[siteKey] = {};
  }
  if (!settingsDraft.source_templates[siteKey][format]) {
    const entry = {} as TemplateSettings;
    for (const field of TEMPLATE_FIELDS) entry[field.key] = defaultTemplate(field);
    settingsDraft.source_templates[siteKey][format] = entry;
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

  const field = TEMPLATE_FIELDS.find(
    (candidate) => inputId(siteKey, format, candidate) === targetId,
  );
  if (!field) return;

  const current = getTemplate(siteKey, format, field);
  const el = document.getElementById(targetId) as HTMLInputElement | null;
  const start = el?.selectionStart ?? current.length;
  const end = el?.selectionEnd ?? current.length;
  const text = braces(token);
  setTemplate(
    siteKey,
    format,
    field,
    current.slice(0, start) + text + current.slice(end),
  );

  void nextTick(() => {
    const caret = start + text.length;
    el?.focus();
    el?.setSelectionRange(caret, caret);
  });
}
</script>

<template>
  <TooltipProvider>
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
                <Field v-for="field in TEMPLATE_FIELDS" :key="field.key">
                  <FieldLabel
                    :for="inputId(site.key, template, field)"
                    class="items-center gap-1.5"
                  >
                    <span>{{ field.label }}</span>
                    <Tooltip v-if="field.help">
                      <TooltipTrigger as-child>
                        <button
                          type="button"
                          class="inline-flex h-4 w-4 items-center justify-center rounded-full border border-(--glass-border) bg-black/20 text-[0.625rem] font-semibold leading-none text-muted-foreground transition-all duration-300 ease-glass hover:border-accent hover:text-white focus-visible:ring-2 focus-visible:ring-accent in-[.light-mode]:bg-white/40 in-[.light-mode]:hover:text-black"
                          :aria-label="`${field.label} help`"
                        >
                          i
                        </button>
                      </TooltipTrigger>
                      <TooltipContent side="top">
                        {{ field.help }}
                      </TooltipContent>
                    </Tooltip>
                  </FieldLabel>
                  <FieldContent>
                    <Input
                      :id="inputId(site.key, template, field)"
                      :model-value="getTemplate(site.key, template, field)"
                      :placeholder="defaultTemplate(field)"
                      @focus="recordFocus(inputId(site.key, template, field))"
                      @update:model-value="
                        (v) => setTemplate(site.key, template, field, String(v))
                      "
                    />
                  </FieldContent>
                </Field>
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
  </TooltipProvider>
</template>
