<script setup lang="ts">
import { computed, ref, nextTick } from "vue";
import IconInfo from "~icons/material-symbols/info-outline";
import IconResolve from "~icons/material-symbols/cloud-sync";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Field, FieldContent, FieldLabel, FieldLegend } from "@/components/ui/field";
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
  sourceIconUrl,
  templateParts,
} from "@/utils/dashboard";

const {
  settings,
  settingsDraft,
  learnedFormatsDraft,
  editableSourceProfiles,
  renameCount,
  renameRunning,
  openRename,
} = useSettingsContext();

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
        <AccordionTrigger :image="sourceIconUrl(site.key)">
          {{ site.label }}
        </AccordionTrigger>

        <AccordionContent>
          <div class="flex flex-col gap-6">
            <!-- Links no learned format matches follow the default templates. -->
            <div
              v-if="!formatsFor(site.key).length || renameCount(site.key, 'templates')"
              class="flex flex-wrap items-center justify-between gap-3"
            >
              <p class="flex min-w-0 flex-1 items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground">
                <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
                {{
                  formatsFor(site.key).length
                    ? "Links outside these formats use the default templates."
                    : "Download once from this source to learn its URL format, then customize its templates."
                }}
              </p>
              <Button
                compact
                variant="secondary"
                class="shrink-0"
                size="sm"
                type="button"
                title="Resolve these files"
                :disabled="!renameCount(site.key, 'templates') || renameRunning(site.key, 'templates')"
                :aria-busy="renameRunning(site.key, 'templates')"
                @click="openRename(site.key, site.label, 'templates')"
              >
                <template #icon>
                  <IconResolve
                    aria-hidden="true"
                    :class="{ 'animate-spin': renameRunning(site.key, 'templates') }"
                  />
                </template>
                Resolve History
              </Button>
            </div>

            <div v-if="formatsFor(site.key).length" class="flex flex-col gap-10">
              <section
                v-for="template in formatsFor(site.key)"
                :key="template"
                class="flex flex-col gap-4"
                :aria-label="displayUrlTemplate(template)"
              >
                <FieldLegend as="div" variant="divider" class="mb-0">
                  <span class="min-w-0 wrap-anywhere">
                    <span
                      v-for="(part, index) in templateParts(displayUrlTemplate(template))"
                      :key="index"
                      :class="part.token && 'text-accent-ink'"
                      >{{ part.text }}</span
                    >
                  </span>
                  <template #end>
                    <Button
                      compact
                      variant="secondary"
                      class="shrink-0"
                      size="sm"
                      type="button"
                      title="Resolve this format"
                      :disabled="!renameCount(site.key, 'templates', template) || renameRunning(site.key, 'templates', template)"
                      :aria-busy="renameRunning(site.key, 'templates', template)"
                      @click="openRename(site.key, site.label, 'templates', template)"
                    >
                      <template #icon>
                        <IconResolve
                          aria-hidden="true"
                          :class="{ 'animate-spin': renameRunning(site.key, 'templates', template) }"
                        />
                      </template>
                      Resolve History
                    </Button>
                  </template>
                </FieldLegend>

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
                          class="-m-1 inline-flex size-6 items-center justify-center rounded-md text-muted-foreground transition-colors duration-200 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
                          :aria-label="`${field.label} help`"
                        >
                          <IconInfo class="size-4" aria-hidden="true" />
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

                <div
                  v-if="baseTokens.length || customTokensFor(site.key).length"
                  class="flex flex-wrap gap-1.5 sm:pl-43"
                >
                  <Button
                    v-for="token in baseTokens"
                    :key="token.key"
                    variant="outline"
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
                    variant="outline"
                    size="sm"
                    type="button"
                    class="font-mono text-[0.8125rem]"
                    title="Custom token"
                    @mousedown.prevent
                    @click="insert(site.key, template, token)"
                  >
                    {{ token }}
                  </Button>
                </div>
              </section>
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </TooltipProvider>
</template>
