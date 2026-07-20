<script setup lang="ts">
import { reactive } from "vue";
import IconAdd from "~icons/material-symbols/add";
import IconChevron from "~icons/material-symbols/keyboard-arrow-down";
import IconSearch from "~icons/material-symbols/search";
import IconSpinner from "~icons/material-symbols/sync";
import IconTrash from "~icons/material-symbols/delete";

import { Button } from "../../../../components/ui/button";
import { Checkbox } from "../../../../components/ui/checkbox";
import { ComboboxSelect as Combobox } from "../../../../components/ui/combobox";
import { Input } from "../../../../components/ui/input";
import { Textarea } from "../../../../components/ui/textarea";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "../../../../components/ui/segmented-control";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../../../../components/ui/table";
import type { ScrapeRule, TokenRole } from "../../../../types";
import { normalizeTokenName } from "../../../../utils/dashboard";
import { useScrapeTests } from "../../composables/useScrapeTests";
import { useSettingsContext } from "../../context";
import SettingsRow from "../../SettingsRow.vue";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "../../../../components/ui/accordion";

const SCRAPE_ATTR_ITEMS = [
  { key: "text", label: "Text" },
  { key: "href", label: "Link (href)" },
  { key: "src", label: "src" },
  { key: "alt", label: "alt" },
  { key: "title", label: "title" },
];

const ROLE_ITEMS: { key: TokenRole; label: string }[] = [
  { key: "ignore", label: "None" },
  { key: "username", label: "Username" },
  { key: "nickname", label: "Nickname" },
  { key: "title", label: "Title" },
];

// Built in script so the literal braces never reach the template tokenizer.
function tokenLabel(token: string): string {
  return `{{${token}}}`;
}

function ruleTitle(rule: ScrapeRule, index: number): string {
  const token = normalizeTokenName(rule.token);
  return token ? tokenLabel(token) : `Rule ${index + 1}`;
}

function ruleSummary(rule: ScrapeRule): string {
  return rule.xpath || rule.selector || rule.match_label || "No selector yet";
}

const expanded = reactive<Record<string, boolean>>({});

function rowKey(siteKey: string, index: number): string {
  return `${siteKey}:${index}`;
}

function toggleRow(siteKey: string, index: number): void {
  const key = rowKey(siteKey, index);
  expanded[key] = !expanded[key];
}

function updateRole(siteKey: string, rule: ScrapeRule, value: unknown): void {
  const next = String(value || "ignore") as TokenRole;
  const role = ROLE_ITEMS.some((item) => item.key === next) ? next : "ignore";
  setTokenRole(siteKey, rule.token, role);
}

const { settingsDraft, editableSourceProfiles } = useSettingsContext();
const {
  scrapeTests,
  tokenRole,
  isTitleRoleDisabled,
  setTokenRole,
  addScrapeRule,
  removeScrapeRule,
  runScrapeTest,
} = useScrapeTests(settingsDraft, editableSourceProfiles);
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
        <div class="flex flex-col gap-[0.85rem]">
          <SettingsRow label="Probe URL">
            <div class="flex items-center gap-2 w-full">
              <Input
                :id="`${site.key}ScraperProbeInput`"
                v-model="scrapeTests[site.key].url"
                type="text"
                inputmode="url"
                placeholder="Paste a link"
                class="flex-1"
                @keydown.enter.prevent="runScrapeTest(site.key)"
              />
              <Button
                class="shrink-0"
                variant="primary"
                type="button"
                aria-label="Test"
                title="Test"
                :disabled="scrapeTests[site.key].loading"
                :aria-busy="scrapeTests[site.key].loading"
                @click="runScrapeTest(site.key)"
              >
                <template #icon>
                  <IconSpinner
                    v-if="scrapeTests[site.key].loading"
                    class="w-4 h-4 animate-spin"
                    aria-hidden="true"
                  />
                  <IconSearch v-else class="w-4 h-4" aria-hidden="true" />
                </template>
              </Button>
            </div>
          </SettingsRow>

          <p
            v-if="scrapeTests[site.key].message"
            class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55"
          >
            {{ scrapeTests[site.key].message }}
          </p>

          <Table
            v-if="scrapeTests[site.key].results.length"
            class="text-[0.8125rem]"
          >
            <TableHeader>
              <TableRow>
                <TableHead
                  class="w-48 text-[0.68rem] uppercase tracking-wider text-white/45 in-[.light-mode]:text-black/45"
                >
                  Token
                </TableHead>
                <TableHead
                  class="text-[0.68rem] uppercase tracking-wider text-white/45 in-[.light-mode]:text-black/45"
                >
                  Sample
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <TableRow
                v-for="result in scrapeTests[site.key].results"
                :key="result.token"
              >
                <TableCell class="w-48 max-w-48 font-mono">
                  <span class="block truncate" :title="tokenLabel(result.token)">
                    {{ tokenLabel(result.token) }}
                  </span>
                </TableCell>
                <TableCell>
                  <span
                    v-if="result.matched"
                    class="block min-w-0 wrap-anywhere"
                    :title="result.value"
                  >
                    {{ result.value }}
                  </span>
                  <span v-else class="opacity-50 italic">no match</span>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>

          <div class="flex flex-col gap-2">
            <div
              v-for="(rule, index) in settingsDraft.source_scrape_rules[
                site.key
              ].rules"
              :key="index"
              class="rounded-lg border border-(--glass-border) bg-black/10 in-[.light-mode]:bg-white/35"
            >
              <button
                type="button"
                class="flex w-full items-center gap-3 px-3 py-2 text-left"
                :aria-expanded="Boolean(expanded[rowKey(site.key, index)])"
                @click="toggleRow(site.key, index)"
              >
                <IconChevron
                  class="h-5 w-5 shrink-0 opacity-60 transition-transform"
                  :class="expanded[rowKey(site.key, index)] ? 'rotate-180' : ''"
                  aria-hidden="true"
                />
                <span class="min-w-0 flex-1">
                  <span class="block truncate font-mono text-[0.8125rem]">
                    {{ ruleTitle(rule, index) }}
                  </span>
                  <span
                    class="block truncate text-[0.75rem] text-white/50 in-[.light-mode]:text-black/50"
                  >
                    {{ ruleSummary(rule) }}
                  </span>
                </span>
              </button>

              <div
                v-if="expanded[rowKey(site.key, index)]"
                class="grid grid-cols-1 gap-3 border-t border-(--glass-border) px-3 py-3 lg:grid-cols-2"
              >
                <label class="flex flex-col gap-1.5">
                  <span class="text-xs uppercase tracking-wider opacity-55">
                    Token
                  </span>
                  <Input
                    v-model="rule.token"
                    aria-label="Token name"
                    input-class="font-mono"
                  />
                </label>

                <label class="flex flex-col gap-1.5">
                  <span class="text-xs uppercase tracking-wider opacity-55">
                    Label
                  </span>
                  <Input
                    v-model="rule.match_label"
                    aria-label="Label to anchor on"
                  />
                </label>

                <label class="flex flex-col gap-1.5">
                  <span class="text-xs uppercase tracking-wider opacity-55">
                    Selector
                  </span>
                  <Input
                    v-model="rule.selector"
                    aria-label="CSS selector"
                    input-class="font-mono"
                  />
                </label>

                <label class="flex flex-col gap-1.5">
                  <span class="text-xs uppercase tracking-wider opacity-55">
                    Attribute
                  </span>
                  <Combobox
                    :model-value="rule.attr"
                    :items="SCRAPE_ATTR_ITEMS"
                    @update:model-value="(val) => (rule.attr = val)"
                    layout="fill"
                    empty-text="No matches."
                  />
                </label>

                <label class="flex flex-col gap-1.5 lg:col-span-2">
                  <span class="text-xs uppercase tracking-wider opacity-55">
                    XPath (Optional)
                  </span>
                  <Textarea
                    v-model="rule.xpath"
                    aria-label="XPath"
                    class="min-h-24 font-mono"
                  />
                </label>

                <div class="flex flex-col gap-1.5 lg:col-span-2">
                  <span class="text-xs uppercase tracking-wider opacity-55">
                    Role
                  </span>
                  <SegmentedControl
                    :model-value="tokenRole(site.key, rule.token)"
                    class="flex-wrap h-auto min-h-9"
                    @update:model-value="(value) => updateRole(site.key, rule, value)"
                  >
                    <SegmentedControlItem
                      v-for="role in ROLE_ITEMS"
                      :key="role.key"
                      :value="role.key"
                      :disabled="
                        role.key === 'title' &&
                        isTitleRoleDisabled(site.key, rule.token)
                      "
                    >
                      {{ role.label }}
                    </SegmentedControlItem>
                  </SegmentedControl>
                </div>

                <div class="flex items-center justify-between gap-3 lg:col-span-2">
                  <label class="flex items-center gap-2 text-sm">
                    <Checkbox
                      :checked="rule.multi"
                      aria-label="Match multiple"
                      @update:checked="(v: boolean) => (rule.multi = v)"
                    />
                    <span>Multi</span>
                  </label>

                  <Button
                    variant="danger"
                    type="button"
                    title="Remove rule"
                    aria-label="Remove rule"
                    @click="removeScrapeRule(site.key, index)"
                  >
                    <template #icon>
                      <IconTrash class="w-4 h-4" aria-hidden="true" />
                    </template>
                    Remove
                  </Button>
                </div>
              </div>
            </div>

            <div>
              <Button
                variant="soft"
                type="button"
                title="Add rule"
                @click="addScrapeRule(site.key)"
              >
                <template #icon>
                  <IconAdd class="w-4 h-4" aria-hidden="true" />
                </template>
                Add rule
              </Button>
            </div>
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
