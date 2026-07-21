<script setup lang="ts">
import IconAdd from "~icons/material-symbols/add";
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
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "../../../../components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../../../../components/ui/table";
import type { ScrapeRule, TokenRole } from "../../../../types";
import { tokenLabel } from "../../../../utils/dashboard";
import { useScrapeTests } from "../../composables/useScrapeTests";
import { useSettingsContext } from "../../context";
import SettingsLabel from "../../SettingsLabel.vue";
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
  { key: "creator", label: "Creator" },
  { key: "title", label: "Title" },
];

function updateRole(siteKey: string, rule: ScrapeRule, index: number, value: unknown): void {
  const next = String(value || "ignore") as TokenRole;
  const role = ROLE_ITEMS.some((item) => item.key === next) ? next : "ignore";
  setTokenRole(siteKey, rule.token || `var${index}`, role);
}

const { settings, settingsDraft, editableSourceProfiles } = useSettingsContext();
const {
  scrapeTests,
  formatsFor,
  rulesForFormat,
  tokenRole,
  isTitleRoleDisabled,
  setTokenRole,
  addScrapeRule,
  removeScrapeRule,
  runScrapeTest,
  setRuleToken,
} = useScrapeTests(settingsDraft, settings, editableSourceProfiles);
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
          <div class="flex flex-col gap-1.5">
            <SettingsLabel>Probe URL</SettingsLabel>
            <div class="flex items-center gap-2 w-full">
              <Input
                :id="`${site.key}ScraperProbeInput`"
                v-model="scrapeTests[site.key].url"
                data-settings-system
                type="text"
                inputmode="url"
                aria-label="Probe URL"
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
          </div>

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

          <div
            v-for="template in formatsFor(site.key)"
            :key="template"
            class="flex flex-col gap-2"
          >
            <SettingsLabel class="wrap-anywhere">
              {{ template }}
            </SettingsLabel>

            <Card
              v-for="{ rule, index } in rulesForFormat(site.key, template)"
              :key="index"
            >
              <CardHeader>
                <CardTitle class="font-mono text-sm leading-snug">
                  {{ rule.token ? tokenLabel(rule.token) : tokenLabel(`var${index}`) }}
                </CardTitle>
                <CardDescription class="font-mono text-xs">
                  {{ rule.selector }}
                </CardDescription>
              </CardHeader>

              <CardContent class="grid grid-cols-1 gap-3 lg:grid-cols-2">
                <label class="flex flex-col gap-1.5">
                  <SettingsLabel>Token</SettingsLabel>
                  <Input
                    :model-value="rule.token"
                    aria-label="Token name"
                    input-class="font-mono"
                    @update:model-value="(v) => setRuleToken(site.key, rule, index, String(v))"
                  />
                </label>

                <label class="flex flex-col gap-1.5">
                  <SettingsLabel>Label</SettingsLabel>
                  <Input
                    v-model="rule.match_label"
                    aria-label="Label to anchor on"
                  />
                </label>

                <label class="flex flex-col gap-1.5">
                  <SettingsLabel>Selector</SettingsLabel>
                  <Input
                    v-model="rule.selector"
                    aria-label="CSS selector"
                    input-class="font-mono"
                  />
                </label>

                <label class="flex flex-col gap-1.5">
                  <SettingsLabel>Attribute</SettingsLabel>
                  <Combobox
                    :model-value="rule.attr"
                    :items="SCRAPE_ATTR_ITEMS"
                    @update:model-value="(val) => (rule.attr = val)"
                    layout="fill"
                    empty-text="No matches."
                  />
                </label>

                <label class="flex flex-col gap-1.5 lg:col-span-2">
                  <SettingsLabel>XPath (Optional)</SettingsLabel>
                  <Textarea
                    v-model="rule.xpath"
                    aria-label="XPath"
                    class="min-h-24 font-mono"
                  />
                </label>

                <div class="flex flex-col gap-1.5 lg:col-span-2">
                  <SettingsLabel>Role</SettingsLabel>
                  <SegmentedControl
                    :model-value="tokenRole(site.key, rule.token || `var${index}`)"
                    class="flex-wrap h-auto min-h-9"
                    @update:model-value="(value) => updateRole(site.key, rule, index, value)"
                  >
                    <SegmentedControlItem
                      v-for="role in ROLE_ITEMS"
                      :key="role.key"
                      :value="role.key"
                      :disabled="
                        role.key === 'title' &&
                        isTitleRoleDisabled(site.key, rule.token || `var${index}`)
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
              </CardContent>
            </Card>

            <div>
              <Button
                variant="soft"
                type="button"
                title="Add rule"
                @click="addScrapeRule(site.key, template)"
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
