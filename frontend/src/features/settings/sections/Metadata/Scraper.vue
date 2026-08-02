<script setup lang="ts">
import IconAdd from "~icons/material-symbols/add";
import IconSearch from "~icons/material-symbols/search";
import IconSpinner from "~icons/material-symbols/sync";
import IconTrash from "~icons/material-symbols/delete";

import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ScrapeRule, TokenRole } from "@/types";
import { displayUrlTemplate, tokenLabel } from "@/utils/dashboard";
import { useScrapeTests } from "@/features/settings/composables/useScrapeTests";
import { useSettingsContext } from "@/features/settings/context";
import { Label } from "@/components/ui/label";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

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

function updateRole(
  siteKey: string,
  rule: ScrapeRule,
  index: number,
  value: unknown,
): void {
  const next = String(value || "ignore") as TokenRole;
  const role = ROLE_ITEMS.some((item) => item.key === next) ? next : "ignore";
  setTokenRole(siteKey, rule.token || `var${index}`, role);
}

const { settings, settingsDraft, learnedFormatsDraft, editableSourceProfiles } =
  useSettingsContext();
const {
  scrapeTests,
  formatsFor,
  rulesForFormat,
  platformRules,
  tokenRole,
  isRoleDisabled,
  setTokenRole,
  addScrapeRule,
  removeScrapeRule,
  runScrapeTest,
  setRuleToken,
} = useScrapeTests(
  settingsDraft,
  settings,
  learnedFormatsDraft,
  editableSourceProfiles,
);
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
          <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
            <Label class="sm:w-40 sm:shrink-0">Probe URL</Label>
            <div class="flex items-center gap-2 w-full sm:flex-auto">
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

          <Card v-if="scrapeTests[site.key].message" class="px-6">
            <p class="text-[0.8125rem] text-muted-foreground">
              {{ scrapeTests[site.key].message }}
            </p>
          </Card>

          <Table
            v-if="scrapeTests[site.key].results.length"
            class="w-full table-fixed text-[0.8125rem]"
          >
            <TableHeader>
              <TableRow>
                <TableHead
                  class="w-36 sm:w-44 text-[0.68rem] uppercase tracking-wider text-white/45 in-[.light-mode]:text-black/45"
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
                <TableCell
                  class="w-36 sm:w-44 max-w-[9rem] sm:max-w-[11rem] font-mono align-top"
                >
                  <span
                    class="block truncate"
                    :title="tokenLabel(result.token)"
                  >
                    {{ tokenLabel(result.token) }}
                  </span>
                </TableCell>
                <TableCell class="min-w-0 align-top">
                  <span
                    v-if="result.matched"
                    class="block min-w-0 break-words whitespace-pre-wrap leading-normal [word-break:break-word] max-h-32 overflow-y-auto"
                    :title="result.value"
                  >
                    {{ result.value }}
                  </span>
                  <span v-else class="opacity-50 italic">no match</span>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>

          <Card v-if="!formatsFor(site.key).length" class="px-6">
            <p class="text-[0.8125rem] text-muted-foreground">
              Download once from this source to learn its URL format, then add
              scraper rules.
            </p>
          </Card>

          <Card
            v-for="template in formatsFor(site.key)"
            :key="template"
          >
            <CardHeader class="flex flex-row items-center justify-between gap-2">
              <CardTitle class="min-w-0 font-mono text-sm leading-snug wrap-anywhere">
                {{ displayUrlTemplate(template) }}
              </CardTitle>
              <Button
                class="shrink-0"
                variant="primary"
                type="button"
                title="Add rule"
                aria-label="Add rule"
                @click="addScrapeRule(site.key, template)"
              >
                <template #icon>
                  <IconAdd class="w-4 h-4" aria-hidden="true" />
                </template>
              </Button>
            </CardHeader>

            <CardContent class="flex flex-col gap-4">
              <p
                v-if="!rulesForFormat(site.key, template).length"
                class="text-[0.8125rem] text-muted-foreground"
              >
                No scraper rules configured for this format yet.
              </p>

              <template
                v-for="({ rule, index }, i) in rulesForFormat(site.key, template)"
                :key="index"
              >
                <Separator v-if="i > 0" class="my-2" />

                <div class="flex flex-col gap-3">
                  <div class="flex items-center justify-between gap-2">
                    <span class="min-w-0 font-mono text-sm font-semibold wrap-anywhere">
                      {{
                        rule.token
                          ? tokenLabel(rule.token)
                          : tokenLabel(`var${index}`)
                      }}
                    </span>
                    <Button
                      class="shrink-0"
                      variant="destructive"
                      type="button"
                      title="Remove rule"
                      aria-label="Remove rule"
                      @click="removeScrapeRule(site.key, index)"
                    >
                      <template #icon>
                        <IconTrash class="w-4 h-4" aria-hidden="true" />
                      </template>
                    </Button>
                  </div>

                  <div class="flex flex-col gap-3">
                    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                      <Label class="sm:w-28 sm:shrink-0">Token</Label>
                      <div class="w-full sm:flex-auto">
                        <Input
                          :model-value="rule.token"
                          aria-label="Token name"
                          @update:model-value="
                            (v) => setRuleToken(site.key, rule, index, String(v))
                          "
                        />
                      </div>
                    </div>

                    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                      <Label class="sm:w-28 sm:shrink-0">Label</Label>
                      <div class="w-full sm:flex-auto">
                        <Input
                          v-model="rule.match_label"
                          aria-label="Label to anchor on"
                        />
                      </div>
                    </div>

                    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                      <Label class="sm:w-28 sm:shrink-0">Selector</Label>
                      <div class="w-full sm:flex-auto">
                        <Input
                          v-model="rule.selector"
                          aria-label="CSS selector"
                        />
                      </div>
                    </div>

                    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                      <Label class="sm:w-28 sm:shrink-0">Attribute</Label>
                      <div class="w-full sm:flex-auto">
                        <Combobox
                          :model-value="rule.attr"
                          :items="SCRAPE_ATTR_ITEMS"
                          @update:model-value="(val) => (rule.attr = val)"
                          layout="fill"
                          empty-text="No matches."
                        />
                      </div>
                    </div>

                    <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:gap-3">
                      <Label class="sm:w-28 sm:shrink-0 sm:pt-2">XPath (Optional)</Label>
                      <div class="w-full sm:flex-auto">
                        <Textarea
                          v-model="rule.xpath"
                          aria-label="XPath"
                          class="min-h-24"
                        />
                      </div>
                    </div>

                    <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
                      <Label class="sm:w-28 sm:shrink-0">Role</Label>
                      <div class="w-full sm:flex-auto">
                        <SegmentedControl
                          :model-value="
                            tokenRole(site.key, rule.token || `var${index}`)
                          "
                          @update:model-value="
                            (value) => updateRole(site.key, rule, index, value)
                          "
                        >
                          <SegmentedControlItem
                            v-for="role in ROLE_ITEMS"
                            :key="role.key"
                            :value="role.key"
                            :disabled="
                              isRoleDisabled(
                                site.key,
                                rule.token || `var${index}`,
                                role.key,
                              )
                            "
                          >
                            {{ role.label }}
                          </SegmentedControlItem>
                        </SegmentedControl>
                      </div>
                    </div>

                    <div class="flex items-center gap-2 text-sm pt-1">
                      <Checkbox
                        :checked="rule.multi"
                        aria-label="Match multiple"
                        @update:checked="(v: boolean) => (rule.multi = v)"
                      />
                      <span>Multi</span>
                    </div>
                  </div>
                </div>
              </template>
            </CardContent>
          </Card>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
