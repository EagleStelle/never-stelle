<script setup lang="ts">
import { reactive } from "vue";
import IconAdd from "~icons/material-symbols/add";
import IconInfo from "~icons/material-symbols/info-outline";
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ScrapeRule, TokenRole } from "@/types";
import {
  displayUrlTemplate,
  sourceIconUrl,
  templateParts,
  tokenLabel,
} from "@/utils/dashboard";
import { useScrapeTests } from "@/features/settings/composables/useScrapeTests";
import { useSettingsContext } from "@/features/settings/context";
import { Field, FieldContent, FieldLabel } from "@/components/ui/field";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

const SCRAPE_ATTR_ITEMS = [
  { key: "text", label: "Text" },
  { key: "href", label: "Link (href)" },
  { key: "src", label: "Media (src)" },
  { key: "alt", label: "Alt text (alt)" },
  { key: "title", label: "Tooltip (title)" },
];

type FindMode = "css" | "xpath";

const FIND_ITEMS: { key: FindMode; label: string }[] = [
  { key: "css", label: "CSS" },
  { key: "xpath", label: "XPath" },
];

// A rule keeps only the fields of its chosen method, since a set XPath would win over
// the selector. What a switch clears is kept here so switching back restores it.
const findModes = reactive(new WeakMap<ScrapeRule, FindMode>());
const parked = new WeakMap<ScrapeRule, Partial<ScrapeRule>>();

function findMode(rule: ScrapeRule): FindMode {
  return findModes.get(rule) ?? (rule.xpath ? "xpath" : "css");
}

function setFindMode(rule: ScrapeRule, value: string | string[]): void {
  const next = Array.isArray(value) ? value[0] : value;
  if (next !== "css" && next !== "xpath") return;
  if (next === findMode(rule)) return;
  findModes.set(rule, next);
  const keys: (keyof ScrapeRule)[] =
    next === "css" ? ["xpath"] : ["selector", "match_label"];
  const restore = parked.get(rule) ?? {};
  parked.set(rule, Object.fromEntries(keys.map((key) => [key, rule[key]])));
  for (const key of keys) (rule[key] as string) = "";
  Object.assign(rule, restore);
}

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

const {
  settings,
  settingsDraft,
  learnedFormatsDraft,
  editableSourceProfiles,
  showRequired,
} = useSettingsContext();

// Only after a Save found it empty, so a fresh rule does not start out red.
function missing(value: string): "true" | undefined {
  return showRequired.value && !value.trim() ? "true" : undefined;
}
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
      <AccordionTrigger :image="sourceIconUrl(site.key)">
        {{ site.label }}
      </AccordionTrigger>

      <AccordionContent>
        <div class="flex flex-col gap-4">
          <Field>
            <FieldLabel :for="`${site.key}ScraperProbeInput`">
              Probe URL
            </FieldLabel>
            <FieldContent class="flex-row items-center gap-2">
              <Input
                :id="`${site.key}ScraperProbeInput`"
                v-model="scrapeTests[site.key].url"
                data-settings-system
                type="text"
                inputmode="url"
                placeholder="Paste a link"
                class="flex-1"
                @keydown.enter.prevent="runScrapeTest(site.key)"
              />
              <Button
                variant="primary"
                size="icon"
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
            </FieldContent>
          </Field>

          <p
            v-if="scrapeTests[site.key].message"
            class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
          >
            <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
            {{ scrapeTests[site.key].message }}
          </p>

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
                  class="w-36 sm:w-44 max-w-36 sm:max-w-44 font-mono align-top"
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
                    class="block min-w-0 wrap-break-word whitespace-pre-wrap leading-normal [word-break:break-word] max-h-32 overflow-y-auto"
                    :title="result.value"
                  >
                    {{ result.value }}
                  </span>
                  <span v-else class="opacity-50 italic">no match</span>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>

          <p
            v-if="!formatsFor(site.key).length"
            class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
          >
            <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
            Download once from this source to learn its URL format, then add
            scraper rules.
          </p>

          <div
            v-else
            class="flex flex-col divide-y divide-(--glass-border) border-t border-(--glass-border)"
          >
            <section
              v-for="template in formatsFor(site.key)"
              :key="template"
              class="flex flex-col gap-3 py-4 last:pb-0"
              :aria-label="displayUrlTemplate(template)"
            >
              <div class="flex min-h-8 items-center justify-between gap-3">
                <p class="min-w-0 font-mono text-[0.8125rem] leading-snug wrap-anywhere text-muted-foreground">
                  <span
                    v-for="(part, partIndex) in templateParts(displayUrlTemplate(template))"
                    :key="partIndex"
                    :class="part.token && 'text-accent-ink'"
                    >{{ part.text }}</span
                  >
                </p>
                <Button
                  compact
                  variant="secondary"
                  class="shrink-0"
                  size="sm"
                  type="button"
                  @click="addScrapeRule(site.key, template)"
                >
                  <template #icon>
                    <IconAdd aria-hidden="true" />
                  </template>
                  Add Rule
                </Button>
              </div>

              <p
                v-if="!rulesForFormat(site.key, template).length"
                class="flex items-start gap-2 text-[0.8125rem] leading-normal text-muted-foreground"
              >
                <IconInfo class="mt-0.5 size-4 shrink-0" aria-hidden="true" />
                No scraper rules configured for this format yet.
              </p>

              <div
                v-if="rulesForFormat(site.key, template).length"
                class="flex flex-col gap-5"
              >
                <!-- One rule: name and role, where to find it, what to read. -->
                <div
                  v-for="{ rule, index } in rulesForFormat(site.key, template)"
                  :key="index"
                  class="flex flex-col gap-5 border-l border-(--glass-border) pl-4"
                >
                  <div class="flex flex-col gap-3">
                    <div class="flex min-h-8 items-center justify-between gap-2">
                      <span class="min-w-0 font-mono text-sm leading-snug wrap-anywhere text-accent-ink">
                        {{ tokenLabel(rule.token || `var${index}`) }}
                      </span>
                      <Button
                        variant="destructive-ghost"
                        size="icon-sm"
                        type="button"
                        title="Remove rule"
                        aria-label="Remove rule"
                        @click="removeScrapeRule(site.key, index)"
                      >
                        <template #icon>
                          <IconTrash aria-hidden="true" />
                        </template>
                      </Button>
                    </div>

                    <!-- Two columns like Cookies; on phones the right one wraps only when out of room. -->
                    <div class="flex flex-wrap gap-x-6 gap-y-3 sm:grid sm:grid-cols-2 sm:gap-x-8">
                      <div class="min-w-36 flex-1">
                        <Field label-width="sm">
                          <FieldLabel :for="`${site.key}RuleToken${index}`" required>
                            Token
                          </FieldLabel>
                          <FieldContent>
                            <Input
                              :id="`${site.key}RuleToken${index}`"
                              :model-value="rule.token"
                              required
                              :aria-invalid="missing(rule.token)"
                              @update:model-value="
                                (v) => setRuleToken(site.key, rule, index, String(v))
                              "
                            />
                          </FieldContent>
                        </Field>
                      </div>
                      <div>
                        <SegmentedControl
                          label="Role"
                          label-placement="start"
                          label-width="sm"
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
                  </div>

                  <div class="flex flex-col gap-3">
                    <div class="flex flex-wrap gap-x-6 gap-y-3 sm:grid sm:grid-cols-2 sm:gap-x-8">
                      <div class="min-w-36 flex-1">
                        <Combobox
                          :model-value="rule.attr"
                          :items="SCRAPE_ATTR_ITEMS"
                          @update:model-value="(val) => (rule.attr = val)"
                          label="Attribute"
                          label-placement="start"
                          label-width="sm"
                          empty-text="No matches."
                        />
                      </div>
                      <div>
                        <SegmentedControl
                          label="Type"
                          label-placement="start"
                          label-width="sm"
                          :model-value="findMode(rule)"
                          @update:model-value="(value) => setFindMode(rule, value)"
                        >
                          <SegmentedControlItem
                            v-for="mode in FIND_ITEMS"
                            :key="mode.key"
                            :value="mode.key"
                          >
                            {{ mode.label }}
                          </SegmentedControlItem>
                        </SegmentedControl>
                      </div>
                    </div>

                    <template v-if="findMode(rule) === 'css'">
                      <Field label-width="sm">
                        <FieldLabel :for="`${site.key}RuleSelector${index}`" required>
                          Selector
                        </FieldLabel>
                        <FieldContent>
                          <Input
                            :id="`${site.key}RuleSelector${index}`"
                            v-model="rule.selector"
                            required
                            :aria-invalid="missing(rule.selector)"
                          />
                        </FieldContent>
                      </Field>

                      <Field label-width="sm">
                        <FieldLabel :for="`${site.key}RuleMatchLabel${index}`">
                          Label
                        </FieldLabel>
                        <FieldContent>
                          <Input
                            :id="`${site.key}RuleMatchLabel${index}`"
                            v-model="rule.match_label"
                          />
                        </FieldContent>
                      </Field>
                    </template>

                    <Field
                      v-else
                      label-width="sm"
                      class="sm:items-start sm:*:data-[slot=field-label]:pt-2"
                    >
                      <FieldLabel :for="`${site.key}RuleXpath${index}`" required>
                        XPath
                      </FieldLabel>
                      <FieldContent>
                        <Textarea
                          :id="`${site.key}RuleXpath${index}`"
                          v-model="rule.xpath"
                          required
                          :aria-invalid="missing(rule.xpath)"
                          class="min-h-24"
                        />
                      </FieldContent>
                    </Field>

                    <label class="flex w-fit cursor-pointer items-center gap-2 text-sm sm:ml-31">
                      <Checkbox
                        :checked="rule.multi"
                        @update:checked="(v: boolean) => (rule.multi = v)"
                      />
                      <span>All matches</span>
                    </label>
                  </div>
                </div>
              </div>
            </section>
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  </Accordion>
</template>
