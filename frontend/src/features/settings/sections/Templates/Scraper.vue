<script setup lang="ts">
import IconAdd from "~icons/material-symbols/add";
import IconCheck from "~icons/material-symbols/check";
import IconClose from "~icons/material-symbols/close";
import IconTrash from "~icons/material-symbols/delete";
import IconSearch from "~icons/material-symbols/search";

import { Button } from "../../../../components/ui/button";
import { Checkbox } from "../../../../components/ui/checkbox";
import { Combobox } from "../../../../components/ui/combobox";
import { Input } from "../../../../components/ui/input";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "../../../../components/ui/segmented-control";
import { normalizeTokenName } from "../../../../utils/dashboard";
import { useScrapeTests } from "../../composables/useScrapeTests";
import { useSettingsContext } from "../../context";

const SCRAPE_ATTR_ITEMS = [
  { key: "text", label: "Text" },
  { key: "href", label: "Link (href)" },
  { key: "src", label: "src" },
  { key: "alt", label: "alt" },
  { key: "title", label: "title" },
];

// Built in script so the literal braces never reach the template tokenizer.
function tokenLabel(token: string): string {
  return `{{${token}}}`;
}

const { settingsDraft, editableSourceProfiles } = useSettingsContext();
const {
  scrapeTests,
  isCreatorTokenRole,
  setCreatorTokenRole,
  addScrapeRule,
  removeScrapeRule,
  runScrapeTest,
} = useScrapeTests(settingsDraft, editableSourceProfiles);
</script>

<template>
  <div
    v-for="site in editableSourceProfiles"
    :key="site.key"
    class="flex flex-col gap-[0.65rem] p-[0.85rem] mb-3 rounded-xl border border-(--glass-border)"
  >
    <div class="flex items-center justify-between gap-3 mb-2">
      <span class="text-sm font-medium">{{ site.label }}</span>
      <div class="flex items-center gap-4">
        <Button
          v-if="settingsDraft.source_scrape_rules[site.key].enabled"
          variant="soft"
          type="button"
          aria-label="Add rule"
          title="Add rule"
          @click="addScrapeRule(site.key)"
        >
          <template #icon>
            <IconAdd class="w-4 h-4" aria-hidden="true" />
          </template>
        </Button>
        <SegmentedControl
          size="lg"
          :model-value="
            settingsDraft.source_scrape_rules[site.key].enabled
              ? 'enabled'
              : 'disabled'
          "
          @update:model-value="
            (val: string) =>
              (settingsDraft.source_scrape_rules[site.key].enabled =
                val === 'enabled')
          "
        >
          <SegmentedControlItem
            value="enabled"
            aria-label="Enabled"
            title="Enabled"
          >
            <IconCheck class="w-4 h-4" aria-hidden="true" />
          </SegmentedControlItem>
          <SegmentedControlItem
            value="disabled"
            aria-label="Disabled"
            title="Disabled"
          >
            <IconClose class="w-4 h-4" aria-hidden="true" />
          </SegmentedControlItem>
        </SegmentedControl>
      </div>
    </div>

    <template v-if="settingsDraft.source_scrape_rules[site.key].enabled">
      <p
        v-if="!settingsDraft.source_scrape_rules[site.key].rules.length"
        class="text-xs text-white/50 in-[.light-mode]:text-black/50"
      >
        No rules yet.
      </p>

      <div
        v-for="(rule, index) in settingsDraft.source_scrape_rules[site.key].rules"
        :key="index"
        class="flex flex-col p-3 mb-3 rounded-xl bg-black/10 in-[.light-mode]:bg-white/40 border border-(--glass-border)"
      >
        <div
          class="grid grid-cols-1 gap-[0.4rem] sm:grid-cols-[minmax(0,0.9fr)_minmax(0,1fr)_minmax(0,1.2fr)_minmax(0,0.9fr)]"
        >
          <Input
            v-model="rule.token"
            placeholder="token"
            aria-label="Token name"
            input-class="font-mono"
          />
          <Input
            v-model="rule.match_label"
            placeholder="label (optional)"
            aria-label="Label to anchor on"
          />
          <Input
            v-model="rule.selector"
            placeholder="CSS selector"
            aria-label="CSS selector"
            input-class="font-mono"
          />
          <Combobox
            :model-value="rule.attr"
            :items="SCRAPE_ATTR_ITEMS"
            @update:model-value="(val) => (rule.attr = val)"
            layout="fill"
            placeholder="Attribute"
            empty-text="—"
          />
        </div>
        <div class="flex items-center gap-2 mt-2">
          <label
            class="flex items-center gap-2 cursor-pointer select-none text-sm shrink-0"
          >
            <Checkbox
              :checked="rule.multi"
              @update:checked="(v: boolean) => (rule.multi = v)"
            />
            <span>Multiple</span>
          </label>
          <label
            class="flex items-center gap-2 cursor-pointer select-none text-sm shrink-0"
            :class="!normalizeTokenName(rule.token) && 'opacity-50'"
          >
            <Checkbox
              :checked="isCreatorTokenRole(site.key, rule.token)"
              :disabled="!normalizeTokenName(rule.token)"
              @update:checked="
                (v: boolean) => setCreatorTokenRole(site.key, rule.token, Boolean(v))
              "
            />
            <span>Creator</span>
          </label>
          <Input
            v-model="rule.xpath"
            placeholder="XPath (optional, overrides selector)"
            aria-label="XPath"
            input-class="font-mono"
            class="flex-1"
          />
          <Button
            variant="soft"
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
      </div>

      <div class="flex items-center gap-2 mt-[0.35rem]">
        <Input
          v-model="scrapeTests[site.key].url"
          type="text"
          inputmode="url"
          placeholder="Sample URL to test"
          class="flex-1"
        />
        <Button
          variant="primary"
          type="button"
          class="shrink-0"
          :disabled="scrapeTests[site.key].loading"
          @click="runScrapeTest(site.key)"
        >
          <template #icon>
            <IconSearch class="w-4 h-4" aria-hidden="true" />
          </template>
          {{ scrapeTests[site.key].loading ? "Testing..." : "Test" }}
        </Button>
      </div>

      <p
        v-if="scrapeTests[site.key].message"
        class="text-xs text-white/60 in-[.light-mode]:text-black/60"
      >
        {{ scrapeTests[site.key].message }}
      </p>
      <ul
        v-if="scrapeTests[site.key].results.length"
        class="flex flex-col gap-1 text-[0.8125rem]"
      >
        <li
          v-for="result in scrapeTests[site.key].results"
          :key="result.token"
          class="flex items-baseline gap-2 min-w-0"
        >
          <span class="font-mono shrink-0">{{ tokenLabel(result.token) }}</span>
          <span v-if="result.matched" class="min-w-0 wrap-anywhere">{{
            result.value
          }}</span>
          <span v-else class="opacity-50 italic">no match</span>
        </li>
      </ul>
    </template>
  </div>
</template>
