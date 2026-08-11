<script setup lang="ts">
import { computed, nextTick, reactive, ref } from "vue";
import IconDrag from "~icons/material-symbols/drag-indicator";


import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSeparator,
  FieldSet,
  FieldTitle,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { CookiePolicyField, NamingChoice } from "@/types";
import {
  POST_PROCESSING_FIELDS,
  isAudioCodecCompatibleWithVideoContainer,
  isCodecCompatibleWithContainer,
  isLosslessAudioFormat,
  postProcessingCapabilitiesForDefaults,
  videoAudioCodecOptionsForContainer,
  videoCodecOptionsForContainer,
} from "@/utils/dashboard";
import { COOKIE_POLICY_FIELDS } from "@/features/settings/cookiePolicy";
import { useSettingsContext } from "@/features/settings/context";
import {
  GLOBAL_NAMING_KEY,
  useNamingSettings,
} from "@/features/settings/composables/useNamingSettings";
import {
  FIELD_ROLE_DEFS,
  type FieldRole,
} from "@/features/settings/composables/useFieldsSettings";

const { settings, settingsDraft } = useSettingsContext();
const {
  cleanupRules,
  titleLengthRule,
  namingChoices,
  ruleEnabled,
  setRule,
  maxChars,
  setMaxChars,
  stemMaxChars,
  setStemMaxChars,
  choiceValue,
  setChoice,
} = useNamingSettings(settingsDraft, settings);

// Only codecs the chosen container can play back (Auto always fits); prevents VP9-in-MP4.
const videoCodecItems = computed(() =>
  videoCodecOptionsForContainer(
    settings.quality_options,
    settingsDraft.default_quality.video_container,
  ),
);
const videoAudioCodecItems = computed(() =>
  videoAudioCodecOptionsForContainer(
    settings.quality_options,
    settingsDraft.default_quality.video_container,
  ),
);

const defaultEmbedCapabilities = computed(() =>
  postProcessingCapabilitiesForDefaults(
    settingsDraft.default_quality,
    settings.quality_options,
    settingsDraft.default_post_processing.save_as,
  ),
);

function setDefaultPostProcessing(next: typeof settingsDraft.default_post_processing): void {
  // Defaults cover both the video and audio pickers. Do not erase a compatible
  // video choice merely because the currently configured audio format cannot
  // carry it; the download toolbar filters only the task being submitted.
  Object.assign(settingsDraft.default_post_processing, next);
}

function setDefaultPostProcessingSaveAs(value: string | string[]): void {
  if (value !== "sidecar" && value !== "embed") return;
  setDefaultPostProcessing({
    ...settingsDraft.default_post_processing,
    save_as: value,
  });
}

function updateContainer(container: string): void {
  settingsDraft.default_quality.video_container = container;
  // A codec the new container can't play back would force an unplayable stream; reset to Auto.
  if (
    !isCodecCompatibleWithContainer(
      settingsDraft.default_quality.video_codec,
      container,
      settings.quality_options.video_containers,
    )
  ) {
    settingsDraft.default_quality.video_codec = "auto";
  }
  if (
    !isAudioCodecCompatibleWithVideoContainer(
      settingsDraft.default_quality.video_audio_codec,
      container,
      settings.quality_options.video_containers,
    )
  ) {
    settingsDraft.default_quality.video_audio_codec = "auto";
  }
}

const policyEdits = reactive<Partial<Record<CookiePolicyField, string>>>({});

function policyInherited(field: CookiePolicyField): number {
  return Number(settings.cookie_policy_defaults[field]);
}

function policyValue(field: CookiePolicyField): string {
  const editing = policyEdits[field];
  if (editing !== undefined) return editing;
  const value = settingsDraft.default_cookie_policy[field];
  return String(
    value === undefined || value === null ? policyInherited(field) : value,
  );
}

function setPolicyValue(field: CookiePolicyField, raw: string | number): void {
  const text = String(raw ?? "").trim();
  policyEdits[field] = text;
  const parsed = Number(text);
  if (!text || (Number.isFinite(parsed) && parsed === policyInherited(field))) {
    delete settingsDraft.default_cookie_policy[field];
  } else {
    if (!Number.isFinite(parsed)) return;
    settingsDraft.default_cookie_policy[field] = parsed;
  }
}

function endPolicyEdit(field: CookiePolicyField): void {
  delete policyEdits[field];
}

// Slug and scraper tokens belong to one source, so the global order is plain fields only.
function builtinFields(role: FieldRole): string[] {
  return settings.field_defaults?.[role] || [];
}

function sameList(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((field, index) => field === right[index]);
}

function fieldList(role: FieldRole): string[] {
  const configured = settingsDraft.default_fields[role] || [];
  return configured.length ? configured : builtinFields(role);
}

function isFieldOrderConfigured(role: FieldRole): boolean {
  const configured = settingsDraft.default_fields[role] || [];
  if (!configured.length) return false;
  const builtin = builtinFields(role);
  return (
    configured.length !== builtin.length ||
    configured.some((field, index) => field !== builtin[index])
  );
}

function resetFieldOrder(role: FieldRole): void {
  settingsDraft.default_fields[role] = [];
}

function reorderField(role: FieldRole, from: number, to: number): void {
  const list = [...fieldList(role)];
  if (
    from === to ||
    from < 0 ||
    from >= list.length ||
    to < 0 ||
    to >= list.length
  ) {
    return;
  }
  const [moved] = list.splice(from, 1);
  list.splice(to, 0, moved);
  settingsDraft.default_fields[role] = sameList(list, builtinFields(role)) ? [] : list;
}

// Native drag-and-drop reordering, scoped to one role list at a time.
const drag = reactive<{ role: FieldRole | ""; from: number; over: number }>({
  role: "",
  from: -1,
  over: -1,
});

function onDragStart(role: FieldRole, index: number, event: DragEvent): void {
  drag.role = role;
  drag.from = index;
  drag.over = index;
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", String(index));
  }
}

function onDragOver(role: FieldRole, index: number): void {
  if (drag.role === role) drag.over = index;
}

function onDrop(role: FieldRole, index: number): void {
  if (drag.role === role) reorderField(role, drag.from, index);
  resetDrag();
}

function resetDrag(): void {
  drag.role = "";
  drag.from = -1;
  drag.over = -1;
}

function isDragging(role: FieldRole, index: number): boolean {
  return drag.role === role && drag.from === index;
}

function isDropTarget(role: FieldRole, index: number): boolean {
  return drag.role === role && drag.over === index && drag.from !== index;
}

const FOLDER_INPUT_ID = "defaultFolderTemplate";
const FILENAME_INPUT_ID = "defaultFilenameTemplate";

const templateTokens = computed(() => settings.template_tokens || []);

// Track the last focused template input so token clicks insert into it.
const lastFocusedInputId = ref<string>("");

function recordFocus(id: string): void {
  lastFocusedInputId.value = id;
}

function templateValue(id: string): string {
  return id === FOLDER_INPUT_ID
    ? settingsDraft.template_settings.folder_template
    : settingsDraft.template_settings.filename_template;
}

function setTemplate(id: string, value: string): void {
  if (id === FOLDER_INPUT_ID)
    settingsDraft.template_settings.folder_template = value;
  else settingsDraft.template_settings.filename_template = value;
}

function insertToken(token: string): void {
  const targetId = lastFocusedInputId.value;
  if (targetId !== FOLDER_INPUT_ID && targetId !== FILENAME_INPUT_ID) return;
  const current = templateValue(targetId);
  const el = document.getElementById(targetId) as HTMLInputElement | null;
  const start = el?.selectionStart ?? current.length;
  const end = el?.selectionEnd ?? current.length;
  const text = `{{${token}}}`;
  setTemplate(targetId, current.slice(0, start) + text + current.slice(end));
  void nextTick(() => {
    const caret = start + text.length;
    el?.focus();
    el?.setSelectionRange(caret, caret);
  });
}

// A segmented control clears its value when the active item is clicked again; keep the
// current choice instead of writing an empty one.
function onChoice(choice: NamingChoice, value: string | string[]): void {
  const next = Array.isArray(value) ? value[0] : value;
  if (next) setChoice(GLOBAL_NAMING_KEY, choice, next);
}
</script>

<template>
  <TooltipProvider>
    <div class="flex flex-col gap-6">
      <FieldSet>
        <FieldLegend>Video</FieldLegend>
        <FieldGroup>
          <Combobox
            :model-value="settingsDraft.default_quality.video_quality"
            :items="settings.quality_options.video"
            @update:model-value="
              (val) => (settingsDraft.default_quality.video_quality = val)
            "
            label="Quality"
            label-placement="start"
            placeholder="Choose a quality"
            empty-text="No presets."
          />
          <Combobox
            :model-value="settingsDraft.default_quality.video_container"
            :items="settings.quality_options.video_containers"
            @update:model-value="(val) => updateContainer(val)"
            label="Container"
            label-placement="start"
            placeholder="Choose a container"
            empty-text="No containers."
          />
          <Combobox
            :model-value="settingsDraft.default_quality.video_codec"
            :items="videoCodecItems"
            @update:model-value="
              (val) => (settingsDraft.default_quality.video_codec = val)
            "
            label="Video codec"
            label-placement="start"
            placeholder="Video codec..."
            empty-text="No codecs."
          />
          <Combobox
            :model-value="settingsDraft.default_quality.video_audio_codec"
            :items="videoAudioCodecItems"
            @update:model-value="
              (val) => (settingsDraft.default_quality.video_audio_codec = val)
            "
            label="Audio codec"
            label-placement="start"
            placeholder="Choose an audio codec"
            empty-text="No audio codecs."
          />
        </FieldGroup>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>Audio</FieldLegend>
        <FieldGroup>
          <!-- Bitrate leads, the way video quality does; format takes the container slot. -->
          <Combobox
            v-if="!isLosslessAudioFormat(settingsDraft.default_quality.audio_format)"
            :model-value="settingsDraft.default_quality.audio_bitrate"
            :items="settings.quality_options.audio_bitrates"
            @update:model-value="
              (val) => (settingsDraft.default_quality.audio_bitrate = val)
            "
            label="Bitrate"
            label-placement="start"
            placeholder="Choose a bitrate"
            empty-text="No bitrates."
          />
          <Combobox
            :model-value="settingsDraft.default_quality.audio_format"
            :items="settings.quality_options.audio_formats"
            @update:model-value="
              (val) => (settingsDraft.default_quality.audio_format = val)
            "
            label="Format"
            label-placement="start"
            placeholder="Choose a format"
            empty-text="No formats."
          />
        </FieldGroup>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>
          Post-Processing
        </FieldLegend>
        <FieldGroup>
          <FieldGroup data-slot="checkbox-group">
            <FieldLabel
              v-for="field in POST_PROCESSING_FIELDS"
              :key="field.key"
              :class="
                defaultEmbedCapabilities[field.key]
                  ? 'cursor-pointer'
                  : 'cursor-not-allowed opacity-60'
              "
            >
              <Checkbox
                :checked="settingsDraft.default_post_processing[field.key]"
                :disabled="!defaultEmbedCapabilities[field.key]"
                @update:checked="
                  (value: boolean) =>
                    setDefaultPostProcessing({
                      ...settingsDraft.default_post_processing,
                      [field.key]: Boolean(value),
                    })
                "
              />
              <span>{{ field.label }}</span>
            </FieldLabel>
          </FieldGroup>
          <SegmentedControl
            :model-value="settingsDraft.default_post_processing.save_as"
            label="Save as"
            label-placement="start"
            @update:model-value="setDefaultPostProcessingSaveAs"
          >
            <SegmentedControlItem value="sidecar">Sidecar</SegmentedControlItem>
            <SegmentedControlItem value="embed">Embed</SegmentedControlItem>
          </SegmentedControl>
        </FieldGroup>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>
          Cookies
        </FieldLegend>
        <FieldGroup>
          <Field
            v-for="field in COOKIE_POLICY_FIELDS"
            :key="field.key"
          >
            <FieldLabel :for="`defaultCookie${field.key}`" class="items-center gap-1.5">
              <span>{{ field.label }}</span>
              <Tooltip>
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
                :id="`defaultCookie${field.key}`"
                data-settings-system
                type="number"
                :min="field.min"
                :placeholder="String(policyInherited(field.key))"
                :model-value="policyValue(field.key)"
                class="w-full"
                @blur="endPolicyEdit(field.key)"
                @update:model-value="
                  (value: string | number) => setPolicyValue(field.key, value)
                "
              />
            </FieldContent>
          </Field>
        </FieldGroup>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>
          Fields
        </FieldLegend>
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-x-4 gap-y-3">
          <div
            v-for="role in FIELD_ROLE_DEFS"
            :key="role.key"
            class="flex flex-col gap-2"
          >
            <div class="flex items-center justify-between gap-2 min-h-6">
              <Label>{{ role.label }}</Label>
              <button
                v-if="isFieldOrderConfigured(role.key)"
                type="button"
                class="text-xs opacity-70 hover:opacity-100 transition-opacity"
                title="Restore the built-in order"
                @click="resetFieldOrder(role.key)"
              >
                Reset
              </button>
            </div>
            <ul class="flex flex-col gap-1">
              <li
                v-for="(field, index) in fieldList(role.key)"
                :key="field"
                draggable="true"
                class="flex items-center gap-1.5 rounded-md px-1 py-1 transition-colors cursor-grab active:cursor-grabbing"
                :class="[
                  isDragging(role.key, index) ? 'opacity-40' : '',
                  isDropTarget(role.key, index) ? 'bg-accent/15' : '',
                ]"
                @dragstart="onDragStart(role.key, index, $event)"
                @dragover.prevent="onDragOver(role.key, index)"
                @drop.prevent="onDrop(role.key, index)"
                @dragend="resetDrag"
              >
                <IconDrag
                  class="w-4 h-4 shrink-0 opacity-50"
                  aria-hidden="true"
                />
                <span
                  class="font-mono text-[0.8125rem] flex-1 min-w-0 wrap-anywhere"
                >
                  {{ field }}
                </span>
              </li>
            </ul>
          </div>
        </div>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>
          Templates
        </FieldLegend>
        <FieldGroup>
          <Field>
            <FieldLabel :for="FOLDER_INPUT_ID">Folder</FieldLabel>
            <FieldContent>
              <Input
                :id="FOLDER_INPUT_ID"
                :model-value="settingsDraft.template_settings.folder_template"
                placeholder="{{username}}"
                @focus="recordFocus(FOLDER_INPUT_ID)"
                @update:model-value="
                  (v) => setTemplate(FOLDER_INPUT_ID, String(v))
                "
              />
            </FieldContent>
          </Field>
          <Field>
            <FieldLabel :for="FILENAME_INPUT_ID">Filename</FieldLabel>
            <FieldContent>
              <Input
                :id="FILENAME_INPUT_ID"
                :model-value="settingsDraft.template_settings.filename_template"
                placeholder="{{username}} - {{title}} [{{id}}]"
                @focus="recordFocus(FILENAME_INPUT_ID)"
                @update:model-value="
                  (v) => setTemplate(FILENAME_INPUT_ID, String(v))
                "
              />
            </FieldContent>
          </Field>
          <div v-if="templateTokens.length" class="flex flex-wrap gap-1.5">
            <Button
              v-for="token in templateTokens"
              :key="token.key"
              variant="secondary"
              size="sm"
              type="button"
              class="font-mono text-[0.8125rem]"
              :title="token.description"
              @mousedown.prevent
              @click="insertToken(token.key)"
            >
              {{ token.key }}
            </Button>
          </div>
        </FieldGroup>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>
          Title
        </FieldLegend>
        <FieldGroup>
          <FieldGroup data-slot="checkbox-group">
            <FieldLabel
              v-for="rule in cleanupRules"
              :key="rule.key"
              class="cursor-pointer"
            >
              <Checkbox
                :checked="ruleEnabled(GLOBAL_NAMING_KEY, rule)"
                @update:checked="
                  (v: boolean) => setRule(GLOBAL_NAMING_KEY, rule, Boolean(v))
                "
              />
              <span>{{ rule.label }}</span>
            </FieldLabel>
          </FieldGroup>
          <Field
            v-if="ruleEnabled(GLOBAL_NAMING_KEY, titleLengthRule)"
          >
            <FieldLabel for="defaultTitleMaxChars">Maximum length</FieldLabel>
            <FieldContent class="flex-row items-center gap-2">
              <Input
                id="defaultTitleMaxChars"
                type="number"
                min="12"
                :model-value="String(maxChars(GLOBAL_NAMING_KEY))"
                class="w-24 shrink-0"
                @update:model-value="
                  (v: string | number) =>
                    setMaxChars(GLOBAL_NAMING_KEY, Number(v))
                "
              />
              <span class="text-white/55 in-[.light-mode]:text-black/55">
                characters
              </span>
            </FieldContent>
          </Field>
        </FieldGroup>
      </FieldSet>

      <FieldSeparator />

      <FieldSet>
        <FieldLegend>
          Filename
        </FieldLegend>
        <FieldGroup>
          <SegmentedControl
            v-for="choice in namingChoices"
            :key="choice.key"
            class="max-w-full overflow-x-auto"
            :label="choice.label"
            label-placement="start"
            :model-value="choiceValue(GLOBAL_NAMING_KEY, choice)"
            @update:model-value="(v: string | string[]) => onChoice(choice, v)"
          >
            <SegmentedControlItem
              v-for="option in choice.options"
              :key="option.value"
              :value="option.value"
            >
              {{ option.label }}
            </SegmentedControlItem>
          </SegmentedControl>
          <Field>
            <FieldLabel for="defaultStemMaxChars">Maximum length</FieldLabel>
            <FieldContent class="flex-row items-center gap-2">
              <Input
                id="defaultStemMaxChars"
                type="number"
                min="0"
                placeholder="Off"
                :model-value="
                  stemMaxChars(GLOBAL_NAMING_KEY)
                    ? String(stemMaxChars(GLOBAL_NAMING_KEY))
                    : ''
                "
                class="w-24 shrink-0"
                @update:model-value="
                  (v: string | number) =>
                    setStemMaxChars(GLOBAL_NAMING_KEY, Number(v || 0))
                "
              />
              <span class="text-white/55 in-[.light-mode]:text-black/55">
                characters
              </span>
            </FieldContent>
          </Field>
        </FieldGroup>
      </FieldSet>
    </div>
  </TooltipProvider>
</template>
