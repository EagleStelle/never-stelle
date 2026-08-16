<script setup lang="ts">
import type { HTMLAttributes, InputHTMLAttributes } from "vue";
import { computed, ref, useAttrs, useSlots } from "vue";
import { useVModel } from "@vueuse/core";
import IconPaste from "~icons/material-symbols/content-paste-rounded";
import IconVisibility from "~icons/material-symbols/visibility";
import IconVisibilityOff from "~icons/material-symbols/visibility-off";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const CONTAINER =
  "flex items-center min-w-0 h-9 p-1 gap-1 rounded-lg bg-black/20 in-[.light-mode]:bg-white/40 border border-(--glass-border) shadow-inner focus-within:ring-2 focus-within:ring-accent transition-all duration-300 ease-glass";
const ICON =
  "flex items-center justify-center shrink-0 w-7 h-7 ml-1 text-white in-[.light-mode]:text-black";
// Stands in for the icon so a field without one still opens on the same text inset.
const GAP = "w-1";
const TEXT =
  "flex-1 min-w-0 h-full bg-transparent outline-none text-base md:text-sm text-white in-[.light-mode]:text-black pr-2 placeholder:text-white/50 in-[.light-mode]:placeholder:text-black/50";
const DISABLED_CONTAINER =
  "border-white/10 bg-black/10 shadow-none in-[.light-mode]:border-black/10 in-[.light-mode]:bg-black/5";
const DISABLED_TEXT =
  "pointer-events-none cursor-default text-white/55 placeholder:text-white/30 in-[.light-mode]:text-black/50 in-[.light-mode]:placeholder:text-black/30";

const props = defineProps<{
  defaultValue?: string | number;
  modelValue?: string | number | null;
  class?: HTMLAttributes["class"];
  // `true` pastes the clipboard as it comes; a function pastes what it returns and
  // cleans keyboard pastes too, so a field's own rules live with the field.
  paste?: boolean | ((text: string) => string);
}>();

const emits = defineEmits<{
  (e: "update:modelValue", payload: string | number): void;
}>();

const modelValue = useVModel(props, "modelValue", emits, {
  passive: true,
  defaultValue: props.defaultValue,
});

defineOptions({
  inheritAttrs: false,
});

const attrs = useAttrs();
const slots = useSlots();
const revealed = ref(false);

// Revealing swaps the rendered type only: `attrs.type` stays `password`, so the field
// keeps its toggle and its password autofill while the text is readable.
const isPassword = computed(() => attrs.type === "password");
const type = computed<InputHTMLAttributes["type"]>(() =>
  isPassword.value && revealed.value
    ? "text"
    : (attrs.type as InputHTMLAttributes["type"]),
);
const clean = computed(() =>
  typeof props.paste === "function" ? props.paste : null,
);
const disabled = computed(
  () =>
    attrs.disabled === "" ||
    attrs.disabled === true ||
    attrs.disabled === "true",
);

// Text that survives cleaning pastes natively, so the caret and selection replacement
// behave normally; only rewritten text takes over the field.
function onPaste(event: ClipboardEvent): void {
  if (!clean.value) return;
  const raw = event.clipboardData?.getData("text") || "";
  const cleaned = clean.value(raw);
  if (cleaned === raw.trim()) return;
  event.preventDefault();
  modelValue.value = cleaned;
}

// The button never fires a paste event, so it reads the clipboard itself.
async function pasteFromClipboard(): Promise<void> {
  try {
    const text = await navigator.clipboard.readText();
    modelValue.value = clean.value ? clean.value(text) : text.trim();
  } catch (err) {
    console.error("Failed to read clipboard contents:", err);
  }
}
</script>

<template>
  <div
    :data-disabled="disabled ? 'true' : undefined"
    :class="
      cn(
        CONTAINER,
        disabled && DISABLED_CONTAINER,
        props.class,
        attrs.class as string,
      )
    "
  >
    <div
      v-if="slots.icon"
      :class="
        cn(ICON, disabled && 'text-white/55 in-[.light-mode]:text-black/55')
      "
    >
      <slot name="icon" />
    </div>
    <div v-else :class="GAP" />

    <input
      v-bind="{ ...attrs, class: undefined, type }"
      v-model="modelValue"
      data-slot="input"
      @paste="onPaste"
      @click="
        ($event as MouseEvent).detail === 3 &&
          ($event.target as HTMLInputElement).select()
      "
      :class="cn(TEXT, disabled && DISABLED_TEXT)"
    />

    <Button
      v-if="props.paste"
      type="button"
      variant="ghost"
      size="icon-sm"
      class="mr-0.5"
      :disabled="disabled"
      aria-label="Paste from clipboard"
      @click="pasteFromClipboard"
    >
      <template #icon>
        <IconPaste class="group-hover:scale-110" aria-hidden="true" />
      </template>
    </Button>

    <Button
      v-if="isPassword"
      type="button"
      variant="ghost"
      size="icon-sm"
      class="mr-0.5"
      :disabled="disabled"
      :aria-label="revealed ? 'Hide password' : 'Show password'"
      :aria-pressed="revealed"
      @click="revealed = !revealed"
    >
      <template #icon>
        <component
          :is="revealed ? IconVisibilityOff : IconVisibility"
          class="group-hover:scale-110"
          aria-hidden="true"
        />
      </template>
    </Button>
  </div>
</template>
