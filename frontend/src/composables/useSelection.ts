import { computed, reactive, shallowRef, watch, type Ref } from "vue";

import { SELECTABLE_ITEM, SELECTING_ITEM } from "@/ui";

export type CheckState = boolean | "indeterminate";

const HOLD_MS = 450;
const HOLD_SLOP_PX = 10;

// Checked rows of one list. A row that leaves the list stops counting without a write.
export function useSelection<T>(items: Ref<T[]>, keyOf: (item: T) => string, resetKey: Ref<string>) {
  const selected = shallowRef<ReadonlySet<string>>(new Set());
  let anchor = "";

  const selectedItems = computed(() =>
    selected.value.size ? items.value.filter((item) => selected.value.has(keyOf(item))) : [],
  );
  const count = computed(() => selectedItems.value.length);
  const total = computed(() => items.value.length);
  const state = computed<CheckState>(() => {
    if (count.value === 0) return false;
    return count.value === total.value ? true : "indeterminate";
  });

  function isSelected(item: T): boolean {
    return selected.value.has(keyOf(item));
  }

  // With `range`, every row from the last one toggled takes this row's new state.
  function toggle(item: T, range = false): void {
    const keys = items.value.map(keyOf);
    const key = keyOf(item);
    const on = !selected.value.has(key);
    const from = range && anchor ? keys.indexOf(anchor) : -1;
    const to = keys.indexOf(key);
    const span = from >= 0 && to >= 0 ? keys.slice(Math.min(from, to), Math.max(from, to) + 1) : [key];
    const next = new Set(selected.value);
    for (const each of span) {
      if (on) next.add(each);
      else next.delete(each);
    }
    anchor = key;
    selected.value = next;
  }

  function setAll(on: boolean): void {
    selected.value = new Set(on ? items.value.map(keyOf) : []);
    anchor = "";
  }

  watch(resetKey, () => setAll(false));

  // A touch hold in progress, and whether the last one fired.
  let hold: { item: T; x: number; y: number; timer: number } | null = null;
  let held = false;

  function endHold(): void {
    if (hold) window.clearTimeout(hold.timer);
    hold = null;
  }

  function completeHold(): void {
    if (!hold) return;
    const { item } = hold;
    endHold();
    held = true;
    if (isSelected(item)) return;
    toggle(item);
    navigator.vibrate?.(10);
  }

  // Bindings for one list item. A touch hold selects it. While anything is selected,
  // a press anywhere on the item toggles it and nothing inside it runs.
  function itemProps(item: T, open?: () => void) {
    return {
      class: [SELECTABLE_ITEM, count.value ? SELECTING_ITEM : ""],
      onPointerdown(event: PointerEvent): void {
        held = false;
        endHold();
        if (event.pointerType === "mouse" || !event.isPrimary) return;
        hold = { item, x: event.clientX, y: event.clientY, timer: window.setTimeout(completeHold, HOLD_MS) };
      },
      onPointermove(event: PointerEvent): void {
        if (hold && Math.hypot(event.clientX - hold.x, event.clientY - hold.y) > HOLD_SLOP_PX) endHold();
      },
      onPointerup: endHold,
      onPointercancel: endHold,
      onContextmenu(event: MouseEvent): void {
        if (!hold && !held) return;
        event.preventDefault();
        completeHold();
      },
      onClickCapture(event: MouseEvent): void {
        if (!held && !count.value) return;
        event.preventDefault();
        event.stopPropagation();
        if (held) held = false;
        else toggle(item, event.shiftKey);
      },
      onClick(event: MouseEvent): void {
        if (!event.defaultPrevented) open?.();
      },
      onKeydown(event: KeyboardEvent): void {
        if (event.target !== event.currentTarget || (event.key !== "Enter" && event.key !== " ")) return;
        event.preventDefault();
        if (count.value) toggle(item, event.shiftKey);
        else open?.();
      },
    };
  }

  return reactive({ selectedItems, count, total, state, isSelected, toggle, setAll, itemProps });
}

export type Selection<T> = ReturnType<typeof useSelection<T>>;
export type SelectAll = Pick<Selection<unknown>, "state" | "total" | "count" | "setAll">;
