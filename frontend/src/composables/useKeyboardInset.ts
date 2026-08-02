import { onScopeDispose, ref, toValue, watch } from "vue";
import type { MaybeRefOrGetter } from "vue";

/** Below this the shrink is a browser toolbar, not the on-screen keyboard. */
const KEYBOARD_THRESHOLD = 80;

const isEditableElement = (element: Element | null): boolean => {
  if (typeof HTMLElement === "undefined" || !(element instanceof HTMLElement)) {
    return false;
  }

  const tagName = element.tagName.toLowerCase();
  return element.isContentEditable || tagName === "input" || tagName === "textarea";
};

/**
 * Tracks the on-screen keyboard.
 *
 * iOS Safari overlays the virtual keyboard on top of fixed elements, while
 * Android often resizes the viewport. Track the visual viewport itself so fixed
 * sheets can be positioned inside the currently visible rectangle.
 */
export function useKeyboardInset(active: MaybeRefOrGetter<boolean>) {
  const inset = ref(0);
  const keyboardOpen = ref(false);
  const viewportTop = ref(0);
  const viewportHeight = ref(0);
  const viewportBottom = ref(0);
  const baselineHeight = ref(0);

  let frame = 0;
  let timers: number[] = [];

  const updateNow = () => {
    if (typeof window === "undefined") return;

    const viewport = window.visualViewport;
    const top = Math.max(0, Math.round(viewport?.offsetTop ?? 0));
    const height = Math.round(viewport?.height ?? window.innerHeight);
    const bottom = top + height;
    const layoutHeight = Math.round(window.innerHeight);
    const overlap = Math.max(0, layoutHeight - bottom);
    const editableFocused = isEditableElement(document.activeElement);

    if (!editableFocused || height > baselineHeight.value) {
      baselineHeight.value = height;
    }

    const shrink = Math.max(0, baselineHeight.value - height);
    const isKeyboardOpen =
      editableFocused && (overlap > KEYBOARD_THRESHOLD || shrink > KEYBOARD_THRESHOLD);

    keyboardOpen.value = isKeyboardOpen;
    inset.value = isKeyboardOpen && overlap > KEYBOARD_THRESHOLD ? Math.round(overlap) : 0;
    viewportTop.value = top;
    viewportHeight.value = height;
    viewportBottom.value = bottom;
  };

  const scheduleUpdate = () => {
    if (typeof window === "undefined" || frame) return;
    frame = window.requestAnimationFrame(() => {
      frame = 0;
      updateNow();
    });
  };

  const scheduleSettledUpdates = () => {
    if (typeof window === "undefined") return;
    scheduleUpdate();
    timers.push(
      window.setTimeout(scheduleUpdate, 50),
      window.setTimeout(scheduleUpdate, 150),
      window.setTimeout(scheduleUpdate, 300),
    );
  };

  const detach = () => {
    if (typeof window === "undefined") return;

    const viewport = window.visualViewport;
    viewport?.removeEventListener("resize", scheduleSettledUpdates);
    viewport?.removeEventListener("scroll", scheduleSettledUpdates);
    window.removeEventListener("resize", scheduleSettledUpdates);
    window.removeEventListener("orientationchange", scheduleSettledUpdates);
    document.removeEventListener("focusin", scheduleSettledUpdates);
    document.removeEventListener("focusout", scheduleSettledUpdates);

    if (frame) {
      window.cancelAnimationFrame(frame);
      frame = 0;
    }
    timers.forEach((timer) => window.clearTimeout(timer));
    timers = [];

    inset.value = 0;
    keyboardOpen.value = false;
    viewportTop.value = 0;
    viewportHeight.value = 0;
    viewportBottom.value = 0;
  };

  watch(
    () => toValue(active),
    (isActive) => {
      if (!isActive) {
        detach();
        return;
      }

      if (typeof window === "undefined") return;

      const viewport = window.visualViewport;
      updateNow();
      scheduleSettledUpdates();
      viewport?.addEventListener("resize", scheduleSettledUpdates);
      viewport?.addEventListener("scroll", scheduleSettledUpdates);
      window.addEventListener("resize", scheduleSettledUpdates);
      window.addEventListener("orientationchange", scheduleSettledUpdates);
      document.addEventListener("focusin", scheduleSettledUpdates);
      document.addEventListener("focusout", scheduleSettledUpdates);
    },
    { immediate: true },
  );

  onScopeDispose(detach);

  return { inset, keyboardOpen, viewportTop, viewportHeight, viewportBottom };
}
