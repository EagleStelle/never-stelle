import { computed, inject, provide, ref, watch, type ComponentPublicInstance, type InjectionKey, type Ref } from "vue";
import { unrefElement, type MaybeElementRef } from "@vueuse/core";
import { useVirtualizer, type Virtualizer } from "@tanstack/vue-virtual";

const SCROLL_ROOT: InjectionKey<Readonly<Ref<HTMLElement | null>>> = Symbol("scroll-root");

// Names the element that lists below it scroll inside.
export function provideScrollRoot(root: Readonly<Ref<HTMLElement | null>>): void {
  provide(SCROLL_ROOT, root);
}

interface VirtualRowsOptions {
  count: Readonly<Ref<number>>;
  estimate: number;
  overscan: number;
  gap?: Readonly<Ref<number>>;
}

// Mounts only the rows near the viewport. The rest are held as space before and after them.
export function useVirtualRows(list: MaybeElementRef, { count, estimate, overscan, gap }: VirtualRowsOptions) {
  const root = inject(SCROLL_ROOT, ref(null));
  // Where row 0 sits in the root's content. It moves when anything above the list resizes.
  const margin = ref(0);

  function measureMargin(): void {
    const start = unrefElement(list);
    const box = root.value;
    if (!start || !box) return;
    margin.value = Math.round(start.getBoundingClientRect().top - box.getBoundingClientRect().top + box.scrollTop);
  }

  watch(() => unrefElement(list), measureMargin, { flush: "post" });
  watch(
    root,
    (box, _, onCleanup) => {
      if (!box) return;
      // Browser scroll anchoring would shift the view each time the padding swaps for mounted rows.
      box.style.overflowAnchor = "none";
      const observer = new ResizeObserver(measureMargin);
      for (const element of [box, ...box.children]) observer.observe(element);
      onCleanup(() => observer.disconnect());
    },
    { immediate: true },
  );

  const virtualizer = useVirtualizer(
    computed(() => ({
      count: count.value,
      getScrollElement: () => root.value,
      estimateSize: () => estimate,
      overscan,
      gap: gap?.value,
      scrollMargin: margin.value,
      // Sizes come from the resize observer only, so mounting a row never forces a layout.
      measureElement: (element: Element, entry: ResizeObserverEntry | undefined, instance: Virtualizer<HTMLElement, Element>) => {
        const size = entry?.borderBoxSize[0]?.blockSize;
        return size === undefined ? (instance.measurementsCache[instance.indexFromElement(element)]?.size ?? estimate) : Math.round(size);
      },
    })),
  );

  const rows = computed(() => virtualizer.value.getVirtualItems());
  const before = computed(() => (rows.value.length ? rows.value[0].start - margin.value : 0));
  const after = computed(() => {
    const last = rows.value.at(-1);
    return last ? virtualizer.value.getTotalSize() - (last.end - margin.value) : 0;
  });

  // Ref callback for mounted rows. Only elements carrying data-index are measured.
  function measure(target: Element | ComponentPublicInstance | null): void {
    const element = unrefElement(target as MaybeElementRef) as HTMLElement | null | undefined;
    if (!element) virtualizer.value.measureElement(null);
    else if (element.dataset.index !== undefined) virtualizer.value.measureElement(element);
  }

  return { rows, before, after, measure };
}
