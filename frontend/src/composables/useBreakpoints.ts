import { useMediaQuery } from "@vueuse/core";

/** Below Tailwind's `sm` breakpoint we swap anchored popovers for bottom sheets. */
const MOBILE_QUERY = "(max-width: 639px)";
/** Tailwind's `lg`: the shell docks its toolbar to the top instead of the bottom. */
const DESKTOP_QUERY = "(min-width: 1024px)";

export function useIsMobile() {
  return useMediaQuery(MOBILE_QUERY);
}

export function useIsDesktop() {
  return useMediaQuery(DESKTOP_QUERY);
}
