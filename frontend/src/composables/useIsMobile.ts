import { useMediaQuery } from "@vueuse/core";

/** Below Tailwind's `sm` breakpoint we swap anchored popovers for bottom sheets. */
const MOBILE_QUERY = "(max-width: 639px)";

export function useIsMobile() {
  return useMediaQuery(MOBILE_QUERY);
}
