import type { CookiePolicyField } from "@/types";

export interface CookiePolicyFieldDef {
  key: CookiePolicyField;
  label: string;
  help: string;
  min: string;
}

// One description of every rate-limit knob, shared by the global Defaults pane and the
// per-source Cookies pane so the two can never drift apart.
// A field starts filled with the value it inherits; typing that same value back drops
// the override so the field keeps tracking its source.
export const COOKIE_POLICY_FIELDS: CookiePolicyFieldDef[] = [
  {
    key: "limit",
    label: "Uses before rotate",
    help: "Downloads a cookies file handles before the next file takes over.",
    min: "1",
  },
  {
    key: "window",
    label: "Quota resets after",
    help: "Seconds before that use count starts over from zero.",
    min: "1",
  },
  {
    key: "delay",
    label: "Rest between uses",
    help: "Seconds a cookies file rests between two of its own downloads.",
    min: "0",
  },
  {
    key: "cooldown",
    label: "Rest after block",
    help: "Seconds a cookies file rests after the site blocks or rate-limits it.",
    min: "0",
  },
  {
    key: "wait",
    label: "Download wait limit",
    help: "Seconds a download waits for a free cookies file before continuing without one.",
    min: "0",
  },
];
