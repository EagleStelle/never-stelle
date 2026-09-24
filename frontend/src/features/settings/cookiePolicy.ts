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
    label: "Use limit",
    help: "Times a cookie file can be used in one use window.",
    min: "1",
  },
  {
    key: "window",
    label: "Use window",
    help: "Seconds until the use count starts over.",
    min: "1",
  },
  {
    key: "delay",
    label: "Use gap",
    help: "Seconds a cookie file rests between uses.",
    min: "0",
  },
  {
    key: "interval",
    label: "Request gap",
    help: "Seconds a cookie file pauses between requests.",
    min: "0",
  },
  {
    key: "wait",
    label: "Wait limit",
    help: "Seconds to wait for a free cookie file before going without.",
    min: "0",
  },
  {
    key: "cooldown",
    label: "Ban cooldown",
    help: "Seconds a cookie file rests after a site blocks it.",
    min: "0",
  },
];
