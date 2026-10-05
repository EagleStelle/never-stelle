import type { TrackerSettings } from "@/types";

type TrackerNumberField = Exclude<keyof TrackerSettings, "interval_seconds" | "skip_lives">;

export interface TrackerFieldDef<K extends TrackerNumberField = TrackerNumberField> {
  key: K;
  label: string;
  help: string;
  min: number;
  max: number;
}

export interface TrackerLimitDef extends TrackerFieldDef<"max_size_mb" | "max_minutes"> {
  unit: string;
  // Values the slider steps through; 0 is no limit.
  stops: number[];
  // The value as a person reads it, as "1 GB".
  format: (value: number) => string;
}

interface TrackerFlagDef {
  key: "skip_lives";
  label: string;
  help: string;
}

type TrackerFieldEntry = TrackerFieldDef | TrackerLimitDef | TrackerFlagDef;

const TRACKER_COUNT_FIELDS: TrackerFieldDef<"page_size" | "caught_up_after">[] = [
  {
    key: "page_size",
    label: "Batch size",
    help:
      "New items one check queues. The next check starts with anything posted since, then continues with older ones.",
    min: 1,
    max: 500,
  },
  {
    key: "caught_up_after",
    label: "Match streak",
    help:
      "After this many saved items in a row, a check knows nothing is new. It then goes back to where it last stopped, or ends if there is nothing left. Raise it if old posts are pinned at the top.",
    min: 1,
    max: 500,
  },
];

function formatSize(megabytes: number): string {
  if (!megabytes) return "Off";
  return megabytes >= 1000 ? `${Number((megabytes / 1000).toFixed(1))} GB` : `${megabytes} MB`;
}

function formatLength(minutes: number): string {
  if (!minutes) return "Off";
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return [hours && `${hours} h`, rest && `${rest} min`].filter(Boolean).join(" ");
}

// The tracker dialog shows these too.
export const TRACKER_LIMIT_FIELDS: (TrackerLimitDef | TrackerFlagDef)[] = [
  {
    key: "max_size_mb",
    label: "Max size",
    help: "Skips files bigger than this. Set 0 to allow any size.",
    unit: "MB",
    min: 0,
    max: 1_000_000,
    stops: [0, 100, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000],
    format: formatSize,
  },
  {
    key: "max_minutes",
    label: "Max length",
    help:
      "Skips videos longer than this. Set 0 to allow any length. Videos with unknown length still download.",
    unit: "min",
    min: 0,
    max: 7 * 24 * 60,
    stops: [0, 5, 10, 15, 30, 45, 60, 90, 120, 180, 240, 360],
    format: formatLength,
  },
  {
    key: "skip_lives",
    label: "Skip live streams",
    help: "Live streams and their replays are not downloaded.",
  },
];

// The tracker panes' field groups, each under its own legend.
export const TRACKER_FIELD_GROUPS: { legend: string; fields: TrackerFieldEntry[] }[] = [
  { legend: "Fetching", fields: TRACKER_COUNT_FIELDS },
  { legend: "Limits", fields: TRACKER_LIMIT_FIELDS },
];

export function isFlagField(field: TrackerFieldEntry): field is TrackerFlagDef {
  return !("min" in field);
}

export function isLimitField(field: TrackerFieldEntry): field is TrackerLimitDef {
  return "stops" in field;
}

// The highest stop at or below the value, so a typed value shows where it falls.
export function stopIndex(stops: number[], value: number): number {
  return Math.max(0, stops.filter((stop) => stop <= value).length - 1);
}
