import type { TrackerSettings } from "@/types";

export type TrackerCountField = Exclude<keyof TrackerSettings, "interval_seconds">;

export interface TrackerFieldDef {
  key: TrackerCountField;
  label: string;
  help: string;
}

// Shared by the global Defaults pane and the per-source Trackers pane.
export const TRACKER_COUNT_FIELDS: TrackerFieldDef[] = [
  {
    key: "page_size",
    label: "Batch size",
    help:
      "New items one check queues. The next check starts with anything posted since, then continues with older ones.",
  },
  {
    key: "caught_up_after",
    label: "Match streak",
    help:
      "After this many saved items in a row, a check knows nothing is new. It then goes back to where it last stopped, or ends if there is nothing left. Raise it if old posts are pinned at the top.",
  },
];
