import type { Component } from "vue";
import IconMovie from "~icons/material-symbols/movie";
import IconMusicVideo from "~icons/material-symbols/music-video";
import IconMusic from "~icons/material-symbols/music-note";

import type { MediaMode, QualityOptions, QualityPreset, QualitySelection } from "@/types";
import {
  audioFormatOptionsForContainer,
  isLosslessAudioFormat,
  videoCodecOptionsForContainer,
} from "@/utils/dashboard";

export interface MediaModeItem {
  value: MediaMode;
  label: string;
  title: string;
  icon: Component;
}

export const MEDIA_MODE_ITEMS: MediaModeItem[] = [
  { value: "merged", label: "Merged", title: "Video with audio", icon: IconMusicVideo },
  { value: "video", label: "Video only", title: "Video only", icon: IconMovie },
  { value: "audio", label: "Audio only", title: "Audio only", icon: IconMusic },
];

/** One combobox in the quality strip; `key` is the selection field it writes. */
export interface QualityField {
  key: Exclude<keyof QualitySelection, "mode">;
  items: QualityPreset[];
  placeholder: string;
  emptyText: string;
  /** Visible caption and accessible name; the mode control supplies the rest of the context. */
  label: string;
}

export interface QualityFieldGroup {
  legend: string;
  fields: QualityField[];
}

/** The pickers the mode uses, grouped by track, minus any the backend has no presets for. */
export function qualityFieldGroups(
  selection: QualitySelection,
  options: QualityOptions,
): QualityFieldGroup[] {
  const groups: QualityFieldGroup[] = [];
  if (selection.mode !== "audio") {
    groups.push({
      legend: "Video",
      fields: [
        {
          key: "video_quality",
          items: options.video,
          placeholder: "Select...",
          emptyText: "No presets.",
          label: "Quality",
        },
        {
          key: "video_container",
          items: options.video_containers,
          placeholder: "Select...",
          emptyText: "No containers.",
          label: "Container",
        },
        {
          key: "video_codec",
          // Only codecs the chosen container can play back (Auto always fits);
          // prevents VP9-in-MP4.
          items: videoCodecOptionsForContainer(options, selection.video_container),
          placeholder: "Select...",
          emptyText: "No codecs.",
          label: "Codec",
        },
      ],
    });
  }
  if (selection.mode !== "video") {
    groups.push({
      legend: "Audio",
      fields: [
        {
          key: "audio_bitrate",
          // Lossless ignores a target bitrate, so the control drops out.
          items: isLosslessAudioFormat(selection.audio_format) ? [] : options.audio_bitrates,
          placeholder: "Select...",
          emptyText: "No bitrates.",
          label: "Bitrate",
        },
        {
          key: "audio_format",
          // Merged puts the audio in the video container, so only formats it can hold.
          items:
            selection.mode === "merged"
              ? audioFormatOptionsForContainer(options, selection.video_container)
              : options.audio_formats,
          placeholder: "Select...",
          emptyText: "No formats.",
          label: "Format",
        },
      ],
    });
  }
  return groups
    .map((group) => ({ ...group, fields: group.fields.filter((field) => field.items.length) }))
    .filter((group) => group.fields.length);
}

/** The one picker the toolbar strip keeps outside Advanced Settings. */
export function quickQualityField(
  groups: QualityFieldGroup[],
  mode: MediaMode,
): QualityField | undefined {
  const key = mode === "audio" ? "audio_bitrate" : "video_quality";
  return groups.flatMap((group) => group.fields).find((field) => field.key === key);
}
