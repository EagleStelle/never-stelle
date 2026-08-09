import type { QualityOptions, QualityPreset, QualitySelection } from "@/types";
import {
  isLosslessAudioFormat,
  videoAudioCodecOptionsForContainer,
  videoCodecOptionsForContainer,
} from "@/utils/dashboard";

/** One combobox in the quality strip; `key` is the selection field it writes. */
export interface QualityField {
  key: Exclude<keyof QualitySelection, "mode">;
  items: QualityPreset[];
  placeholder: string;
  emptyText: string;
  /** Visible caption and accessible name; the mode control supplies the rest of the context. */
  label: string;
  /** Technical setting that belongs in the Advanced Settings dialog rather than main toolbar strip. */
  isAdvanced?: boolean;
}

/** The pickers the current mode can offer, minus any the backend has no presets for. */
export function qualityFieldsFor(
  selection: QualitySelection,
  options: QualityOptions,
): QualityField[] {
  const fields: QualityField[] =
    selection.mode === "audio"
      ? [
          {
            key: "audio_bitrate",
            items: isLosslessAudioFormat(selection.audio_format)
              ? // Lossless ignores a target bitrate, so the control drops out.
                []
              : options.audio_bitrates,
            placeholder: "Select...",
            emptyText: "No bitrates.",
            label: "Bitrate",
            isAdvanced: false,
          },
          {
            key: "audio_format",
            items: options.audio_formats,
            placeholder: "Select...",
            emptyText: "No formats.",
            label: "Format",
            isAdvanced: true,
          },
        ]
      : [
          {
            key: "video_quality",
            items: options.video,
            placeholder: "Select...",
            emptyText: "No presets.",
            label: "Quality",
            isAdvanced: false,
          },
          {
            key: "video_container",
            items: options.video_containers,
            placeholder: "Select...",
            emptyText: "No containers.",
            label: "Container",
            isAdvanced: true,
          },
          {
            key: "video_codec",
            // Only codecs the chosen container can play back (Auto always fits);
            // prevents VP9-in-MP4.
            items: videoCodecOptionsForContainer(
              options,
              selection.video_container,
            ),
            placeholder: "Select...",
            emptyText: "No codecs.",
            label: "Video codec",
            isAdvanced: true,
          },
          {
            key: "video_audio_codec",
            items: videoAudioCodecOptionsForContainer(
              options,
              selection.video_container,
            ),
            placeholder: "Select...",
            emptyText: "No audio codecs.",
            label: "Audio codec",
            isAdvanced: true,
          },
        ];

  return fields.filter((field) => field.items.length);
}
