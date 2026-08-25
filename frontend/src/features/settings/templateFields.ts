import type { TemplateSettings } from "@/types";

export interface TemplateFieldDef {
  key: keyof TemplateSettings;
  label: string;
  builtin: string;
  help?: string;
}

// One description of every naming template, shared by the global Defaults pane and the
// per-source Templates pane so the two can never drift apart. `builtin` is what the app
// falls back to, and what each field shows as its placeholder.
export const TEMPLATE_FIELDS: TemplateFieldDef[] = [
  {
    key: "folder_template",
    label: "Folder",
    builtin: "{{username}}",
  },
  {
    key: "subfolder_template",
    label: "Subfolder",
    builtin: "{{id}}",
    help: "Made only when a post has more than one file. Empty means no subfolder.",
  },
  {
    key: "filename_template",
    label: "Filename",
    builtin: "{{username}} - {{title}} [{{id}}]",
  },
];

export function templateFieldSlug(field: TemplateFieldDef): string {
  return field.key.replace(/_template$/, "");
}
