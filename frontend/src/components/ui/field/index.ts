import type { VariantProps } from "class-variance-authority"
import { cva } from "class-variance-authority"

export const fieldVariants = cva(
  "group/field flex w-full gap-3 data-[invalid=true]:text-destructive",
  {
    variants: {
      orientation: {
        vertical: ["flex-col [&>*]:w-full [&>.sr-only]:w-auto"],
        horizontal: [
          "flex-row items-center",
          "[&>[data-slot=field-label]]:flex-auto",
          "has-[>[data-slot=field-content]]:items-start has-[>[data-slot=field-content]]:[&>[role=checkbox],[role=radio]]:mt-px",
        ],
        responsive: [
          "flex-col [&>*]:w-full [&>.sr-only]:w-auto @md/field-group:flex-row @md/field-group:items-center @md/field-group:[&>*]:w-auto",
          "@md/field-group:[&>[data-slot=field-label]]:flex-auto",
          "@md/field-group:has-[>[data-slot=field-content]]:items-start @md/field-group:has-[>[data-slot=field-content]]:[&>[role=checkbox],[role=radio]]:mt-px",
        ],
        // The two orientations below pin the label to the Label default leading, so a
        // FieldLabel reads the same as the plain Labels used elsewhere.

        // Toolbar caption: a tight stack that hugs its control instead of filling the row.
        top: [
          "flex-col items-start gap-1",
          "*:data-[slot=field-label]:leading-normal",
          "*:data-[slot=field-content]:w-full *:data-[slot=field-content]:flex-none",
        ],
        // Settings row: the label stacks above on phones, then takes a fixed column at the
        // start of the row from sm up.
        start: [
          "flex-col gap-2 sm:flex-row sm:items-center sm:gap-3",
          // Stacked, the label spans the row; beside the control it takes the label column.
          "max-sm:*:data-[slot=field-label]:w-auto sm:*:data-[slot=field-label]:shrink-0",
          "*:data-[slot=field-label]:leading-normal",
          "*:data-[slot=field-content]:w-full *:data-[slot=field-content]:flex-none sm:*:data-[slot=field-content]:flex-auto",
        ],
      },
      // Width of the label column, which only the `start` orientation lays out. Field
      // applies `md` there by default.
      labelWidth: {
        xs: "sm:*:data-[slot=field-label]:w-16",
        sm: "sm:*:data-[slot=field-label]:w-28",
        md: "sm:*:data-[slot=field-label]:w-40",
        lg: "sm:*:data-[slot=field-label]:w-48",
      },
    },
    defaultVariants: {
      orientation: "vertical",
    },
  },
)

export type FieldVariants = VariantProps<typeof fieldVariants>

export { default as Field } from "@/components/ui/field/Field.vue"
export { default as FieldContent } from "@/components/ui/field/FieldContent.vue"
export { default as FieldDescription } from "@/components/ui/field/FieldDescription.vue"
export { default as FieldError } from "@/components/ui/field/FieldError.vue"
export { default as FieldGroup } from "@/components/ui/field/FieldGroup.vue"
export { default as FieldLabel } from "@/components/ui/field/FieldLabel.vue"
export { default as FieldLegend } from "@/components/ui/field/FieldLegend.vue"
export { default as FieldSeparator } from "@/components/ui/field/FieldSeparator.vue"
export { default as FieldSet } from "@/components/ui/field/FieldSet.vue"
export { default as FieldTitle } from "@/components/ui/field/FieldTitle.vue"
