import type { VariantProps } from "class-variance-authority"
import { cva } from "class-variance-authority"

export const fieldVariants = cva(
  [
    "group/field flex w-full flex-col gap-2 sm:flex-row sm:items-center sm:gap-3 data-[invalid=true]:text-destructive",
    "max-sm:*:data-[slot=field-label]:w-auto sm:*:data-[slot=field-label]:shrink-0",
    "*:data-[slot=field-label]:leading-normal",
    "*:data-[slot=field-content]:w-full *:data-[slot=field-content]:flex-none sm:*:data-[slot=field-content]:flex-auto",
  ],
  {
    variants: {
      labelWidth: {
        xs: "sm:*:data-[slot=field-label]:w-16",
        sm: "sm:*:data-[slot=field-label]:w-28",
        md: "sm:*:data-[slot=field-label]:w-40",
        lg: "sm:*:data-[slot=field-label]:w-48",
      },
    },
    defaultVariants: {
      labelWidth: "md",
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
