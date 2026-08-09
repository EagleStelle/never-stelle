import type { VariantProps } from "class-variance-authority";
import { cva } from "class-variance-authority";

export { default as Button } from "@/components/ui/button/Button.vue";

export const buttonVariants = cva(
  [
    "group inline-flex shrink-0 items-center justify-center gap-1.5 rounded-lg text-sm font-medium leading-none whitespace-nowrap",
    "transition-all duration-300 ease-glass active:scale-[0.96]",
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent",
    "disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 disabled:active:scale-100",
    "[&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
    "[&_svg]:transition-transform [&_svg]:duration-300 [&_svg]:ease-glass",
  ].join(" "),
  {
    variants: {
      variant: {
        primary:
          "border border-(--glass-border) bg-accent text-black hover:bg-accent/45",
        secondary:
          "bg-secondary text-secondary-foreground hover:bg-secondary/80 shadow-sm",
        ghost:
          "border border-transparent bg-transparent text-white/70 hover:bg-white/10 hover:text-white in-[.light-mode]:text-black/70 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black",
        cancel:
          "glass-soft text-white hover:bg-white/10 in-[.light-mode]:text-black in-[.light-mode]:hover:bg-black/5",
        destructive:
          "border border-(--glass-border) bg-destructive text-destructive-foreground hover:bg-destructive/85",
        link: "h-auto p-0 text-accent underline-offset-4 hover:underline active:scale-100",
      },
      size: {
        default: "h-9 px-4 text-sm",
        sm: "h-8 px-3 text-xs",
        lg: "h-10 px-5 text-base",
        icon: "h-9 w-9 p-0 text-sm",
        "icon-sm": "h-8 w-8 p-0 text-xs",
      },
    },
    defaultVariants: {
      variant: "primary",
      size: "default",
    },
  },
);

export type ButtonVariants = VariantProps<typeof buttonVariants>;
