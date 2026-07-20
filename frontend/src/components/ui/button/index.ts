import type { VariantProps } from "class-variance-authority";
import { cva } from "class-variance-authority";

export { default as Button } from "./Button.vue";

const PRIMARY =
  "border border-(--glass-border) bg-accent text-black hover:bg-accent/45";

export const buttonVariants = cva(
  [
    "group inline-flex items-center justify-center gap-1.5 rounded-lg text-sm font-medium leading-none whitespace-nowrap",
    "transition-all duration-300 ease-glass active:scale-[0.96]",
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent",
    "disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 disabled:active:scale-100",
    "[&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
  ].join(" "),
  {
    variants: {
      variant: {
        default: PRIMARY,
        primary: PRIMARY,
        secondary: PRIMARY,
        soft: PRIMARY,
        outline: PRIMARY,
        ghost:
          "border border-transparent bg-transparent text-white/70 hover:bg-white/10 hover:text-white in-[.light-mode]:text-black/70 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black",
        cancel:
          "glass-soft text-white hover:bg-white/10 in-[.light-mode]:text-black in-[.light-mode]:hover:bg-black/5",
        destructive:
          "border border-(--glass-border) bg-destructive text-destructive-foreground hover:bg-destructive/85",
        danger:
          "border border-(--glass-border) bg-destructive text-destructive-foreground hover:bg-destructive/85",
        link: "h-auto p-0 text-accent underline-offset-4 hover:underline active:scale-100",
      },
      size: {
        default: "h-9 px-4",
        sm: "h-8 px-3 text-xs",
        lg: "h-10 px-5",
        icon: "h-9 w-9 p-0",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

export type ButtonVariants = VariantProps<typeof buttonVariants>;
