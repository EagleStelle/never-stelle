import type { VariantProps } from "class-variance-authority";
import { cva } from "class-variance-authority";

export { default as Button } from "@/components/ui/button/Button.vue";

// Filled variants: flat fill, focus ring set off from it.
const solid =
  "border border-transparent focus-visible:ring-offset-2 focus-visible:ring-offset-background";
// Colored variants turn neutral when disabled.
const inert =
  "disabled:border-transparent disabled:bg-white/10 disabled:text-white/45 disabled:opacity-100 in-[.light-mode]:disabled:bg-black/6 in-[.light-mode]:disabled:text-black/40";

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
        primary: `${solid} ${inert} bg-accent text-accent-foreground hover:bg-accent-hover`,
        secondary: `${inert} border border-accent/30 bg-accent/12 text-accent-ink hover:border-accent/50 hover:bg-accent/20 in-[.light-mode]:border-accent/45 in-[.light-mode]:bg-accent/14 in-[.light-mode]:hover:border-accent/65 in-[.light-mode]:hover:bg-accent/22`,
        ghost:
          "border border-transparent bg-transparent text-white/70 hover:bg-white/10 hover:text-white in-[.light-mode]:text-black/70 in-[.light-mode]:hover:bg-black/5 in-[.light-mode]:hover:text-black",
        outline:
          "border border-(--glass-border) bg-transparent text-white hover:bg-white/10 in-[.light-mode]:text-black in-[.light-mode]:hover:bg-black/5",
        destructive: `${solid} ${inert} bg-destructive-solid text-destructive-foreground hover:bg-destructive-solid-hover`,
        "destructive-ghost":
          "border border-transparent bg-transparent text-destructive-ink hover:bg-destructive/15 in-[.light-mode]:hover:bg-destructive/10",
      },
      size: {
        default: "h-9 px-4 text-sm",
        sm: "h-8 px-3 text-xs",
        icon: "size-9 p-0 text-sm",
        "icon-sm": "size-8 p-0 text-xs",
      },
      // Below `sm` a labelled button keeps only its icon, squared to its height.
      compact: {
        true: "max-sm:aspect-square max-sm:px-0",
        false: "",
      },
    },
    defaultVariants: {
      variant: "primary",
      size: "default",
    },
  },
);

export type ButtonVariants = VariantProps<typeof buttonVariants>;
