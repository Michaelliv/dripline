import { cva } from "class-variance-authority";

export const listRow = cva(
  "flex items-baseline gap-3 overflow-hidden rounded-md px-3 py-0.5 cursor-pointer transition-colors duration-75",
  {
    variants: {
      selected: {
        true: "bg-shift-accent/10",
        false: "hover:bg-shift-accent/5",
      },
    },
    defaultVariants: {
      selected: false,
    },
  },
);

export const listPrimaryText = cva("shrink-0 font-medium", {
  variants: {
    selected: {
      true: "text-shift-accent",
      false: "text-shift-text",
    },
  },
  defaultVariants: {
    selected: false,
  },
});
