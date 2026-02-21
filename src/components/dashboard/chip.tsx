"use client";

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const chipVariants = cva(
  "inline-flex items-center gap-[3px] text-xs font-medium leading-none whitespace-nowrap tabular-nums tracking-wide rounded px-2 py-0.5 transition-colors",
  {
    variants: {
      variant: {
        neutral: "text-muted-foreground bg-muted",
        positive: "text-emerald-700 bg-emerald-50",
        negative: "text-red-700 bg-red-50",
        accent: "text-foreground bg-muted",
      },
    },
    defaultVariants: {
      variant: "neutral",
    },
  }
);

interface ChipProps
  extends React.ComponentProps<"span">,
    VariantProps<typeof chipVariants> {}

function Chip({ className, variant, ...props }: ChipProps) {
  return (
    <span className={cn(chipVariants({ variant }), className)} {...props} />
  );
}

export { Chip, chipVariants };
export type { ChipProps };
