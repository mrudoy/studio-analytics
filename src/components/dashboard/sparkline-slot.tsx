"use client";

import * as React from "react";
import { cn } from "@/lib/utils";

interface SparklineSlotProps extends React.ComponentProps<"div"> {}

function SparklineSlot({ className, ...props }: SparklineSlotProps) {
  return (
    <div className={cn("shrink-0 w-full", className)} {...props} />
  );
}

export { SparklineSlot };
