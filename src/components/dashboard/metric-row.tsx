"use client";

import * as React from "react";

interface MetricSlot {
  value: React.ReactNode;
  valueSuffix?: React.ReactNode;
  label: string;
  labelExtra?: React.ReactNode;
}

interface MetricRowProps {
  slots: [MetricSlot, MetricSlot, MetricSlot];
}

function MetricRow({ slots }: MetricRowProps) {
  return (
    <div className="grid grid-cols-3 gap-3 sm:gap-6 items-start">
      {slots.map((slot, i) => (
        <div
          key={i}
          className={`min-w-0${i > 0 ? " border-l border-border pl-3 sm:pl-6" : ""}`}
        >
          <div className="flex items-baseline">
            <span className="text-2xl sm:text-4xl font-semibold tracking-tight tabular-nums">
              {slot.value}
            </span>
            {slot.valueSuffix && (
              <span className="text-sm sm:text-lg font-semibold tracking-tight text-muted-foreground ml-0.5 tabular-nums">
                {slot.valueSuffix}
              </span>
            )}
          </div>
          <div className="mt-1 inline-flex items-center gap-1">
            <span className="text-sm text-muted-foreground">
              {slot.label}
            </span>
            {slot.labelExtra}
          </div>
        </div>
      ))}
    </div>
  );
}

export { MetricRow };
export type { MetricRowProps, MetricSlot };
