"use client";

import * as React from "react";
import { cn } from "@/lib/utils";
import {
  CardHeader,
  CardTitle,
  CardDescription,
  CardAction,
} from "@/components/ui/card";

interface ModuleHeaderProps {
  color: string;
  title: string;
  summaryPill?: string;
  description?: string;
  detailsOpen?: boolean;
  onToggleDetails?: () => void;
  children?: React.ReactNode;
}

function ModuleHeader({
  color,
  title,
  summaryPill,
  description,
  detailsOpen,
  onToggleDetails,
  children,
}: ModuleHeaderProps) {
  return (
    <CardHeader>
      <CardTitle className="flex items-center gap-2.5">
        <span
          className="w-2.5 h-2.5 rounded-full shrink-0"
          style={{ backgroundColor: color }}
        />
        <span>{title}</span>
        {summaryPill && (
          <span className="text-xs font-medium text-muted-foreground bg-muted rounded-full px-2 py-0.5">
            {summaryPill}
          </span>
        )}
      </CardTitle>
      {description && (
        <CardDescription>{description}</CardDescription>
      )}
      {(children || onToggleDetails) && (
        <CardAction>
          <div className="flex items-center gap-3">
            {children}
            {onToggleDetails && (
              <button
                type="button"
                onClick={onToggleDetails}
                className="text-xs text-muted-foreground hover:text-foreground font-medium inline-flex items-center gap-1 transition-colors"
              >
                {detailsOpen ? "Hide" : "Details"}
                <span
                  className="text-[8px] transition-transform duration-150"
                  style={{
                    transform: detailsOpen ? "rotate(90deg)" : "rotate(0)",
                  }}
                >
                  &#9654;
                </span>
              </button>
            )}
          </div>
        </CardAction>
      )}
    </CardHeader>
  );
}

export { ModuleHeader };
export type { ModuleHeaderProps };
