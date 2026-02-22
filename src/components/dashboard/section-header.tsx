"use client";

import * as React from "react";

interface SectionHeaderProps {
  children: React.ReactNode;
  subtitle?: string;
}

function SectionHeader({ children, subtitle }: SectionHeaderProps) {
  return (
    <div className="space-y-1">
      <h2 className="text-2xl font-semibold tracking-tight">
        {children}
      </h2>
      {subtitle && (
        <p className="text-sm text-muted-foreground">
          {subtitle}
        </p>
      )}
    </div>
  );
}

export { SectionHeader };
