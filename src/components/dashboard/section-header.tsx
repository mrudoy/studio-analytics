"use client";

import * as React from "react";

interface SectionHeaderProps {
  children: React.ReactNode;
  subtitle?: string;
}

function SectionHeader({ children, subtitle }: SectionHeaderProps) {
  return (
    <div>
      <h2 className="text-base font-semibold tracking-tight">
        {children}
      </h2>
      {subtitle && (
        <p className="text-sm mt-0.5 text-muted-foreground">
          {subtitle}
        </p>
      )}
    </div>
  );
}

export { SectionHeader };
