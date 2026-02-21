"use client";

import * as React from "react";

interface CardDisclosureProps {
  open: boolean;
  children: React.ReactNode;
}

function CardDisclosure({ open, children }: CardDisclosureProps) {
  return (
    <div
      className="overflow-hidden transition-all duration-300 ease-in-out"
      style={{
        maxHeight: open ? "2000px" : "0px",
        opacity: open ? 1 : 0,
      }}
    >
      <div className="mt-4 pt-4 border-t border-border space-y-4">
        {children}
      </div>
    </div>
  );
}

export { CardDisclosure };
