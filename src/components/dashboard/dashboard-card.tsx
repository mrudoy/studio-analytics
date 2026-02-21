"use client";

import * as React from "react";
import { cn } from "@/lib/utils";
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardAction,
  CardContent,
  CardFooter,
} from "@/components/ui/card";

interface DashboardCardProps extends React.ComponentProps<typeof Card> {
  matchHeight?: boolean;
}

function DashboardCard({
  className,
  matchHeight = false,
  ...props
}: DashboardCardProps) {
  return (
    <Card
      className={cn(
        "overflow-hidden min-w-0",
        matchHeight && "h-full",
        className
      )}
      {...props}
    />
  );
}

export {
  DashboardCard,
  CardHeader,
  CardTitle,
  CardDescription,
  CardAction,
  CardContent,
  CardFooter,
};
