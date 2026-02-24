"use client"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

export type PromptSuggestionProps = {
  children: React.ReactNode
  className?: string
} & React.ButtonHTMLAttributes<HTMLButtonElement>

function PromptSuggestion({
  children,
  className,
  ...props
}: PromptSuggestionProps) {
  return (
    <Button
      variant="outline"
      size="lg"
      className={cn("rounded-full", className)}
      {...props}
    >
      {children}
    </Button>
  )
}

export { PromptSuggestion }
