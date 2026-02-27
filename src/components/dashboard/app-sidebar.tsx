"use client";

import { useEffect, useState } from "react";
import { ChevronRight, Settings, FileText } from "lucide-react";
import { BubbleIcon } from "./icons";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  SidebarSeparator,
} from "@/components/ui/sidebar";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { SkyTingSwirl, SkyTingWordmark } from "./sky-ting-logo";
import { NAV_ITEMS, type SectionKey } from "./sidebar-nav";

interface DataFreshness {
  overall?: string;
  unionAutoRenews?: string;
  shopifySync?: string;
  isPartial?: boolean;
}

function formatRelativeTime(iso: string): string {
  const date = new Date(iso);
  if (isNaN(date.getTime())) return iso;
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins}m ago`;
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

function DataFreshnessLine() {
  const [freshness, setFreshness] = useState<DataFreshness | null>(null);

  useEffect(() => {
    let mounted = true;
    async function fetchFreshness() {
      try {
        const res = await fetch("/api/stats");
        if (!res.ok) return;
        const data = await res.json();
        if (mounted && data.dataFreshness) {
          setFreshness(data.dataFreshness);
        }
      } catch {
        // Silently ignore — sidebar still works without freshness
      }
    }
    fetchFreshness();
    // Refresh every 60s so the "Xm ago" stays current
    const interval = setInterval(fetchFreshness, 60_000);
    return () => { mounted = false; clearInterval(interval); };
  }, []);

  if (!freshness?.overall) return null;

  return (
    <div className="px-4 pb-1 group-data-[collapsible=icon]:hidden">
      <p className="text-[10px] leading-relaxed text-muted-foreground/70">
        Updated {formatRelativeTime(freshness.overall)}
        {freshness.isPartial ? " (partial)" : ""}
        {" · "}
        Union {freshness.unionAutoRenews ? formatRelativeTime(freshness.unionAutoRenews) : "—"}
        {" · "}
        Shopify {freshness.shopifySync ? formatRelativeTime(freshness.shopifySync) : "—"}
      </p>
    </div>
  );
}

interface AppSidebarProps {
  activeSection: SectionKey;
  onSectionChange: (key: SectionKey) => void;
  variant?: "sidebar" | "inset";
}

export function AppSidebar({ activeSection, onSectionChange, variant = "inset" }: AppSidebarProps) {
  return (
    <Sidebar collapsible="icon" variant={variant}>
      <SidebarHeader className="px-4 py-5">
        <div className="flex items-center gap-2.5">
          <SkyTingSwirl size={26} />
          <SkyTingWordmark className="group-data-[collapsible=icon]:hidden" />
        </div>
        <DataFreshnessLine />
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map((item) => {
                if (!item.children) {
                  const isActive = activeSection === item.key;
                  return (
                    <SidebarMenuItem key={item.key}>
                      <SidebarMenuButton
                        isActive={isActive}
                        onClick={() => onSectionChange(item.key)}
                        tooltip={item.label}
                      >
                        {item.icon && (
                          <item.icon
                            className="size-5"
                            style={{ color: item.color }}
                          />
                        )}
                        <span className="text-[0.94rem]">{item.label}</span>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                }

                const groupActive = item.children.some(
                  (child) => activeSection === child.key
                );

                return (
                  <Collapsible key={item.key} defaultOpen={groupActive} asChild className="group/collapsible">
                    <SidebarMenuItem>
                      <CollapsibleTrigger asChild>
                        <SidebarMenuButton tooltip={item.label}>
                          {item.icon && (
                            <item.icon
                              className="size-5"
                              style={{ color: item.color }}
                            />
                          )}
                          <span className="flex-1 text-left text-[0.94rem]">{item.label}</span>
                          <ChevronRight className="size-4 transition-transform group-data-[state=open]/collapsible:rotate-90" />
                        </SidebarMenuButton>
                      </CollapsibleTrigger>
                      <CollapsibleContent>
                        <SidebarMenuSub>
                          {item.children.map((child) => (
                            <SidebarMenuSubItem key={child.key}>
                              <SidebarMenuSubButton
                                isActive={activeSection === child.key}
                                onClick={() => onSectionChange(child.key)}
                                className="cursor-pointer"
                              >
                                {child.icon && (
                                  <child.icon
                                    className="size-[1.1rem]"
                                    style={{ color: item.color }}
                                  />
                                )}
                                <span>{child.label}</span>
                              </SidebarMenuSubButton>
                            </SidebarMenuSubItem>
                          ))}
                        </SidebarMenuSub>
                      </CollapsibleContent>
                    </SidebarMenuItem>
                  </Collapsible>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter>
        <SidebarSeparator />
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild tooltip="Ask AI">
              <a href="/ask" className="flex items-center gap-2">
                <BubbleIcon size={16} />
                <span>Ask AI</span>
              </a>
            </SidebarMenuButton>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <SidebarMenuButton asChild tooltip="Settings">
              <a href="/settings">
                <Settings className="size-4" />
                <span>Settings</span>
              </a>
            </SidebarMenuButton>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <SidebarMenuButton asChild tooltip="Results">
              <a href="/results">
                <FileText className="size-4" />
                <span>Results</span>
              </a>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}
