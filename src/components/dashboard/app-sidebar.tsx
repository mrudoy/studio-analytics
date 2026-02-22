"use client";

import { ChevronRight } from "lucide-react";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  SidebarRail,
} from "@/components/ui/sidebar";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { SkyTingSwirl, SkyTingWordmark } from "./sky-ting-logo";
import { NAV_ITEMS, type SectionKey } from "./sidebar-nav";

interface AppSidebarProps {
  activeSection: SectionKey;
  onSectionChange: (key: SectionKey) => void;
}

export function AppSidebar({ activeSection, onSectionChange }: AppSidebarProps) {
  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="p-4">
        <div className="flex items-center gap-2">
          <SkyTingSwirl size={24} />
          <SkyTingWordmark className="group-data-[collapsible=icon]:hidden" />
        </div>
      </SidebarHeader>

      <SidebarContent>
        {NAV_ITEMS.map((item) => {
          if (!item.children) {
            // Simple top-level item (Overview, Revenue, Churn)
            return (
              <SidebarGroup key={item.key}>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={activeSection === item.key}
                      onClick={() => onSectionChange(item.key)}
                      tooltip={item.label}
                    >
                      {item.icon && <item.icon />}
                      <span>{item.label}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>
              </SidebarGroup>
            );
          }

          // Collapsible group (Growth, Conversion)
          const groupActive = item.children.some(
            (child) => activeSection === child.key
          );

          return (
            <Collapsible key={item.key} defaultOpen={groupActive} className="group/collapsible">
              <SidebarGroup>
                <SidebarGroupLabel asChild>
                  <CollapsibleTrigger className="flex w-full items-center gap-2">
                    {item.icon && <item.icon className="size-4" />}
                    <span className="flex-1 text-left">{item.label}</span>
                    <ChevronRight className="size-4 transition-transform group-data-[state=open]/collapsible:rotate-90" />
                  </CollapsibleTrigger>
                </SidebarGroupLabel>
                <CollapsibleContent>
                  <SidebarGroupContent>
                    <SidebarMenu>
                      {item.children.map((child) => (
                        <SidebarMenuItem key={child.key}>
                          <SidebarMenuButton
                            isActive={activeSection === child.key}
                            onClick={() => onSectionChange(child.key)}
                          >
                            <span>{child.label}</span>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                      ))}
                    </SidebarMenu>
                  </SidebarGroupContent>
                </CollapsibleContent>
              </SidebarGroup>
            </Collapsible>
          );
        })}
      </SidebarContent>

      <SidebarFooter className="p-4">
        <div className="flex flex-col gap-2 text-sm">
          <a
            href="/settings"
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            Settings
          </a>
          <a
            href="/results"
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            Results
          </a>
        </div>
      </SidebarFooter>

      <SidebarRail />
    </Sidebar>
  );
}
