"use client";

import { ChevronRight, Settings, FileText } from "lucide-react";
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
  SidebarRail,
  SidebarSeparator,
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
      <SidebarHeader className="px-4 py-5">
        <div className="flex items-center gap-2.5">
          <SkyTingSwirl size={26} />
          <SkyTingWordmark className="group-data-[collapsible=icon]:hidden" />
        </div>
      </SidebarHeader>

      <SidebarContent>
        {NAV_ITEMS.map((item) => {
          if (!item.children) {
            const isActive = activeSection === item.key;
            return (
              <SidebarGroup key={item.key}>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isActive}
                      onClick={() => onSectionChange(item.key)}
                      tooltip={item.label}
                    >
                      {item.icon && (
                        <item.icon
                          className="size-4"
                          style={{ color: item.color }}
                        />
                      )}
                      <span>{item.label}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>
              </SidebarGroup>
            );
          }

          const groupActive = item.children.some(
            (child) => activeSection === child.key
          );

          return (
            <Collapsible key={item.key} defaultOpen={groupActive} className="group/collapsible">
              <SidebarGroup>
                <SidebarGroupLabel asChild>
                  <CollapsibleTrigger className="flex w-full items-center gap-2">
                    {item.icon && (
                      <item.icon
                        className="size-4"
                        style={{ color: item.color }}
                      />
                    )}
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

      <SidebarFooter>
        <SidebarSeparator />
        <SidebarMenu>
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

      <SidebarRail />
    </Sidebar>
  );
}
