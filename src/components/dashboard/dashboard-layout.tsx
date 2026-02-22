"use client";

import { useState, Fragment } from "react";
import { SidebarProvider, SidebarInset, SidebarTrigger } from "@/components/ui/sidebar";
import { Separator } from "@/components/ui/separator";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { AppSidebar } from "./app-sidebar";
import { BREADCRUMB_MAP, type SectionKey } from "./sidebar-nav";

interface DashboardLayoutProps {
  children: (activeSection: SectionKey, setActiveSection: (key: SectionKey) => void) => React.ReactNode;
}

export function DashboardLayout({ children }: DashboardLayoutProps) {
  const [activeSection, setActiveSection] = useState<SectionKey>("overview");
  const crumbs = BREADCRUMB_MAP[activeSection];

  return (
    <SidebarProvider
      style={
        {
          "--sidebar-width": "calc(var(--spacing) * 72)",
        } as React.CSSProperties
      }
    >
      <AppSidebar
        activeSection={activeSection}
        onSectionChange={setActiveSection}
        variant="inset"
      />
      <SidebarInset>
        <header className="flex h-12 shrink-0 items-center gap-2 transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-12">
          <div className="flex items-center gap-2 px-4">
            <SidebarTrigger className="-ml-1" />
            <Separator
              orientation="vertical"
              className="mr-2 data-[orientation=vertical]:h-4"
            />
            <Breadcrumb>
              <BreadcrumbList>
                {crumbs.map((crumb, i) => (
                  <Fragment key={i}>
                    {i > 0 && <BreadcrumbSeparator className="hidden md:block" />}
                    <BreadcrumbItem className={i < crumbs.length - 1 ? "hidden md:block" : ""}>
                      {i === crumbs.length - 1 ? (
                        <BreadcrumbPage>{crumb}</BreadcrumbPage>
                      ) : (
                        <BreadcrumbLink>{crumb}</BreadcrumbLink>
                      )}
                    </BreadcrumbItem>
                  </Fragment>
                ))}
              </BreadcrumbList>
            </Breadcrumb>
          </div>
        </header>
        <div className="flex flex-1 flex-col gap-4 p-4 pt-0 md:p-6 md:pt-0">
          {children(activeSection, setActiveSection)}
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
