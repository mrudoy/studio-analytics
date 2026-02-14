/**
 * Centralized CSS selectors for Union.fit admin dashboard.
 *
 * When Union.fit updates their UI, update selectors HERE ONLY.
 * Each selector has multiple fallback strategies.
 */

export const SELECTORS = {
  login: {
    emailInput: 'input[type="email"], input[name="email"], input[placeholder*="email" i]',
    passwordInput: 'input[type="password"], input[name="password"]',
    submitButton: 'button[type="submit"], input[type="submit"], button:has-text("Sign In")',
    dashboardIndicator: 'text=Dashboard, text=UP NEXT, [href*="/dashboard"]',
  },

  navigation: {
    reportsMenu: 'nav >> text=Reports, a:has-text("Reports"), text=Reports',
    peopleMenu: 'nav >> text=People, a:has-text("People")',
  },

  reports: {
    // The View button with dropdown chevron - appears on all report pages
    viewDropdown: 'button:has-text("View") + button, button:near(button:has-text("View"))',
    viewButtonGroup: 'text=View',
    downloadCsv: 'text=Download CSV, a:has-text("Download CSV"), button:has-text("Download CSV")',

    // Date range input
    dateRangeInput: 'input[type="text"][value*="/"]',

    // Period selector
    periodSelector: 'select, button:has-text("Week"), button:has-text("Month")',
  },

  pagination: {
    nextButton: 'text=Next, a:has-text("Next")',
    lastButton: 'text=Last, a:has-text("Last")',
  },
} as const;

/**
 * Report URL paths relative to /admin/orgs/{org-slug}
 */
export const REPORT_URLS = {
  newCustomers: "/report/customers/created_within",
  orders: "/reports/transactions?transaction_type=orders",
  firstVisits: "/report/registrations/first_visit",
  allRegistrations: "/report/registrations/remaining",
  canceledAutoRenews: "/report/subscriptions/growth?filter=cancelled",
  activeAutoRenews: "/report/subscriptions/list?status=active",
  pausedAutoRenews: "/report/subscriptions/list?status=paused",
  newAutoRenews: "/report/subscriptions/growth?filter=new",
} as const;

export type ReportType = keyof typeof REPORT_URLS;
