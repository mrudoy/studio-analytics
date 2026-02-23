export interface NewCustomer {
  name: string;
  email: string;
  role: string;
  orders: number;
  created: string; // date string
}

export interface Order {
  created: string;
  code: string;
  customer: string;
  type: string;
  payment: string;
  total: number;
}

export interface FirstVisit {
  attendee: string;
  performance: string;
  type: string;
  redeemedAt: string;
  pass: string;
  status: string;
}

export interface Registration {
  customer: string;
  pass: string;
  remaining: number;
  total: number;
  expires: string;
  price: number;
  lastTeacher: string;
  revenueCategory: string;
}

export interface RevenueCategory {
  revenueCategory: string;
  revenue: number;
  unionFees: number;
  stripeFees: number;
  otherFees: number;       // "other_fees" in CSV (was "transfers" in HTML scrape)
  transfers: number;        // kept for backward compat — 0 if CSV has other_fees instead
  refunded: number;
  unionFeesRefunded: number;
  netRevenue: number;
}

export interface AutoRenew {
  name: string; // plan name e.g. "SKY UNLIMITED" (cleaned of "\nSubscription" suffix)
  state: string; // "Valid Now", "Canceled", "In Trial", etc.
  price: number;
  customer: string;
  email?: string;      // "Customer Email" — available in direct CSV downloads
  canceledAt?: string; // "Canceled At" column on canceled report
  created?: string;    // "Created" column on active/new reports
}

export type AutoRenewCategory = "MEMBER" | "SKY3" | "SKY_TING_TV" | "UNKNOWN";

export interface FullRegistration {
  eventName: string;
  eventId?: string;
  performanceId?: string;
  performanceStartsAt?: string;
  locationName: string;
  videoName?: string;
  videoId?: string;
  teacherName: string;
  firstName: string;
  lastName: string;
  email: string;
  phoneNumber?: string;
  role?: string;
  registeredAt?: string;
  canceledAt?: string;
  attendedAt: string;
  registrationType: string;
  state: string;
  pass: string;
  subscription: boolean;  // transformed from "true"/"false" string
  revenueState?: string;
  revenue: number;
}

export interface DownloadedFiles {
  newCustomers: string;
  orders: string;
  firstVisits: string;
  allRegistrations?: string;    // Optional — only used for raw "Class Roster" export
  canceledAutoRenews: string;
  activeAutoRenews: string;
  pausedAutoRenews: string;
  trialingAutoRenews: string;
  newAutoRenews: string;
  revenueCategories?: string;   // Optional — scraped in Phase 3, non-fatal if missing
  fullRegistrations: string;    // Required — /registrations/all with 22 columns
}
