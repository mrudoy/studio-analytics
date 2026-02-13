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

export interface AutoRenew {
  name: string; // plan name e.g. "SKY UNLIMITED" (cleaned of "\nSubscription" suffix)
  state: string; // "Valid Now", "Canceled", "In Trial", etc.
  price: number;
  customer: string;
  canceledAt?: string; // "Canceled At" column on canceled report
  created?: string;    // "Created" column on active/new reports
}

export type SubscriptionCategory = "MEMBER" | "SKY3" | "SKY_TING_TV" | "UNKNOWN";

export interface DownloadedFiles {
  newCustomers: string;
  orders: string;
  firstVisits: string;
  allRegistrations: string;
  canceledAutoRenews: string;
  activeAutoRenews: string;
  newAutoRenews: string;
}
