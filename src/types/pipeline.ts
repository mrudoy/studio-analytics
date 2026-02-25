export type CategoryState = "pending" | "downloading" | "parsing" | "saved" | "failed" | "skipped";

export interface CategoryProgress {
  state: CategoryState;
  recordCount?: number;
  error?: string;
  deliveryMethod?: "direct" | "email";
}

export interface PipelineProgress {
  step: string;
  percent: number;
  startedAt?: number;
  categories?: Record<string, CategoryProgress>;
}

export interface ValidationCheck {
  name: string;
  count: number;
  status: "ok" | "warn" | "fail";
}

export interface ValidationResult {
  passed: boolean;
  checks: ValidationCheck[];
}

export interface PipelineResult {
  success: boolean;
  sheetUrl: string;
  rawDataSheetUrl: string;
  duration: number;
  recordCounts: {
    newCustomers: number;
    orders: number;
    firstVisits: number;
    registrations: number;
    canceledAutoRenews: number;
    activeAutoRenews: number;
    newAutoRenews: number;
  };
  warnings: string[];
  validation?: ValidationResult;
}

export interface PipelineJobData {
  triggeredBy: string;
  dateRangeStart?: string;
  dateRangeEnd?: string;
}
