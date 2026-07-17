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
  /** Direct download URL for the zip export (webhook path — skips Gmail). */
  downloadUrl?: string;
  /**
   * When the source export was generated (webhook `generated_at`). Feeds the
   * anti-resurrection guard: a Canceled row is only re-activated by a source
   * proven newer than the cancellation. Absent → the guard treats this import
   * as unknown-provenance and never resurrects (safe default).
   */
  sourceEffectiveAt?: string;
}
