export interface PipelineProgress {
  step: string;
  percent: number;
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
}

export interface PipelineJobData {
  triggeredBy: string;
  dateRangeStart?: string;
  dateRangeEnd?: string;
}
