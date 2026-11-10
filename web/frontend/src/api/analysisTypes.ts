/** 上传分析 API 契约（与 web/backend/app/analysis/router.py 对齐）。 */

export type AnalysisField =
  | 'user_id'
  | 'item_id'
  | 'event_type'
  | 'event_time'
  | 'transaction_id'
  | 'category_id'
  | 'amount'
  | 'product_name'

export type AnalysisStatus =
  | 'received'
  | 'detecting'
  | 'awaiting_mapping'
  | 'queued'
  | 'parsing'
  | 'normalizing'
  | 'aggregating'
  | 'done'
  | 'failed'
  | 'cancelled'

export interface DetectedField {
  column: string | null
  confidence: number
  reason: string
}

export interface AnalysisColumn {
  name: string
  dtype: string
  sample: string[]
}

export interface AnalysisPreview {
  kind: 'csv' | 'xlsx'
  container: 'file' | 'zip'
  encoding: string
  delimiter: string
  has_header: boolean
  sheet: string | null
  members: string[]
  selected_member: string | null
  extracted_name: string | null
  rows_estimated: number
  columns: AnalysisColumn[]
  sample_rows: string[][]
  detected: Record<AnalysisField, DetectedField>
  event_values: string[]
}

export interface AnalysisProgress {
  stage: string
  percent: number
  rows_processed: number
  rows_total: number | null
  eta_seconds: number | null
  elapsed_seconds: number
  message: string
}

export interface AnalysisJob {
  job_id: string
  status: AnalysisStatus
  created_at: string
  updated_at: string
  filename: string
  size_bytes: number
  file_type: string
  sample: boolean
  progress: AnalysisProgress
  error: string | null
  started_at: string | null
  finished_at: string | null
  elapsed_seconds: number | null
  preview: AnalysisPreview | null
  mapping: Record<string, string | boolean | null> | null
}

export interface AnalysisListResponse {
  jobs: AnalysisJob[]
}

export interface StartAnalysisPayload {
  mapping: Record<string, string | null>
  timezone: string
  has_header: boolean
}

export interface AnalysisTrafficPoint {
  date: string
  event_date?: string
  pv: number
  uv: number
  dau: number
  cart_events: number
  purchase_events: number
  orders: number
  revenue: number
  view_users?: number
  cart_users?: number
  buyer_users?: number
  strict_conv_users?: number
  add_to_cart_rate?: number | null
  conv_strict_rate?: number | null
  conv_global_rate?: number | null
}

export interface AnalysisFunnelStage {
  stage: string
  label: string
  users: number
  rate?: number | null
}

export interface AnalysisD1Point {
  date: string
  cohort_size: number
  retained_users: number
  d1_rate: number | null
  window_status: 'full' | 'partial' | string
}

export interface AnalysisItemRow {
  item_id: string
  product_name: string | null
  views: number
  carts: number
  purchases: number
  buyers: number
  revenue: number
}

export interface AnalysisCategoryRow {
  category_id: string
  views: number
  carts: number
  purchases: number
  buyers: number
  revenue: number
}

export interface AnalysisQuality {
  rows_read: number
  rows_parsed: number
  rows_kept: number
  rows_rejected: number
  rejected_reasons: Record<string, number>
  duplicates: number
  unknown_events: number
  other_events_kept: number
  event_types: Record<string, number>
  date_min: string | null
  date_max: string | null
  days: number
  distinct_users: number
  distinct_items: number
  rows_with_transaction_id: number
  rows_with_category_id: number
  rows_with_amount: number
  rows_with_product_name: number
  has_transaction_id: boolean
  has_category_id: boolean
  has_amount: boolean
  has_product_name: boolean
  orders_basis: string
}

export interface AnalysisProvenance {
  filename: string
  file_type: string
  size_bytes: number
  encoding: string
  timezone: string
  rows_estimated: number | null
  rows_kept: number
  parse_seconds: number | null
  elapsed_seconds: number
  sample: boolean
}

export interface AnalysisTotals {
  events: number
  pv: number
  uv: number
  dau_avg: number
  view_users: number
  cart_users: number
  buyer_users: number
  orders: number
  revenue: number | null
  add_to_cart_rate: number | null
  conv_strict_rate: number | null
  conv_global_rate: number | null
  d1_retention: number | null
}

export interface AnalysisResult {
  job_id: string
  filename: string
  file_type: string
  generated_at: string
  elapsed_seconds: number
  range: { date_min: string | null; date_max: string | null; days: number }
  totals: AnalysisTotals
  traffic: AnalysisTrafficPoint[]
  funnel: AnalysisFunnelStage[]
  d1_series: AnalysisD1Point[]
  top_items: AnalysisItemRow[]
  categories: AnalysisCategoryRow[]
  quality: AnalysisQuality
  provenance: AnalysisProvenance
  mapping: Record<string, string | boolean | null>
  has_product_name: boolean
  has_category_id: boolean
  has_amount: boolean
}
