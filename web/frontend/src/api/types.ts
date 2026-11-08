/** API 响应类型：严格对应 SPEC §3 契约（v1 冻结）。 */

export interface HealthResponse {
  status: string
  ts: string
}

export interface LatestPublish {
  publish_id: number
  scope_type: string
  dates_csv: string | null
  quality_status: string
  published_at: string
}

export interface LatestQuality {
  run_id: number
  status: string
  rules_total: number
  rules_passed: number
  rules_failed: number
  checks_total: number
}

export interface RealtimeMeta {
  latest_updated_at: string | null
  final_days: number
  rows: number
  reconciliation: {
    MATCH?: number
    DIFF?: number
    SKIP?: number
    [key: string]: number | undefined
  }
}

export interface MetaResponse {
  date_min: string
  date_max: string
  full_days: number
  partial_days: string[]
  latest_publish: LatestPublish | null
  latest_quality: LatestQuality | null
  realtime: RealtimeMeta | null
}

export interface RangeInfo {
  from: string
  to: string
  days: number
}

export interface OverviewKpis {
  pv: number
  uv_daily_avg: number
  dau_daily_avg: number
  orders: number
  view_users: number
  cart_users: number
  buyer_users: number
  add_to_cart_rate: number | null
  conv_strict_rate: number | null
  conv_global_rate: number | null
  d1_retention: number | null
}

export interface TrafficPoint {
  date: string
  pv: number
  uv: number
  dau: number
}

export interface RatesPoint {
  date: string
  add_to_cart_rate: number | null
  conv_strict_rate: number | null
  conv_global_rate: number | null
}

export interface FunnelStage {
  stage: 'view' | 'cart' | 'buy' | string
  users: number
}

export interface TopItem {
  item_id: number
  vcnt: number
  acnt: number
  tcnt: number
}

export interface D1Point {
  date: string
  d1_rate: number | null
  /** 行级窗口状态；缺失时回退到 cohort 级 window_status */
  window_status?: string
}

export interface OverviewResponse {
  range: RangeInfo
  kpis: OverviewKpis
  traffic: TrafficPoint[]
  rates: RatesPoint[]
  funnel: FunnelStage[]
  top_items: TopItem[]
  d1_series: D1Point[]
}

export interface ItemRow {
  item_id: number
  view_cnt: number
  add_to_cart_cnt: number
  transaction_cnt: number
  view_uv: number
  transaction_uv: number
}

export interface CategoryRow {
  category_id: number
  view_cnt: number
  order_cnt: number
  view_uv: number
  transaction_uv: number
}

export interface ItemBehaviorPoint {
  date: string
  items_viewed: number
  items_carted: number
  items_bought: number
}

export interface ItemsResponse {
  items: ItemRow[]
  categories: CategoryRow[]
  behavior: ItemBehaviorPoint[]
}

export interface RetentionValue {
  age_day: number
  rate: number | null
  /** 行级窗口状态；缺失时回退到 cohort 级 window_status */
  window_status?: string
}

export interface RetentionCohort {
  cohort_date: string
  cohort_size: number
  values: RetentionValue[]
  /** 后端透传：full / partial；缺省按 full 处理 */
  window_status?: string
}

export interface RetentionResponse {
  cohorts: RetentionCohort[]
  d1_series: D1Point[]
  weighted_d1: number | null
}

export interface RealtimeTable {
  table: string
  rows: number
  final_rows: number | null
  latest_date: string | null
  updated_at: string | null
}

export interface ReconciliationSummary {
  MATCH?: number
  DIFF?: number
  SKIP?: number
  checked_at?: string | number | null
  [key: string]: number | string | null | undefined
}

export interface ReconcileRow {
  date: string
  metric: string
  offline: number | null
  realtime: number | null
  status: string
}

export interface RealtimeResponse {
  tables: RealtimeTable[]
  reconciliation: ReconciliationSummary
  compare: ReconcileRow[]
  late_events: number
}

export interface QualityRun {
  run_id: number
  scope_type: string
  dates_csv: string | null
  status: string
  rules_total: number
  rules_passed: number
  rules_failed: number
  checks_total: number
  started_at: string
  finished_at: string | null
}

export interface QualityRule {
  rule_key: string
  category: string
  status: string
  checked: number
  failed_rows: number
  detail: string | null
  duration_ms: number
}

export interface QualityRunDetail extends QualityRun {
  rules: QualityRule[]
}

export interface QualityRunsResponse {
  runs: QualityRun[]
}

export interface PipelineRun {
  run_id: number
  dag_run_id: string
  mode: string
  window_start: string
  window_end: string
  status: string
  published: string | null
  started_at: string
  finished_at: string | null
}

export interface PublishLogRow {
  publish_id: number
  scope_type: string
  dates_csv: string | null
  quality_status: string
  published_at: string
}

export interface WatermarkRow {
  pipeline: string
  event_date_utc: string
  max_ingest_batch: number
  processed_at: string
}

export interface TableStatRow {
  table: string
  rows: number
}

export interface OpsResponse {
  pipeline_runs: PipelineRun[]
  publish_log: PublishLogRow[]
  watermark: WatermarkRow[]
  table_stats: TableStatRow[]
}
