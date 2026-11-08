/**
 * 本地 mock 数据（仅当 VITE_USE_MOCK=true 时启用）。
 * 数值形态与 SPEC §3 契约一致，用于后端未就绪时的视觉验收；
 * 默认构建与 npm run dev 均不会加载此模块路径的 mock 开关。
 */
import type {
  D1Point,
  ItemBehaviorPoint,
  ItemRow,
  CategoryRow,
  ItemsResponse,
  MetaResponse,
  OpsResponse,
  OverviewResponse,
  QualityRule,
  QualityRun,
  QualityRunDetail,
  QualityRunsResponse,
  RealtimeResponse,
  RetentionCohort,
  RetentionResponse,
  ReconcileRow,
} from './types'

const DATA_MIN = '2015-05-03'
const DATA_MAX = '2015-09-18'
const FULL_MIN = '2015-05-04'
const FULL_COHORT_MAX = '2015-09-16'

const METRICS = [
  'pv',
  'uv',
  'dau',
  'view_users',
  'cart_users',
  'buyer_users',
  'orders',
  'add_to_cart_rate',
  'conv_strict_rate',
  'conv_global_rate',
  'items_viewed',
  'items_carted',
  'items_bought',
]

function round(value: number): number {
  return Math.round(value)
}

function round6(value: number): number {
  return Math.round(value * 1e6) / 1e6
}

/** 稳定伪随机：同一日期/键在多次请求中保持一致。 */
function rnd(key: string): number {
  let h = 2166136261
  for (let i = 0; i < key.length; i += 1) {
    h ^= key.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  h ^= h >>> 13
  h = Math.imul(h, 0x5bd1e995)
  h ^= h >>> 15
  return (h >>> 0) / 4294967296
}

function addDays(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`)
  d.setUTCDate(d.getUTCDate() + days)
  return d.toISOString().slice(0, 10)
}

function daysBetween(from: string, to: string): string[] {
  const values: string[] = []
  let cursor = from
  while (cursor <= to && values.length < 1000) {
    values.push(cursor)
    cursor = addDays(cursor, 1)
  }
  return values
}

interface DayStat {
  date: string
  pv: number
  uv: number
  dau: number
  orders: number
  cart_users: number
  buyer_users: number
  add_to_cart_rate: number
  conv_strict_rate: number
  conv_global_rate: number
  items_viewed: number
  items_carted: number
  items_bought: number
}

function makeDayStat(date: string): DayStat {
  let pv: number
  let uv: number
  let dau: number

  if (date === DATA_MIN) {
    pv = 13303
    uv = 7865
    dau = 7865
  } else if (date === DATA_MAX) {
    pv = 1479
    uv = 1016
    dau = 1016
  } else if (date === '2015-07-26') {
    pv = 32077
    uv = 16980
    dau = 17516
  } else {
    const dow = new Date(`${date}T00:00:00Z`).getUTCDay()
    const weekend = dow === 0 || dow === 6 ? 1.06 : 1
    pv = round(19300 * weekend * (0.88 + rnd(`pv:${date}`) * 0.24))
    uv = round(pv * (0.6 + rnd(`uv:${date}`) * 0.04))
    dau = round(uv * (1.002 + rnd(`dau:${date}`) * 0.008))
  }

  const cartRate = 0.024 + rnd(`cart:${date}`) * 0.005
  const buyerRate = 0.0068 + rnd(`buyer:${date}`) * 0.0026
  const cartUsers = round(uv * cartRate)
  const buyerUsers = round(uv * buyerRate)
  const strictUsers = round(buyerUsers * (0.95 + rnd(`strict:${date}`) * 0.02))
  const orders = round(buyerUsers * (1.45 + rnd(`orders:${date}`) * 0.12))

  return {
    date,
    pv,
    uv,
    dau,
    orders,
    cart_users: cartUsers,
    buyer_users: buyerUsers,
    add_to_cart_rate: uv > 0 ? round6(cartUsers / uv) : 0,
    conv_strict_rate: uv > 0 ? round6(strictUsers / uv) : 0,
    conv_global_rate: uv > 0 ? round6(buyerUsers / uv) : 0,
    items_viewed: round(32000 + rnd(`iv:${date}`) * 21000),
    items_carted: round(1200 + rnd(`ic:${date}`) * 900),
    items_bought: round(210 + rnd(`ib:${date}`) * 160),
  }
}

const ALL_DATES = daysBetween(DATA_MIN, DATA_MAX)
const DAY_STATS = ALL_DATES.map(makeDayStat)

function newUsers(date: string): number {
  const stat = DAY_STATS.find((item) => item.date === date)
  if (!stat) return 9000
  return round(stat.uv * (0.84 + rnd(`new:${date}`) * 0.12))
}

function d1Rate(date: string): number {
  if (date === '2015-05-05') return 0.041076
  if (date === '2015-09-15') return 0.016686
  return 0.0225 + rnd(`d1:${date}`) * 0.01
}

function clampRange(from: string | null, to: string | null): { from: string; to: string } {
  let start = from && from >= DATA_MIN ? from : DATA_MIN
  let end = to && to <= DATA_MAX ? to : DATA_MAX
  if (start > end) {
    const swap = start
    start = end
    end = swap
  }
  return { from: start, to: end }
}

function filteredStats(from: string, to: string): DayStat[] {
  return DAY_STATS.filter((item) => item.date >= from && item.date <= to)
}

function d1Series(from: string, to: string): D1Point[] {
  return filteredStats(from, to)
    .filter((item) => item.date < DATA_MAX)
    .map((item) => ({
      date: item.date,
      d1_rate: round6(d1Rate(item.date)),
      window_status:
        item.date < FULL_MIN || addDays(item.date, 1) >= DATA_MAX ? 'partial' : 'full',
    }))
}

function getMeta(): MetaResponse {
  return {
    date_min: DATA_MIN,
    date_max: DATA_MAX,
    full_days: 137,
    partial_days: [DATA_MIN, DATA_MAX],
    latest_publish: {
      publish_id: 13,
      scope_type: 'DATES',
      dates_csv: '2015-09-10,2015-09-11,2015-09-12,2015-09-13,2015-09-14,2015-09-15,2015-09-16,2015-09-17,2015-09-18',
      quality_status: 'PASS',
      published_at: '2026-11-07T03:12:41Z',
    },
    latest_quality: {
      run_id: 8,
      status: 'PASS',
      rules_total: 60,
      rules_passed: 60,
      rules_failed: 0,
      checks_total: 76488744,
    },
    realtime: {
      latest_updated_at: '2026-11-07T03:20:00Z',
      final_days: 138,
      rows: 139,
      reconciliation: { MATCH: 1781, DIFF: 0, SKIP: 26 },
    },
  }
}

const TOP_ITEM_IDS = [
  461686, 187946, 231174, 53069, 123456, 98765, 345678, 56789, 789012, 111222,
  222333, 333444, 444555, 555666, 666777, 777888, 888999, 999000, 112233, 445566,
]

function buildTopItems(days: number): ItemRow[] {
  const factor = days / ALL_DATES.length
  return TOP_ITEM_IDS.slice(0, 20).map((itemId, index) => {
    const baseView = round(12345 / (1 + index * 0.12) + rnd(`item:${itemId}`) * 900)
    const viewCnt = Math.max(1, round(baseView * factor))
    const cartCnt = Math.max(1, round(viewCnt * (0.026 + rnd(`ac:${itemId}`) * 0.012)))
    const transactionCnt = Math.max(1, round(viewCnt * (0.006 + rnd(`tc:${itemId}`) * 0.004)))
    return {
      item_id: itemId,
      view_cnt: viewCnt,
      add_to_cart_cnt: cartCnt,
      transaction_cnt: transactionCnt,
      view_uv: Math.max(1, round(viewCnt * 0.82)),
      transaction_uv: Math.max(1, round(transactionCnt * 0.95)),
    }
  })
}

function getOverview(fromParam: string | null, toParam: string | null): OverviewResponse {
  const { from, to } = clampRange(fromParam, toParam)
  const stats = filteredStats(from, to)
  const days = stats.length
  const sum = (pick: (item: DayStat) => number) => stats.reduce((acc, item) => acc + pick(item), 0)
  const avg = (pick: (item: DayStat) => number) => (days > 0 ? sum(pick) / days : 0)

  const pv = sum((item) => item.pv)
  const uvAvg = avg((item) => item.uv)
  const dauAvg = avg((item) => item.dau)
  const dedupFactor = 1 - 0.15 * Math.min(1, (days - 1) / (ALL_DATES.length - 1))
  const viewUsers = Math.max(1, round(sum((item) => item.uv) * dedupFactor))
  const cartRatio = sum((item) => item.cart_users) / Math.max(1, sum((item) => item.uv))
  const buyerRatio = sum((item) => item.buyer_users) / Math.max(1, sum((item) => item.uv))
  const strictRatio =
    sum((item) => item.conv_strict_rate * item.uv) / Math.max(1, sum((item) => item.uv))
  const globalRatio =
    sum((item) => item.conv_global_rate * item.uv) / Math.max(1, sum((item) => item.uv))
  const cartUsers = round(viewUsers * cartRatio)
  const buyerUsers = round(viewUsers * buyerRatio)
  const strictUsers = round(viewUsers * strictRatio)
  const globalUsers = round(viewUsers * globalRatio)
  const orders = sum((item) => item.orders)

  let d1Numerator = 0
  let d1Denominator = 0
  for (const item of stats) {
    if (item.date >= DATA_MAX) continue
    const users = newUsers(item.date)
    d1Numerator += users * d1Rate(item.date)
    d1Denominator += users
  }
  const d1 = d1Denominator > 0 ? round6(d1Numerator / d1Denominator) : null

  const topItems = buildTopItems(days)
    .sort((a, b) => b.transaction_cnt - a.transaction_cnt)
    .slice(0, 10)
    .map((item) => ({
      item_id: item.item_id,
      vcnt: item.view_cnt,
      acnt: item.add_to_cart_cnt,
      tcnt: item.transaction_cnt,
    }))

  return {
    range: { from, to, days },
    kpis: {
      pv,
      uv_daily_avg: Math.round(uvAvg),
      dau_daily_avg: Math.round(dauAvg),
      orders,
      view_users: viewUsers,
      cart_users: cartUsers,
      buyer_users: buyerUsers,
      add_to_cart_rate: viewUsers > 0 ? round6(cartUsers / viewUsers) : null,
      conv_strict_rate: viewUsers > 0 ? round6(strictUsers / viewUsers) : null,
      conv_global_rate: viewUsers > 0 ? round6(globalUsers / viewUsers) : null,
      d1_retention: d1,
    },
    traffic: stats.map((item) => ({ date: item.date, pv: item.pv, uv: item.uv, dau: item.dau })),
    rates: stats.map((item) => ({
      date: item.date,
      add_to_cart_rate: item.add_to_cart_rate,
      conv_strict_rate: item.conv_strict_rate,
      conv_global_rate: item.conv_global_rate,
    })),
    funnel: [
      { stage: 'view', users: viewUsers },
      { stage: 'cart', users: cartUsers },
      { stage: 'buy', users: buyerUsers },
    ],
    top_items: topItems,
    d1_series: d1Series(from, to),
  }
}

function getItems(fromParam: string | null, toParam: string | null, limitParam: number): ItemsResponse {
  const { from, to } = clampRange(fromParam, toParam)
  const stats = filteredStats(from, to)
  const days = stats.length
  const items = buildTopItems(days)
    .sort((a, b) => b.transaction_cnt - a.transaction_cnt)
    .slice(0, Math.max(1, Math.min(limitParam, 100)))

  const categoryIds = [
    123, 456, 789, 101112, 131415, 161718, 192021, 222324, 252627, 282930,
    313233, 343536, 373839, 404142, 434445,
  ]
  const unclassifiedView = Math.max(1, round(92000 * (days / 139)))
  const categories: CategoryRow[] = [
    {
      category_id: 0,
      view_cnt: unclassifiedView,
      order_cnt: Math.max(1, round(unclassifiedView * 0.005)),
      view_uv: Math.max(1, round(unclassifiedView * 0.68)),
      transaction_uv: Math.max(1, round(unclassifiedView * 0.004)),
    },
    ...categoryIds.map((categoryId, index) => {
      const viewCnt = round(
        (9200 / (1 + index * 0.18) + rnd(`cat:${categoryId}`) * 700) * (days / 139),
      )
      return {
        category_id: categoryId,
        view_cnt: Math.max(1, viewCnt),
        order_cnt: Math.max(1, round(viewCnt * (0.008 + rnd(`co:${categoryId}`) * 0.004))),
        view_uv: Math.max(1, round(viewCnt * 0.78)),
        transaction_uv: Math.max(1, round(viewCnt * 0.007)),
      }
    }),
  ]

  const behavior: ItemBehaviorPoint[] = stats.map((item) => ({
    date: item.date,
    items_viewed: item.items_viewed,
    items_carted: item.items_carted,
    items_bought: item.items_bought,
  }))

  return { items, categories, behavior }
}

function getRetention(
  fromParam: string | null,
  toParam: string | null,
  maxAgeParam: number,
): RetentionResponse {
  const { from, to } = clampRange(fromParam, toParam)
  const maxAge = Math.max(0, Math.min(maxAgeParam, 30))
  const cohorts: RetentionCohort[] = filteredStats(from, to).map((item) => {
    const size = newUsers(item.date)
    const values: RetentionCohort['values'] = []
    for (let age = 0; age <= maxAge; age += 1) {
      const target = addDays(item.date, age)
      if (target > DATA_MAX) break
      const rate =
        age === 0
          ? 1
          : round6(
              Math.max(
                0.0008,
                d1Rate(item.date) *
                  Math.pow(0.84, age - 1) *
                  (0.92 + rnd(`age:${item.date}:${age}`) * 0.16),
              ),
            )
      values.push({
        age_day: age,
        rate,
        window_status:
          item.date < FULL_MIN || target >= DATA_MAX ? 'partial' : 'full',
      })
    }
    return {
      cohort_date: item.date,
      cohort_size: size,
      window_status:
        item.date >= FULL_MIN && item.date <= FULL_COHORT_MAX ? 'full' : 'partial',
      values,
    }
  })

  const series = d1Series(from, to)
  let numerator = 0
  let denominator = 0
  for (const cohort of cohorts) {
    const d1 = cohort.values.find((value) => value.age_day === 1)
    const status = d1?.window_status ?? cohort.window_status
    if (status !== 'full' || !d1 || d1.rate === null) continue
    numerator += cohort.cohort_size * d1.rate
    denominator += cohort.cohort_size
  }

  return {
    cohorts,
    d1_series: series,
    weighted_d1: denominator > 0 ? round6(numerator / denominator) : null,
  }
}

function metricValue(stat: DayStat, metric: string): number {
  switch (metric) {
    case 'pv':
      return stat.pv
    case 'uv':
      return stat.uv
    case 'dau':
      return stat.dau
    case 'view_users':
      return stat.uv
    case 'cart_users':
      return stat.cart_users
    case 'buyer_users':
      return stat.buyer_users
    case 'orders':
      return stat.orders
    case 'add_to_cart_rate':
      return stat.add_to_cart_rate
    case 'conv_strict_rate':
      return stat.conv_strict_rate
    case 'conv_global_rate':
      return stat.conv_global_rate
    case 'items_viewed':
      return stat.items_viewed
    case 'items_carted':
      return stat.items_carted
    case 'items_bought':
      return stat.items_bought
    default:
      return 0
  }
}

function getRealtime(): RealtimeResponse {
  // 最近 7 个已完成对账的日期（末日部分日尚未对账，不放入对比表）
  const compareDates = ALL_DATES.slice(-8, -1)
  const compare: ReconcileRow[] = []
  for (const date of [...compareDates].reverse()) {
    const stat = DAY_STATS.find((item) => item.date === date)
    if (!stat) continue
    for (const metric of METRICS) {
      const isSkip = date === DATA_MAX
      compare.push({
        date,
        metric,
        offline: isSkip ? null : metricValue(stat, metric),
        realtime: isSkip ? null : metricValue(stat, metric),
        status: isSkip ? 'SKIP' : 'MATCH',
      })
    }
  }

  return {
    tables: [
      {
        table: 'rt_traffic_daily',
        rows: 139,
        final_rows: 138,
        latest_date: DATA_MAX,
        updated_at: '2026-11-07T03:20:00Z',
      },
      {
        table: 'rt_funnel_daily',
        rows: 973,
        final_rows: 966,
        latest_date: DATA_MAX,
        updated_at: '2026-11-07T03:20:00Z',
      },
      {
        table: 'rt_item_behavior_daily',
        rows: 139,
        final_rows: 138,
        latest_date: DATA_MAX,
        updated_at: '2026-11-07T03:20:00Z',
      },
    ],
    reconciliation: {
      MATCH: 1781,
      DIFF: 0,
      SKIP: 26,
      checked_at: '2026-11-07T03:21:00Z',
    },
    compare,
    late_events: 0,
  }
}

const RULE_TABLES = [
  'ads_traffic_daily',
  'ads_funnel_daily',
  'ads_funnel_interval',
  'ads_item_ranking_daily',
  'ads_item_behavior_daily',
  'ads_retention_cohort',
  'ads_publish_log',
  'quality_check_run',
  'quality_check_result',
  'airflow_pipeline_run',
  'etl_watermark',
  'rt_traffic_daily',
  'rt_funnel_daily',
  'rt_item_behavior_daily',
  'rt_reconciliation_daily',
]

const RULE_CATEGORIES = ['completeness', 'uniqueness', 'validity', 'range']

const QUALITY_RUNS: QualityRun[] = [
  {
    run_id: 8,
    scope_type: 'DATES',
    dates_csv: '2015-09-10,2015-09-11,2015-09-12,2015-09-13,2015-09-14,2015-09-15,2015-09-16,2015-09-17,2015-09-18',
    status: 'PASS',
    rules_total: 60,
    rules_passed: 60,
    rules_failed: 0,
    checks_total: 76488744,
    started_at: '2026-11-07T03:00:00Z',
    finished_at: '2026-11-07T03:03:29Z',
  },
  {
    run_id: 7,
    scope_type: 'FULL',
    dates_csv: null,
    status: 'PASS',
    rules_total: 60,
    rules_passed: 60,
    rules_failed: 0,
    checks_total: 89305119,
    started_at: '2026-11-06T02:00:00Z',
    finished_at: '2026-11-06T02:03:30Z',
  },
  {
    run_id: 6,
    scope_type: 'DATES',
    dates_csv: '2015-09-02,2015-09-03,2015-09-04',
    status: 'FAIL',
    rules_total: 60,
    rules_passed: 58,
    rules_failed: 2,
    checks_total: 4551231,
    started_at: '2026-11-05T02:00:00Z',
    finished_at: '2026-11-05T02:00:18Z',
  },
  {
    run_id: 5,
    scope_type: 'DATES',
    dates_csv: '2015-08-20,2015-08-21,2015-08-22,2015-08-23,2015-08-24',
    status: 'PASS',
    rules_total: 60,
    rules_passed: 60,
    rules_failed: 0,
    checks_total: 12033110,
    started_at: '2026-11-04T02:00:00Z',
    finished_at: '2026-11-04T02:00:41Z',
  },
  {
    run_id: 4,
    scope_type: 'FULL',
    dates_csv: null,
    status: 'PASS',
    rules_total: 60,
    rules_passed: 60,
    rules_failed: 0,
    checks_total: 89120987,
    started_at: '2026-11-03T02:00:00Z',
    finished_at: '2026-11-03T02:03:22Z',
  },
  {
    run_id: 3,
    scope_type: 'DATES',
    dates_csv: '2015-07-01,2015-07-02,2015-07-03,2015-07-04,2015-07-05',
    status: 'PASS',
    rules_total: 60,
    rules_passed: 60,
    rules_failed: 0,
    checks_total: 11887332,
    started_at: '2026-11-02T02:00:00Z',
    finished_at: '2026-11-02T02:00:39Z',
  },
  {
    run_id: 2,
    scope_type: 'DATES',
    dates_csv: '2015-06-10,2015-06-11,2015-06-12',
    status: 'PASS',
    rules_total: 60,
    rules_passed: 60,
    rules_failed: 0,
    checks_total: 6120344,
    started_at: '2026-11-01T02:00:00Z',
    finished_at: '2026-11-01T02:00:21Z',
  },
  {
    run_id: 1,
    scope_type: 'FULL',
    dates_csv: null,
    status: 'PASS',
    rules_total: 60,
    rules_passed: 60,
    rules_failed: 0,
    checks_total: 88776012,
    started_at: '2026-10-31T02:00:00Z',
    finished_at: '2026-10-31T02:03:18Z',
  },
]

function buildRules(runId: number): QualityRule[] {
  const failed = runId === 6
  const rules: QualityRule[] = []
  RULE_TABLES.forEach((table, tableIndex) => {
    RULE_CATEGORIES.forEach((category, categoryIndex) => {
      const index = tableIndex * 4 + categoryIndex
      const isFailed = failed && (index === 17 || index === 42)
      rules.push({
        rule_key: `${category}.${table}.rule_${String(index + 1).padStart(2, '0')}`,
        category,
        status: isFailed ? 'FAIL' : 'PASS',
        checked: round(1200 + rnd(`checked:${runId}:${index}`) * 2500000),
        failed_rows: isFailed ? 3 + (index % 5) : 0,
        detail: isFailed
          ? `校验 ${table} 时发现 ${3 + (index % 5)} 行不满足 ${category} 约束，示例：${table} 最近分区出现空值/重复键，已阻止发布。`
          : null,
        duration_ms: round(8 + rnd(`duration:${runId}:${index}`) * 420),
      })
    })
  })
  return rules
}

function getQualityRuns(limit: number): QualityRunsResponse {
  return { runs: QUALITY_RUNS.slice(0, Math.max(1, limit)) }
}

function getQualityRunDetail(runId: number): QualityRunDetail {
  const run = QUALITY_RUNS.find((item) => item.run_id === runId) ?? QUALITY_RUNS[0]
  return { ...run, rules: buildRules(run.run_id) }
}

function getOps(): OpsResponse {
  return {
    pipeline_runs: [
      {
        run_id: 7,
        dag_run_id: 'scheduled__2026-11-07T03:00:00+00:00',
        mode: 'FORCE',
        window_start: '2015-09-10',
        window_end: '2015-09-18',
        status: 'SUCCESS',
        published: 'PUBLISHED',
        started_at: '2026-11-07T03:00:00Z',
        finished_at: '2026-11-07T03:13:05Z',
      },
      {
        run_id: 6,
        dag_run_id: 'scheduled__2026-11-06T02:00:00+00:00',
        mode: 'TAIL',
        window_start: '2015-09-02',
        window_end: '2015-09-09',
        status: 'SUCCESS',
        published: 'PUBLISHED',
        started_at: '2026-11-06T02:00:00Z',
        finished_at: '2026-11-06T02:06:47Z',
      },
      {
        run_id: 5,
        dag_run_id: 'manual__2026-11-05T02:00:00+00:00',
        mode: 'FORCE',
        window_start: '2015-08-20',
        window_end: '2015-08-24',
        status: 'FAILED',
        published: null,
        started_at: '2026-11-05T02:00:00Z',
        finished_at: '2026-11-05T02:00:31Z',
      },
      {
        run_id: 4,
        dag_run_id: 'scheduled__2026-11-04T02:00:00+00:00',
        mode: 'TAIL',
        window_start: '2015-07-01',
        window_end: '2015-07-05',
        status: 'SUCCESS',
        published: 'PUBLISHED',
        started_at: '2026-11-04T02:00:00Z',
        finished_at: '2026-11-04T02:04:12Z',
      },
      {
        run_id: 3,
        dag_run_id: 'scheduled__2026-11-03T02:00:00+00:00',
        mode: 'TAIL',
        window_start: '2015-06-10',
        window_end: '2015-06-12',
        status: 'SUCCESS',
        published: 'PUBLISHED',
        started_at: '2026-11-03T02:00:00Z',
        finished_at: '2026-11-03T02:02:58Z',
      },
      {
        run_id: 2,
        dag_run_id: 'scheduled__2026-11-02T02:00:00+00:00',
        mode: 'TAIL',
        window_start: '2015-05-20',
        window_end: '2015-05-26',
        status: 'SUCCESS',
        published: 'PUBLISHED',
        started_at: '2026-11-02T02:00:00Z',
        finished_at: '2026-11-02T02:03:41Z',
      },
      {
        run_id: 1,
        dag_run_id: 'manual__2026-11-01T02:00:00+00:00',
        mode: 'FORCE',
        window_start: DATA_MIN,
        window_end: DATA_MAX,
        status: 'SUCCESS',
        published: 'PUBLISHED',
        started_at: '2026-11-01T02:00:00Z',
        finished_at: '2026-11-01T02:18:22Z',
      },
    ],
    publish_log: [
      {
        publish_id: 13,
        scope_type: 'DATES',
        dates_csv: '2015-09-10,2015-09-11,2015-09-12,2015-09-13,2015-09-14,2015-09-15,2015-09-16,2015-09-17,2015-09-18',
        quality_status: 'PASS',
        published_at: '2026-11-07T03:12:41Z',
      },
      {
        publish_id: 12,
        scope_type: 'FULL',
        dates_csv: null,
        quality_status: 'PASS',
        published_at: '2026-11-06T02:03:31Z',
      },
      {
        publish_id: 11,
        scope_type: 'DATES',
        dates_csv: '2015-08-20,2015-08-21,2015-08-22,2015-08-23,2015-08-24',
        quality_status: 'PASS',
        published_at: '2026-11-04T02:00:42Z',
      },
      {
        publish_id: 10,
        scope_type: 'DATES',
        dates_csv: '2015-07-01,2015-07-02,2015-07-03,2015-07-04,2015-07-05',
        quality_status: 'PASS',
        published_at: '2026-11-03T02:00:40Z',
      },
      {
        publish_id: 9,
        scope_type: 'DATES',
        dates_csv: '2015-06-10,2015-06-11,2015-06-12',
        quality_status: 'PASS',
        published_at: '2026-11-02T02:00:22Z',
      },
      {
        publish_id: 8,
        scope_type: 'FULL',
        dates_csv: null,
        quality_status: 'PASS',
        published_at: '2026-10-31T02:03:19Z',
      },
    ],
    watermark: [
      {
        pipeline: 'dwd',
        event_date_utc: DATA_MAX,
        max_ingest_batch: 17,
        processed_at: '2026-11-07T03:00:42Z',
      },
      {
        pipeline: 'dws_ads',
        event_date_utc: DATA_MAX,
        max_ingest_batch: 17,
        processed_at: '2026-11-07T03:08:19Z',
      },
      {
        pipeline: 'dwd',
        event_date_utc: '2015-09-17',
        max_ingest_batch: 16,
        processed_at: '2026-11-06T02:00:38Z',
      },
      {
        pipeline: 'dws_ads',
        event_date_utc: '2015-09-17',
        max_ingest_batch: 16,
        processed_at: '2026-11-06T02:05:10Z',
      },
    ],
    table_stats: [
      { table: 'dwd_events', rows: 2755641 },
      { table: 'ads_traffic_daily', rows: 139 },
      { table: 'ads_funnel_daily', rows: 1251 },
      { table: 'ads_item_ranking_daily', rows: 13900 },
      { table: 'ads_item_behavior_daily', rows: 139 },
      { table: 'ads_retention_cohort', rows: 278 },
      { table: 'quality_check_run', rows: 8 },
      { table: 'quality_check_result', rows: 480 },
      { table: 'airflow_pipeline_run', rows: 7 },
      { table: 'ads_publish_log', rows: 13 },
      { table: 'etl_watermark', rows: 278 },
      { table: 'rt_traffic_daily', rows: 139 },
      { table: 'rt_reconciliation_daily', rows: 1807 },
    ],
  }
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException('请求已取消', 'AbortError'))
      return
    }
    const timer = setTimeout(resolve, ms)
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timer)
        reject(new DOMException('请求已取消', 'AbortError'))
      },
      { once: true },
    )
  })
}

export async function mockApi<T>(input: string, signal?: AbortSignal): Promise<T> {
  await delay(240, signal)
  const url = new URL(input, 'http://mock.local')
  const path = url.pathname.replace(/^\/api/, '')
  const params = url.searchParams
  const from = params.get('date_from')
  const to = params.get('date_to')

  let payload: unknown
  if (path === '/health') {
    payload = { status: 'ok', ts: new Date().toISOString() }
  } else if (path === '/meta') {
    payload = getMeta()
  } else if (path === '/overview') {
    payload = getOverview(from, to)
  } else if (path === '/items') {
    const limit = Number(params.get('limit') ?? 20)
    payload = getItems(from, to, Number.isFinite(limit) ? limit : 20)
  } else if (path === '/retention') {
    const maxAge = Number(params.get('max_age') ?? 14)
    payload = getRetention(from, to, Number.isFinite(maxAge) ? maxAge : 14)
  } else if (path === '/realtime') {
    payload = getRealtime()
  } else if (path === '/quality/runs') {
    const limit = Number(params.get('limit') ?? 20)
    payload = getQualityRuns(Number.isFinite(limit) ? limit : 20)
  } else if (/^\/quality\/runs\/\d+$/.test(path)) {
    payload = getQualityRunDetail(Number(path.split('/').pop()))
  } else if (path === '/ops') {
    payload = getOps()
  } else {
    throw new Error(`mock 未实现接口：${path}`)
  }

  return payload as T
}
