import { useMemo, useState } from 'react'
import { apiUrl } from '../api/client'
import { useApi } from '../api/useApi'
import type { RealtimeResponse, RealtimeTable, ReconcileRow } from '../api/types'
import { DataTable, type Column } from '../components/DataTable'
import { EmptyState } from '../components/EmptyState'
import { ErrorState } from '../components/ErrorState'
import { Skeleton, ChartSkeleton, KpiSkeleton } from '../components/Skeleton'
import { StatusPill } from '../components/StatusPill'
import {
  formatDateTime,
  formatInt,
  formatPct,
  formatSignedInt,
} from '../utils/format'

const TABLE_LABELS: Record<string, string> = {
  rt_traffic_daily: '流量日结果',
  rt_funnel_daily: '漏斗日结果',
  rt_item_behavior_daily: '商品行为日结果',
  rt_reconciliation_daily: '对账审计',
  rt_late_events: '迟到事件旁路',
}

const METRIC_LABELS: Record<string, string> = {
  pv: 'PV',
  uv: 'UV',
  dau: 'DAU',
  view_users: '浏览用户',
  cart_users: '加购用户',
  buyer_users: '成交用户',
  orders: '订单数',
  add_to_cart_rate: '加购率',
  conv_strict_rate: '严格转化率',
  conv_global_rate: '全局转化率',
  items_viewed: '浏览商品数',
  items_carted: '加购商品数',
  items_bought: '成交商品数',
}

const METRIC_ORDER = Object.keys(METRIC_LABELS)

function isRateMetric(metric: string): boolean {
  return metric.endsWith('_rate')
}

function formatMetric(value: number | null, metric: string): string {
  return isRateMetric(metric) ? formatPct(value) : formatInt(value)
}

function formatDiff(row: ReconcileRow): string {
  if (row.offline === null || row.realtime === null) return '—'
  const diff = row.realtime - row.offline
  if (isRateMetric(row.metric)) {
    const pp = diff * 100
    return `${pp > 0 ? '+' : ''}${pp.toFixed(2)}pp`
  }
  return formatSignedInt(diff)
}

function RealtimeTableCard({ table }: { table: RealtimeTable }) {
  const finalized =
    table.final_rows !== null && table.final_rows !== undefined && table.final_rows >= table.rows
  const pending =
    table.final_rows !== null && table.final_rows !== undefined
      ? Math.max(0, table.rows - table.final_rows)
      : null

  return (
    <div className="card rt-card">
      <div className="rt-card-head">
        <div>
          <div className="rt-name">{TABLE_LABELS[table.table] ?? table.table}</div>
          <div className="rt-raw">{table.table}</div>
        </div>
        <StatusPill status={finalized ? 'FINAL' : '累积中'} />
      </div>

      <div className="rt-metrics">
        <div className="rt-metric">
          <span>行数</span>
          <strong>{formatInt(table.rows)}</strong>
        </div>
        <div className="rt-metric">
          <span>已定稿</span>
          <strong>{formatInt(table.final_rows)}</strong>
        </div>
        <div className="rt-metric">
          <span>未定稿</span>
          <strong>{formatInt(pending)}</strong>
        </div>
      </div>

      <div className="rt-foot">
        最新日期 {table.latest_date ?? '—'} · 更新于 {formatDateTime(table.updated_at)} UTC
      </div>
    </div>
  )
}

export default function RealtimePage() {
  const [metricFilter, setMetricFilter] = useState('all')
  const url = useMemo(() => apiUrl('/realtime'), [])
  const { data, loading, error, reload } = useApi<RealtimeResponse>(url)

  const metrics = useMemo(() => {
    const present = new Set((data?.compare ?? []).map((row) => row.metric))
    const known = METRIC_ORDER.filter((metric) => present.has(metric))
    const extra = [...present].filter((metric) => !METRIC_ORDER.includes(metric)).sort()
    return [...known, ...extra]
  }, [data])

  const activeMetric =
    metricFilter === 'all' || metrics.includes(metricFilter) ? metricFilter : 'all'

  const compareRows = useMemo(() => {
    const rows = [...(data?.compare ?? [])]
    const rank = (metric: string) => {
      const index = METRIC_ORDER.indexOf(metric)
      return index === -1 ? METRIC_ORDER.length : index
    }
    // 保持日期升序；同一日期内按业务口径顺序（PV/UV/DAU → 漏斗 → 商品）排列。
    rows.sort((a, b) =>
      a.date === b.date ? rank(a.metric) - rank(b.metric) : a.date.localeCompare(b.date),
    )
    if (activeMetric === 'all') return rows
    return rows.filter((row) => row.metric === activeMetric)
  }, [data, activeMetric])

  const compareDates = useMemo(
    () => new Set((data?.compare ?? []).map((row) => row.date)).size,
    [data],
  )

  const compareColumns: Column<ReconcileRow>[] = [
    {
      key: 'date',
      title: '日期',
      render: (row) => <span className="mono">{row.date}</span>,
      width: '14%',
    },
    {
      key: 'metric',
      title: '指标',
      render: (row) => (
        <span title={row.metric}>
          {METRIC_LABELS[row.metric] ?? row.metric}
          <span className="metric-code"> {row.metric}</span>
        </span>
      ),
      width: '24%',
    },
    {
      key: 'offline',
      title: '离线 ADS',
      align: 'right',
      width: '14%',
      render: (row) => formatMetric(row.offline, row.metric),
    },
    {
      key: 'realtime',
      title: '实时 rt',
      align: 'right',
      width: '14%',
      render: (row) => formatMetric(row.realtime, row.metric),
    },
    {
      key: 'diff',
      title: '差异',
      align: 'right',
      width: '12%',
      render: (row) => formatDiff(row),
    },
    {
      key: 'status',
      title: '状态',
      render: (row) => <StatusPill status={row.status} />,
      width: '14%',
    },
  ]

  if (loading) {
    return (
      <div className="page-stack">
        <div className="rt-grid">
          {Array.from({ length: 3 }, (_, index) => (
            <div className="card rt-card" key={index}>
              <Skeleton width={120} height={14} />
              <Skeleton width={90} height={10} style={{ marginTop: 8 }} />
              <Skeleton width="100%" height={34} style={{ marginTop: 18 }} />
              <Skeleton width="70%" height={10} style={{ marginTop: 16 }} />
            </div>
          ))}
        </div>
        <div className="recon-grid">
          {Array.from({ length: 4 }, (_, index) => (
            <KpiSkeleton key={index} />
          ))}
        </div>
        <ChartSkeleton height={480} />
      </div>
    )
  }

  if (error) {
    return (
      <div className="card card-pad">
        <ErrorState message={error} onRetry={reload} />
      </div>
    )
  }

  if (!data) return null

  const match = Number(data.reconciliation.MATCH ?? 0)
  const diff = Number(data.reconciliation.DIFF ?? 0)
  const skip = Number(data.reconciliation.SKIP ?? 0)
  const checkedAt =
    typeof data.reconciliation.checked_at === 'string' ? data.reconciliation.checked_at : null
  const late = data.late_events ?? 0

  return (
    <div className="page-stack">
      <div className="rt-grid">
        {data.tables.length > 0 ? (
          data.tables.map((table) => <RealtimeTableCard key={table.table} table={table} />)
        ) : (
          <div className="card card-pad" style={{ gridColumn: '1 / -1' }}>
            <EmptyState text="暂无实时结果表" />
          </div>
        )}
      </div>

      <section>
        <div className="section-head">
          <h2 className="section-title">对账汇总</h2>
          <span className="page-note">
            {checkedAt ? `对账时间 ${formatDateTime(checkedAt)} UTC` : '对账时间 —'}
          </span>
        </div>
        <div className="recon-grid">
          <div className="card recon-card">
            <div className="recon-label">
              <StatusPill status="MATCH" /> <span>一致</span>
            </div>
            <div className="recon-value num">{formatInt(match)}</div>
            <div className="recon-meta">离线与实时完全一致</div>
          </div>
          <div className="card recon-card">
            <div className="recon-label">
              <StatusPill status="DIFF" /> <span>差异</span>
            </div>
            <div className={diff > 0 ? 'recon-value recon-diag num' : 'recon-value num'}>
              {formatInt(diff)}
            </div>
            <div className="recon-meta">需排查的指标数量</div>
          </div>
          <div className="card recon-card">
            <div className="recon-label">
              <StatusPill status="SKIP" /> <span>跳过</span>
            </div>
            <div className="recon-value num">{formatInt(skip)}</div>
            <div className="recon-meta">部分日或不可比，不参与判定</div>
          </div>
          <div className="card recon-card">
            <div className="recon-label">
              <span className={`status ${late > 0 ? 'status-critical' : 'status-good'}`}>
                <span className="status-dot" aria-hidden="true" />
                <span>{late > 0 ? 'LATE' : '正常'}</span>
              </span>
              <span>迟到事件</span>
            </div>
            <div className={late > 0 ? 'recon-value recon-diag num' : 'recon-value num'}>
              {formatInt(late)}
            </div>
            <div className="recon-meta">超过允许延迟的事件数</div>
          </div>
        </div>
      </section>

      <section className="card ops-card">
        <div className="section-head">
          <div>
            <h2 className="section-title">离线 vs 实时对比</h2>
            <p className="page-note">
              最近 {compareDates} 个已对账日期 · {metrics.length} 个指标；差异 = 实时 − 离线
            </p>
          </div>
          <label className="metric-filter">
            指标
            <select
              className="input"
              value={activeMetric}
              onChange={(event) => setMetricFilter(event.target.value)}
            >
              <option value="all">全部指标</option>
              {metrics.map((metric) => (
                <option key={metric} value={metric}>
                  {METRIC_LABELS[metric] ?? metric}
                </option>
              ))}
            </select>
          </label>
        </div>
        <DataTable
          columns={compareColumns}
          rows={compareRows}
          rowKey={(row) => `${row.date}-${row.metric}`}
          emptyText="暂无对账数据"
          maxHeight={480}
          caption="离线与实时指标对比表"
        />
      </section>
    </div>
  )
}
