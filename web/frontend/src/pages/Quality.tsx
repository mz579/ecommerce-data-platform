import { useMemo, useState } from 'react'
import { apiUrl } from '../api/client'
import { useApi } from '../api/useApi'
import type {
  OpsResponse,
  PipelineRun,
  PublishLogRow,
  QualityRule,
  QualityRun,
  QualityRunDetail,
  QualityRunsResponse,
  WatermarkRow,
} from '../api/types'
import { DataTable, type Column } from '../components/DataTable'
import { EmptyState } from '../components/EmptyState'
import { ErrorState } from '../components/ErrorState'
import { KpiCard } from '../components/KpiCard'
import { Skeleton } from '../components/Skeleton'
import { StatusPill } from '../components/StatusPill'
import { formatDateTime, formatInt } from '../utils/format'

function scopeText(run: QualityRun): string {
  if (run.scope_type === 'FULL') return '全量重建'
  const days = run.dates_csv ? run.dates_csv.split(',').filter(Boolean).length : 0
  return `按日 · ${days} 天`
}

function KpiRowSkeleton() {
  return (
    <div className="kpi-grid">
      {Array.from({ length: 4 }, (_, index) => (
        <div className="card kpi-card" key={index}>
          <Skeleton width={70} height={11} />
          <Skeleton width="52%" height={26} style={{ marginTop: 14 }} />
          <Skeleton width="68%" height={10} style={{ marginTop: 10 }} />
        </div>
      ))}
    </div>
  )
}

function TableBlockSkeleton({ height = 220 }: { height?: number }) {
  return (
    <div className="card ops-card">
      <Skeleton width={130} height={13} />
      <Skeleton width="100%" height={height} radius={8} style={{ marginTop: 12 }} />
    </div>
  )
}

export default function QualityPage() {
  const runsUrl = useMemo(() => apiUrl('/quality/runs', { limit: 20 }), [])
  const opsUrl = useMemo(() => apiUrl('/ops'), [])
  const runsResult = useApi<QualityRunsResponse>(runsUrl)
  const opsResult = useApi<OpsResponse>(opsUrl)
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null)

  const runs = runsResult.data?.runs ?? []
  const activeRun = runs.find((run) => run.run_id === selectedRunId) ?? runs[0] ?? null
  const detailUrl = activeRun ? apiUrl(`/quality/runs/${activeRun.run_id}`) : null
  const detailResult = useApi<QualityRunDetail>(detailUrl)

  const latest = runs[0] ?? null
  const detail = detailResult.data

  const ruleColumns: Column<QualityRule>[] = [
    {
      key: 'rule_key',
      title: '规则',
      width: '38%',
      render: (row) => (
        <div className="rule-cell">
          <span className="rule-name mono" title={row.rule_key}>
            {row.rule_key}
          </span>
          <span className="category-tag">{row.category}</span>
        </div>
      ),
    },
    {
      key: 'status',
      title: '状态',
      width: 90,
      render: (row) => <StatusPill status={row.status} />,
    },
    {
      key: 'checked',
      title: '比对行数',
      align: 'right',
      render: (row) => formatInt(row.checked),
    },
    {
      key: 'failed_rows',
      title: '失败行',
      align: 'right',
      render: (row) =>
        row.failed_rows > 0 ? (
          <span className="cell-critical">{formatInt(row.failed_rows)}</span>
        ) : (
          formatInt(row.failed_rows)
        ),
    },
    {
      key: 'duration_ms',
      title: '耗时 ms',
      align: 'right',
      render: (row) => formatInt(row.duration_ms),
    },
    {
      key: 'detail',
      title: '详情',
      render: (row) =>
        row.detail ? (
          <span className="detail-cell" title={row.detail}>
            {row.detail}
          </span>
        ) : (
          <span className="muted">—</span>
        ),
    },
  ]

  const pipelineColumns: Column<PipelineRun>[] = [
    {
      key: 'run_id',
      title: '运行号',
      width: 70,
      render: (row) => <span className="mono">#{row.run_id}</span>,
    },
    {
      key: 'mode',
      title: '模式',
      width: 80,
      render: (row) => <span className="category-tag">{row.mode}</span>,
    },
    {
      key: 'window',
      title: '窗口',
      render: (row) => (
        <span className="mono">
          {row.window_start} ~ {row.window_end}
        </span>
      ),
    },
    {
      key: 'status',
      title: '状态',
      render: (row) => <StatusPill status={row.status} />,
    },
    {
      key: 'published',
      title: '发布',
      render: (row) =>
        row.published ? <StatusPill status={row.published} /> : <span className="muted">—</span>,
    },
    {
      key: 'started_at',
      title: '开始（UTC）',
      render: (row) => <span className="mono">{formatDateTime(row.started_at)}</span>,
    },
    {
      key: 'finished_at',
      title: '结束（UTC）',
      render: (row) => <span className="mono">{formatDateTime(row.finished_at)}</span>,
    },
  ]

  const publishColumns: Column<PublishLogRow>[] = [
    {
      key: 'publish_id',
      title: '发布号',
      width: 70,
      render: (row) => <span className="mono">#{row.publish_id}</span>,
    },
    {
      key: 'scope',
      title: '范围',
      render: (row) => {
        const days = row.dates_csv ? row.dates_csv.split(',').filter(Boolean).length : 0
        return (
          <span title={row.dates_csv ?? '全库'}>
            <StatusPill status={row.scope_type} />{' '}
            <span className="muted">
              {row.scope_type === 'FULL' ? '' : `${days} 天`}
            </span>
          </span>
        )
      },
    },
    {
      key: 'quality_status',
      title: '质量',
      render: (row) => <StatusPill status={row.quality_status} />,
    },
    {
      key: 'published_at',
      title: '发布时间（UTC）',
      render: (row) => <span className="mono">{formatDateTime(row.published_at)}</span>,
    },
  ]

  const watermarkColumns: Column<WatermarkRow>[] = [
    {
      key: 'pipeline',
      title: '流水线',
      render: (row) => <span className="mono">{row.pipeline}</span>,
    },
    {
      key: 'event_date_utc',
      title: '水位日期',
      render: (row) => <span className="mono">{row.event_date_utc}</span>,
    },
    {
      key: 'max_ingest_batch',
      title: '最大批次',
      align: 'right',
      render: (row) => formatInt(row.max_ingest_batch),
    },
    {
      key: 'processed_at',
      title: '处理时间（UTC）',
      render: (row) => <span className="mono">{formatDateTime(row.processed_at)}</span>,
    },
  ]

  const ops = opsResult.data

  return (
    <div className="page-stack">
      {runsResult.loading ? (
        <KpiRowSkeleton />
      ) : latest ? (
        <div className="kpi-grid">
          <KpiCard
            label="最新门禁"
            value={
              <span
                className={`status ${
                  latest.status === 'PASS' ? 'status-good' : 'status-critical'
                } kpi-status`}
              >
                <span className="status-dot" aria-hidden="true" />
                <span>{latest.status}</span>
              </span>
            }
            hint={`运行 #${latest.run_id} · ${formatDateTime(latest.finished_at)} UTC`}
          />
          <KpiCard
            label="规则通过"
            value={`${formatInt(latest.rules_passed)} / ${formatInt(latest.rules_total)}`}
            hint={`失败 ${formatInt(latest.rules_failed)} 条规则`}
          />
          <KpiCard
            label="比对行数"
            value={formatInt(latest.checks_total)}
            hint="门禁全部规则的比对对象行数之和"
          />
          <KpiCard
            label="失败规则"
            value={formatInt(latest.rules_failed)}
            hint={latest.rules_failed > 0 ? '存在失败规则，发布被阻止' : '全部门禁规则通过'}
          />
        </div>
      ) : null}

      <div className="quality-layout">
        <section className="card ops-card">
          <div className="section-head">
            <h2 className="section-title">门禁运行</h2>
            <span className="page-note">最近 {runs.length} 次</span>
          </div>

          {runsResult.loading ? (
            <div className="run-list">
              {Array.from({ length: 6 }, (_, index) => (
                <div className="run-item" key={index} aria-hidden="true">
                  <Skeleton width={120} height={12} />
                  <Skeleton width="70%" height={10} style={{ marginTop: 8 }} />
                  <Skeleton width="50%" height={9} style={{ marginTop: 6 }} />
                </div>
              ))}
            </div>
          ) : runsResult.error ? (
            <ErrorState message={runsResult.error} onRetry={runsResult.reload} />
          ) : runs.length === 0 ? (
            <EmptyState text="暂无质量门禁运行" />
          ) : (
            <div className="run-list">
              {runs.map((run) => {
                const active = activeRun?.run_id === run.run_id
                return (
                  <button
                    key={run.run_id}
                    type="button"
                    className={active ? 'run-item active' : 'run-item'}
                    onClick={() => setSelectedRunId(run.run_id)}
                    aria-pressed={active}
                  >
                    <div className="run-item-top">
                      <span className="run-id">#{run.run_id}</span>
                      <StatusPill status={run.status} />
                    </div>
                    <div className="run-scope">
                      {scopeText(run)} · 规则 {formatInt(run.rules_passed)}/
                      {formatInt(run.rules_total)}
                    </div>
                    <div className="run-time">{formatDateTime(run.started_at)} UTC</div>
                  </button>
                )
              })}
            </div>
          )}
        </section>

        <section className="card ops-card">
          <div className="section-head">
            <h2 className="section-title">规则明细</h2>
            {detail ? <span className="page-note">运行 #{detail.run_id}</span> : null}
          </div>

          {detailResult.loading ? (
            <div>
              <Skeleton width={280} height={12} style={{ marginBottom: 12 }} />
              <Skeleton width="100%" height={380} radius={8} />
            </div>
          ) : detailResult.error ? (
            <ErrorState message={detailResult.error} onRetry={detailResult.reload} />
          ) : detail ? (
            <>
              <div className="run-detail-head">
                <span>
                  <strong>#{detail.run_id}</strong> · {scopeText(detail)}
                </span>
                <StatusPill status={detail.status} />
                <span>规则 {formatInt(detail.rules_passed)}/{formatInt(detail.rules_total)}</span>
                <span>比对 {formatInt(detail.checks_total)}</span>
                <span>
                  {formatDateTime(detail.started_at)} → {formatDateTime(detail.finished_at)} UTC
                </span>
              </div>
              <DataTable
                columns={ruleColumns}
                rows={detail.rules}
                rowKey={(row) => row.rule_key}
                emptyText="该运行暂无规则明细"
                maxHeight={440}
                caption="质量门禁逐规则结果"
              />
              <p className="page-note table-foot-note">
                共 {formatInt(detail.rules.length)} 条规则，表内滚动查看。
              </p>
            </>
          ) : (
            <EmptyState text="请选择左侧的运行查看规则明细" />
          )}
        </section>
      </div>

      {opsResult.loading ? (
        <div className="page-stack">
          <TableBlockSkeleton height={240} />
          <div className="grid-2-even">
            <TableBlockSkeleton height={200} />
            <TableBlockSkeleton height={200} />
          </div>
          <TableBlockSkeleton height={180} />
        </div>
      ) : opsResult.error ? (
        <div className="card card-pad">
          <ErrorState message={opsResult.error} onRetry={opsResult.reload} />
        </div>
      ) : ops ? (
        <>
          <section className="card ops-card">
            <div className="section-head">
              <h2 className="section-title">管道运行记录</h2>
              <span className="page-note">最近 {ops.pipeline_runs.length} 次 DAG 运行</span>
            </div>
            <DataTable
              columns={pipelineColumns}
              rows={ops.pipeline_runs}
              rowKey={(row) => row.run_id}
              emptyText="暂无管道运行记录"
              maxHeight={320}
              caption="Airflow 管道运行记录"
            />
          </section>

          <div className="grid-2-even">
            <section className="card ops-card">
              <div className="section-head">
                <h2 className="section-title">发布记录</h2>
                <span className="page-note">仅记录门禁通过的发布</span>
              </div>
              <DataTable
                columns={publishColumns}
                rows={ops.publish_log}
                rowKey={(row) => row.publish_id}
                emptyText="暂无发布记录"
                maxHeight={300}
                caption="ADS 发布记录"
              />
            </section>

            <section className="card ops-card">
              <div className="section-head">
                <h2 className="section-title">增量水位线</h2>
                <span className="page-note">按业务日期推进</span>
              </div>
              <DataTable
                columns={watermarkColumns}
                rows={ops.watermark}
                rowKey={(row) => `${row.pipeline}-${row.event_date_utc}`}
                emptyText="暂无水位线数据"
                maxHeight={300}
                caption="ETL 水位线"
              />
            </section>
          </div>

          <section className="card ops-card">
            <div className="section-head">
              <h2 className="section-title">表行数</h2>
              <span className="page-note">dwd_events 为 information_schema 近似值（≈）</span>
            </div>
            {ops.table_stats.length === 0 ? (
              <EmptyState text="暂无表行数信息" />
            ) : (
              <div className="table-stat-grid">
                {ops.table_stats.map((stat) => (
                  <div className="table-stat" key={stat.table}>
                    <span className="table-stat-name" title={stat.table}>
                      {stat.table}
                    </span>
                    <span className="table-stat-value">
                      {stat.table === 'dwd_events' ? '≈ ' : ''}
                      {formatInt(stat.rows)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </section>
        </>
      ) : null}
    </div>
  )
}
