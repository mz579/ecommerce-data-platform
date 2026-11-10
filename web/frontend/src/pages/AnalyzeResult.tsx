import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useLocation, useNavigate, useParams } from 'react-router-dom'
import type { EChartsCoreOption } from 'echarts/core'
import { getAnalysisResult } from '../api/analysis'
import type {
  AnalysisCategoryRow,
  AnalysisItemRow,
  AnalysisResult,
} from '../api/analysisTypes'
import { ApiError } from '../api/client'
import { Chart } from '../components/Chart'
import { ChartCard } from '../components/ChartCard'
import { DataTable, type Column } from '../components/DataTable'
import { EmptyState } from '../components/EmptyState'
import { KpiCard } from '../components/KpiCard'
import { NoirReveal } from '../components/NoirReveal'
import { PageSkeleton } from '../components/Skeleton'
import { fontFamily, getChartTheme, rateLineColors } from '../theme/tokens'
import { useThemeMode } from '../theme/ThemeContext'
import { formatBytes, formatDuration } from '../utils/analysis'
import { prefersReducedMotion } from '../utils/transition'
import {
  formatCompact,
  formatDateTime,
  formatDecimal,
  formatInt,
  formatPct,
} from '../utils/format'

const EVENT_LABELS: Record<string, string> = {
  view: '浏览',
  add_to_cart: '加购',
  transaction: '购买',
  other: '其他',
}

function tooltipBase(theme: ReturnType<typeof getChartTheme>) {
  return {
    backgroundColor: theme.tooltipBg,
    borderColor: theme.tooltipBorder,
    borderWidth: 1,
    padding: [8, 10],
    textStyle: { color: theme.textPrimary, fontSize: 12, fontFamily },
    extraCssText: 'border-radius:0;box-shadow:none;',
  }
}

function axisLabel(theme: ReturnType<typeof getChartTheme>) {
  return { color: theme.textMuted, fontSize: 11, fontFamily }
}

function splitLine(theme: ReturnType<typeof getChartTheme>) {
  return { lineStyle: { color: theme.grid, width: 1 } }
}

export default function AnalyzeResultPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const location = useLocation()
  const navigate = useNavigate()
  const { mode } = useThemeMode()
  const theme = getChartTheme(mode)

  // 从任务页完成跳转过来时带 reveal 标记：在已渲染的结果页上播「案卷展开」，
  // 而不是先播开场动画再硬切页面。刷新或直接打开不重播。
  const [revealing, setRevealing] = useState(() => {
    if (!jobId) return false
    if ((location.state as { reveal?: boolean } | null)?.reveal) return true
    try {
      return sessionStorage.getItem(`analysis-reveal:${jobId}`) === '1'
    } catch {
      return false
    }
  })

  const finishReveal = useCallback(() => {
    setRevealing(false)
    try {
      sessionStorage.removeItem(`analysis-reveal:${jobId ?? ''}`)
    } catch {
      /* 隐私模式下 sessionStorage 可能不可用 */
    }
    // 动画结束后清掉路由状态，刷新时不再重播。
    if ((location.state as { reveal?: boolean } | null)?.reveal) {
      navigate(location.pathname, { replace: true, state: null })
    }
  }, [jobId, location.pathname, location.state, navigate])

  const [result, setResult] = useState<AnalysisResult | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<unknown>(null)

  useEffect(() => {
    if (!jobId) return
    const controller = new AbortController()
    setLoading(true)
    setError(null)
    getAnalysisResult(jobId, controller.signal)
      .then((data) => {
        if (!controller.signal.aborted) setResult(data)
      })
      .catch((caught: unknown) => {
        if (!controller.signal.aborted) setError(caught)
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [jobId])

  const trafficOption = useMemo<EChartsCoreOption>(() => {
    if (!result) return {}
    const points = result.traffic
    return {
      color: theme.categorical.slice(0, 3),
      animationDuration: 360,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          lineStyle: { color: theme.axis },
          crossStyle: { color: theme.axis },
        },
        valueFormatter: (value: unknown) => formatInt(Number(value)),
        ...tooltipBase(theme),
      },
      legend: {
        top: 0,
        right: 0,
        icon: 'rect',
        itemWidth: 8,
        itemHeight: 8,
        itemGap: 16,
        textStyle: { color: theme.textSecondary, fontSize: 11.5, fontFamily },
      },
      grid: { left: 4, right: 10, top: 38, bottom: 4, containLabel: true },
      xAxis: {
        type: 'category',
        data: points.map((point) => point.date),
        boundaryGap: false,
        axisLine: { lineStyle: { color: theme.axis } },
        axisTick: { show: false },
        axisLabel: {
          ...axisLabel(theme),
          hideOverlap: true,
          formatter: (value: string) => value.slice(5),
        },
      },
      yAxis: {
        type: 'value',
        min: 0,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { ...axisLabel(theme), formatter: (value: number) => formatCompact(value) },
        splitLine: splitLine(theme),
      },
      series: [
        {
          name: 'PV',
          type: 'line',
          data: points.map((point) => point.pv),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: 'UV',
          type: 'line',
          data: points.map((point) => point.uv),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: 'DAU',
          type: 'line',
          data: points.map((point) => point.dau),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
      ],
    }
  }, [result, theme])

  const ratesOption = useMemo<EChartsCoreOption>(() => {
    if (!result) return {}
    const points = result.traffic
    return {
      // 与总览一致：加购率绯红，严格 / 全局转化率蓝绿。
      color: [theme.categorical[0], ...rateLineColors[theme.mode]],
      animationDuration: 360,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          lineStyle: { color: theme.axis },
          crossStyle: { color: theme.axis },
        },
        valueFormatter: (value: unknown) => formatPct(Number(value)),
        ...tooltipBase(theme),
      },
      legend: {
        top: 0,
        right: 0,
        icon: 'rect',
        itemWidth: 8,
        itemHeight: 8,
        itemGap: 16,
        textStyle: { color: theme.textSecondary, fontSize: 11.5, fontFamily },
      },
      grid: { left: 4, right: 10, top: 38, bottom: 4, containLabel: true },
      xAxis: {
        type: 'category',
        data: points.map((point) => point.date),
        boundaryGap: false,
        axisLine: { lineStyle: { color: theme.axis } },
        axisTick: { show: false },
        axisLabel: {
          ...axisLabel(theme),
          hideOverlap: true,
          formatter: (value: string) => value.slice(5),
        },
      },
      yAxis: {
        type: 'value',
        min: 0,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { ...axisLabel(theme), formatter: (value: number) => formatPct(value) },
        splitLine: splitLine(theme),
      },
      series: [
        {
          name: '加购率',
          type: 'line',
          data: points.map((point) => point.add_to_cart_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: '严格转化率',
          type: 'line',
          data: points.map((point) => point.conv_strict_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: '全局转化率',
          type: 'line',
          data: points.map((point) => point.conv_global_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
      ],
    }
  }, [result, theme])

  const funnelOption = useMemo<EChartsCoreOption>(() => {
    if (!result) return {}
    const stages = result.funnel
    const maxUsers = Math.max(...stages.map((stage) => stage.users), 1)
    return {
      animationDuration: 360,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        ...tooltipBase(theme),
        formatter: (params: any) => {
          const item = Array.isArray(params) ? params[0] : params
          const stage = stages[item?.dataIndex ?? 0]
          if (!stage) return ''
          const share =
            stage.stage === 'view'
              ? ''
              : `<br/>占浏览用户 ${formatPct(stage.rate ?? null)}`
          return `${stage.label}<br/>用户 ${formatInt(stage.users)}${share}`
        },
      },
      grid: { left: 4, right: 84, top: 12, bottom: 4, containLabel: true },
      xAxis: { type: 'value', max: maxUsers, show: false },
      yAxis: {
        type: 'category',
        inverse: true,
        data: stages.map((stage) => `${stage.label}用户`),
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: theme.textSecondary, fontSize: 12, fontFamily },
      },
      series: [
        {
          type: 'bar',
          barWidth: 26,
          data: stages.map((stage, index) => ({
            value: stage.users,
            itemStyle: { color: theme.categorical[index % theme.categorical.length] },
          })),
          label: {
            show: true,
            position: 'right',
            color: theme.textPrimary,
            fontSize: 12,
            fontFamily,
            formatter: (params: any) => formatInt(Number(params.value ?? 0)),
          },
        },
      ],
    }
  }, [result, theme])

  const d1Option = useMemo<EChartsCoreOption>(() => {
    if (!result) return {}
    const series = result.d1_series
    const partialPoints = series.flatMap((point, index) => {
      if (point.window_status !== 'partial' || point.d1_rate === null) return []
      return [
        {
          value: [point.date, point.d1_rate],
          isLast: index === series.length - 1,
        },
      ]
    })
    return {
      color: [theme.categorical[0]],
      animationDuration: 360,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          lineStyle: { color: theme.axis },
          crossStyle: { color: theme.axis },
        },
        ...tooltipBase(theme),
        formatter: (params: any) => {
          const index = params?.[0]?.dataIndex ?? 0
          const point = series[index]
          if (!point) return ''
          const partial = point.window_status === 'partial'
          return `${point.date}<br/>D1 留存 ${formatPct(point.d1_rate)}<br/>cohort ${formatInt(
            point.cohort_size,
          )} · 次日回访 ${formatInt(point.retained_users)}${
            partial ? '<br/>窗口不完整，不参与加权' : ''
          }`
        },
      },
      grid: { left: 4, right: 26, top: 16, bottom: 4, containLabel: true },
      xAxis: {
        type: 'category',
        data: series.map((point) => point.date),
        boundaryGap: false,
        axisLine: { lineStyle: { color: theme.axis } },
        axisTick: { show: false },
        axisLabel: {
          ...axisLabel(theme),
          hideOverlap: true,
          formatter: (value: string) => value.slice(5),
        },
      },
      yAxis: {
        type: 'value',
        min: 0,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { ...axisLabel(theme), formatter: (value: number) => formatPct(value) },
        splitLine: splitLine(theme),
      },
      series: [
        {
          name: 'D1 留存率',
          type: 'line',
          data: series.map((point) => point.d1_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: 'partial',
          type: 'scatter',
          data: partialPoints.map((point) => ({
            value: point.value,
            label: {
              show: true,
              position: point.isLast ? 'right' : 'top',
              formatter: '*',
              color: theme.textSecondary,
              fontSize: 12,
              fontWeight: 600,
              fontFamily,
            },
          })),
          symbolSize: 4,
          itemStyle: { color: theme.categorical[1] },
          z: 3,
          tooltip: { show: false },
        },
      ],
    }
  }, [result, theme])

  const handleDownload = () => {
    if (!result) return
    const blob = new Blob([JSON.stringify(result, null, 2)], {
      type: 'application/json;charset=utf-8',
    })
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = `${result.filename.replace(/\.[^.]+$/, '')}-analysis.json`
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    URL.revokeObjectURL(url)
  }

  if (!jobId) {
    return (
      <div className="page-stack">
        <div className="card card-pad">
          <p>缺少任务 ID。</p>
          <Link className="link-button" to="/analyze">
            返回分析首页
          </Link>
        </div>
      </div>
    )
  }

  const animateContent = revealing && !prefersReducedMotion()
  const revealOverlay =
    revealing && jobId ? (
      <NoirReveal jobId={jobId} ready={!loading} onDone={finishReveal} />
    ) : null

  if (loading) {
    return (
      <div className={`page-stack analysis-result${animateContent ? ' is-revealing' : ''}`}>
        <PageSkeleton />
        {revealOverlay}
      </div>
    )
  }

  if (error || !result) {
    const status = error instanceof ApiError ? error.status : undefined
    const notReady = status === 409
    const message =
      error instanceof Error
        ? error.message
        : notReady
          ? '分析尚未完成，请稍后再打开结果。'
          : '结果读取失败'
    return (
      <div className="page-stack">
        <section className="card card-pad analysis-result-error">
          <p className="eyebrow">RESULT UNAVAILABLE</p>
          <h1 className="analysis-job-title">
            {notReady ? '结果还没有生成' : '结果读取失败'}
          </h1>
          <p>{message}</p>
          <div className="analysis-actions">
            <Link className="btn btn-primary" to={`/analyze/${jobId}`}>
              返回任务
            </Link>
            <Link className="btn btn-ghost" to="/analyze">
              分析首页
            </Link>
          </div>
        </section>
      </div>
    )
  }

  const { totals, quality, provenance } = result
  const hasTraffic = result.traffic.length > 0
  const hasRates = result.traffic.some(
    (point) => point.add_to_cart_rate !== null || point.conv_strict_rate !== null,
  )
  const hasD1 = result.d1_series.some((point) => point.d1_rate !== null)
  const hasCategories = result.has_category_id && result.categories.length > 0
  const revenueLabel = result.has_amount ? formatDecimal(totals.revenue, 2) : null
  const sampleBadge = provenance.sample ? (
    <span className="analysis-status analysis-status--done">示例数据</span>
  ) : null

  const itemColumns: Column<AnalysisItemRow>[] = [
    {
      key: 'item',
      title: '商品',
      width: '40%',
      render: (row) => (
        <div className="product-cell">
          {row.product_name ? <span className="product-cell__name">{row.product_name}</span> : null}
          <span className="mono product-cell__id">{row.item_id}</span>
        </div>
      ),
    },
    {
      key: 'views',
      title: '浏览',
      align: 'right',
      render: (row) => formatInt(row.views),
    },
    {
      key: 'carts',
      title: '加购',
      align: 'right',
      render: (row) => formatInt(row.carts),
    },
    {
      key: 'purchases',
      title: '成交',
      align: 'right',
      render: (row) => formatInt(row.purchases),
    },
    {
      key: 'buyers',
      title: '买家',
      align: 'right',
      render: (row) => formatInt(row.buyers),
    },
  ]
  if (result.has_amount) {
    itemColumns.push({
      key: 'revenue',
      title: '成交金额',
      align: 'right',
      render: (row) => formatDecimal(row.revenue, 2),
    })
  }

  const categoryColumns: Column<AnalysisCategoryRow>[] = [
    {
      key: 'category_id',
      title: '类目 ID',
      render: (row) => <span className="mono">{row.category_id}</span>,
    },
    {
      key: 'views',
      title: '浏览',
      align: 'right',
      render: (row) => formatInt(row.views),
    },
    {
      key: 'carts',
      title: '加购',
      align: 'right',
      render: (row) => formatInt(row.carts),
    },
    {
      key: 'purchases',
      title: '成交',
      align: 'right',
      render: (row) => formatInt(row.purchases),
    },
    {
      key: 'buyers',
      title: '买家',
      align: 'right',
      render: (row) => formatInt(row.buyers),
    },
  ]
  if (result.has_amount) {
    categoryColumns.push({
      key: 'revenue',
      title: '成交金额',
      align: 'right',
      render: (row) => formatDecimal(row.revenue, 2),
    })
  }

  const rejected = Object.entries(quality.rejected_reasons ?? {})
  const eventTypes = ['view', 'add_to_cart', 'transaction', 'other'].map((type) => ({
    type,
    label: EVENT_LABELS[type] ?? type,
    count: quality.event_types[type] ?? 0,
  }))

  return (
    <div className={`page-stack analysis-result${animateContent ? ' is-revealing' : ''}`}>
      <header className="analysis-report-head">
        <div className="analysis-report-head__main">
          <p className="eyebrow">
            ANALYSIS REPORT · CASE {result.job_id.slice(0, 8).toUpperCase()}
          </p>
          <h1 className="analysis-report-title">分析结果</h1>
          <p className="analysis-report-meta">
            {result.filename} · {result.range.date_min ?? '—'} 至{' '}
            {result.range.date_max ?? '—'} · {formatInt(result.range.days)} 天 ·{' '}
            {formatInt(totals.events)} 个事件
          </p>
          <div className="analysis-report-tags">
            {sampleBadge}
            <span className="analysis-tag">{provenance.file_type.toUpperCase()}</span>
            <span className="analysis-tag">{formatBytes(provenance.size_bytes)}</span>
            <span className="analysis-tag">时区 {provenance.timezone}</span>
            <span className="analysis-tag">
              生成于 {formatDateTime(result.generated_at)}
            </span>
          </div>
        </div>
        <div className="analysis-report-head__actions">
          <button type="button" className="btn btn-primary" onClick={handleDownload}>
            下载 JSON
          </button>
          <Link className="btn btn-ghost" to="/model">
            数据模型
          </Link>
          <Link className="btn btn-ghost" to="/analyze">
            再分析一份
          </Link>
        </div>
      </header>

      <section className="analysis-callout" aria-label="商品标识说明">
        <p className="eyebrow">IDENTIFIER NOTE</p>
        <p>
          <strong>商品 ID 与类目 ID 是上传数据包中的原始值，不是界面生成的代号。</strong>{' '}
          {result.has_product_name
            ? '商品名称取自数据里的商品名列，未做改写；同一商品 ID 展示其任一非空名称。'
            : '这份数据没有可用的商品名称列，因此商品只展示原始 ID。'}
        </p>
      </section>

      <div className="kpi-grid">
        <KpiCard label="PV · 浏览事件" value={formatInt(totals.pv)} hint="view 事件行数" />
        <KpiCard
          label="UV · 区间去重访客"
          value={formatInt(totals.uv)}
          hint="整个区间内去重的浏览访客"
        />
        <KpiCard
          label="日均 DAU"
          value={formatInt(totals.dau_avg)}
          hint="逐日活跃用户数的算术平均"
        />
        <KpiCard
          label="订单数"
          value={formatInt(totals.orders)}
          hint={
            quality.orders_basis === 'transaction_id'
              ? '按 transaction_id 去重'
              : '无订单号，按 purchase 事件计数'
          }
        />
        <KpiCard
          label="加购率"
          value={formatPct(totals.add_to_cart_rate)}
          hint="加购用户 / 浏览用户（区间去重）"
        />
        <KpiCard
          label="严格转化率"
          value={formatPct(totals.conv_strict_rate)}
          hint="购买且浏览用户 / 浏览用户"
        />
        <KpiCard
          label="D1 留存（加权）"
          value={formatPct(totals.d1_retention)}
          hint="Σ 次日留存 / Σ 完整 D1 窗口规模"
        />
        {revenueLabel !== null ? (
          <KpiCard label="成交金额" value={revenueLabel} hint="transaction 事件 amount 求和" />
        ) : (
          <KpiCard
            label="进入分析库事件"
            value={formatInt(quality.rows_kept)}
            hint="去重后保留的事件行数"
          />
        )}
      </div>

      <div className="grid-2">
        <ChartCard
          title="PV / UV / DAU 趋势"
          subtitle="本次上传数据的日粒度走势；PV 为事件行数，UV / DAU 为去重访客"
          footer="首末日可能为部分日；时间按确认的时区解析为 UTC 日期。"
        >
          {hasTraffic ? (
            <Chart option={trafficOption} height={300} ariaLabel="PV、UV、DAU 日趋势折线图" />
          ) : (
            <EmptyState />
          )}
        </ChartCard>

        <ChartCard
          title="用户漏斗"
          subtitle="区间内去重用户：浏览 → 加购 → 购买"
          footer="漏斗为去重用户口径，不按事件次数；严格转化率只统计既浏览又购买的用户。"
        >
          {result.funnel.length > 0 ? (
            <Chart option={funnelOption} height={300} ariaLabel="浏览、加购、购买用户漏斗条形图" />
          ) : (
            <EmptyState />
          )}
        </ChartCard>
      </div>

      <div className="grid-2">
        <ChartCard
          title="转化率趋势"
          subtitle="加购率 / 严格转化率 / 全局转化率"
          footer="比率轴自 0 开始；缺口表示该日浏览用户为 0。"
        >
          {hasRates ? (
            <Chart
              option={ratesOption}
              height={300}
              ariaLabel="加购率、严格转化率、全局转化率趋势折线图"
            />
          ) : (
            <EmptyState />
          )}
        </ChartCard>

        <ChartCard
          title="D1 留存（逐日 cohort）"
          subtitle="次日回访用户 / 当日活跃用户；* 表示窗口不完整"
          footer="末日 cohort 的 D1 窗口不完整，不参与加权；曲线末端下降不代表真实留存恶化。"
        >
          {hasD1 ? (
            <Chart option={d1Option} height={300} ariaLabel="D1 留存率趋势折线图" />
          ) : (
            <EmptyState text="数据不足一天，无法计算 D1 留存" />
          )}
        </ChartCard>
      </div>

      <div className={hasCategories ? 'grid-2' : undefined}>
        <ChartCard
          title="Top 商品"
          subtitle="按成交事件降序 · Top 20"
          footer={
            result.has_product_name
              ? '商品名称取自数据中的商品名列；ID 为原始值。'
              : '数据没有商品名称列，商品只展示上传数据中的原始 ID。'
          }
        >
          <DataTable
            columns={itemColumns}
            rows={result.top_items}
            rowKey={(row) => row.item_id}
            emptyText="没有可用的商品数据"
            maxHeight={380}
            caption="Top 商品表"
          />
        </ChartCard>

        {hasCategories ? (
          <ChartCard
            title="Top 类目"
            subtitle="按成交事件降序 · Top 15"
            footer="类目 ID 为上传数据中的原始值，未映射为类目名称。"
          >
            <DataTable
              columns={categoryColumns}
              rows={result.categories}
              rowKey={(row) => row.category_id}
              emptyText="没有可用的类目数据"
              maxHeight={380}
              caption="Top 类目表"
            />
          </ChartCard>
        ) : null}
      </div>

      <div className="grid-2">
        <section className="card analysis-panel">
          <div className="analysis-panel__head">
            <div>
              <p className="eyebrow">DATA QUALITY</p>
              <h2 className="analysis-panel__title">数据质量</h2>
            </div>
            <span className="analysis-panel__step">
              保留率 {formatPct(
                quality.rows_read > 0 ? quality.rows_kept / quality.rows_read : null,
              )}
            </span>
          </div>

          <div className="quality-hero">
            <strong>{formatInt(quality.rows_kept)}</strong>
            <span>行进入分析库</span>
            <small>
              读取 {formatInt(quality.rows_read)} · 拒绝 {formatInt(quality.rows_rejected)} · 去重{' '}
              {formatInt(quality.duplicates)}
            </small>
          </div>

          <div className="event-composition">
            {eventTypes.map((item) => (
              <div className={`event-count event-count--${item.type}`} key={item.type}>
                <span>{item.label}</span>
                <strong>{formatInt(item.count)}</strong>
              </div>
            ))}
          </div>

          <dl className="stat-list">
            <div className="stat-row">
              <span>日期范围</span>
              <strong>
                {quality.date_min ?? '—'} → {quality.date_max ?? '—'}
              </strong>
            </div>
            <div className="stat-row">
              <span>覆盖天数</span>
              <strong>{formatInt(quality.days)}</strong>
            </div>
            <div className="stat-row">
              <span>去重用户 / 商品</span>
              <strong>
                {formatInt(quality.distinct_users)} / {formatInt(quality.distinct_items)}
              </strong>
            </div>
            <div className="stat-row">
              <span>无法识别的事件值</span>
              <strong>{formatInt(quality.unknown_events)}</strong>
            </div>
            <div className="stat-row">
              <span>保留为 other 的事件</span>
              <strong>{formatInt(quality.other_events_kept)}</strong>
            </div>
          </dl>

          {rejected.length > 0 ? (
            <div className="quality-rejections">
              <p className="event-values__label">拒收原因</p>
              {rejected.map(([reason, count]) => (
                <div className="stat-row" key={reason}>
                  <span>{reason}</span>
                  <strong>{formatInt(count)}</strong>
                </div>
              ))}
            </div>
          ) : null}
        </section>

        <section className="card analysis-panel">
          <div className="analysis-panel__head">
            <div>
              <p className="eyebrow">PROVENANCE</p>
              <h2 className="analysis-panel__title">数据来源</h2>
            </div>
            {provenance.sample ? (
              <span className="analysis-tag">示例</span>
            ) : (
              <span className="analysis-tag">本机沙箱</span>
            )}
          </div>

          <dl className="file-facts file-facts--dense">
            <div>
              <dt>文件</dt>
              <dd className="file-facts__wide">{provenance.filename}</dd>
            </div>
            <div>
              <dt>类型 / 大小</dt>
              <dd>
                {provenance.file_type.toUpperCase()} · {formatBytes(provenance.size_bytes)}
              </dd>
            </div>
            <div>
              <dt>编码</dt>
              <dd>{provenance.encoding ? provenance.encoding.toUpperCase() : '—'}</dd>
            </div>
            <div>
              <dt>时区</dt>
              <dd>{provenance.timezone}</dd>
            </div>
            <div>
              <dt>原始行数</dt>
              <dd>{formatInt(provenance.rows_estimated)}</dd>
            </div>
            <div>
              <dt>保留行数</dt>
              <dd>{formatInt(provenance.rows_kept)}</dd>
            </div>
            <div>
              <dt>解析耗时</dt>
              <dd>{formatDuration(provenance.parse_seconds)}</dd>
            </div>
            <div>
              <dt>总耗时</dt>
              <dd>{formatDuration(provenance.elapsed_seconds)}</dd>
            </div>
          </dl>

          <div className="field-availability">
            <p className="event-values__label">可用字段</p>
            <div className="field-availability__chips">
              <span className={quality.has_transaction_id ? 'field-chip is-on' : 'field-chip'}>
                订单号 {quality.has_transaction_id ? '✓' : '—'}
              </span>
              <span className={quality.has_category_id ? 'field-chip is-on' : 'field-chip'}>
                类目 {quality.has_category_id ? '✓' : '—'}
              </span>
              <span className={quality.has_amount ? 'field-chip is-on' : 'field-chip'}>
                金额 {quality.has_amount ? '✓' : '—'}
              </span>
              <span className={quality.has_product_name ? 'field-chip is-on' : 'field-chip'}>
                商品名 {quality.has_product_name ? '✓' : '—'}
              </span>
            </div>
            <p className="analysis-side__note">
              订单基准：
              {quality.orders_basis === 'transaction_id'
                ? 'transaction_id 去重'
                : '无订单号，按 purchase 事件计数'}
              。字段缺失时对应指标自动降级，不会填报占位值。
            </p>
          </div>

          <p className="analysis-side__note">
            原始文件、规范化事件与分析库都只写在本机任务目录，7 天后自动清理；本次分析没有写入
            MySQL 四层数仓。
          </p>
        </section>
      </div>

      <footer className="analysis-report-foot">
        <span>
          结果快照生成于 {formatDateTime(result.generated_at)} · 任务{' '}
          <span className="mono">{result.job_id.slice(0, 8)}</span>
        </span>
        <Link className="link-button" to={`/analyze/${result.job_id}`}>
          查看任务详情
        </Link>
      </footer>
      {revealOverlay}
    </div>
  )
}
