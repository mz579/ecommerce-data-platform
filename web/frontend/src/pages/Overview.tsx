import { useMemo } from 'react'
import type { EChartsCoreOption } from 'echarts/core'
import { apiUrl } from '../api/client'
import { useApi } from '../api/useApi'
import type { FunnelStage, OverviewResponse, TopItem } from '../api/types'
import { Chart } from '../components/Chart'
import { ChartCard } from '../components/ChartCard'
import { DataTable, type Column } from '../components/DataTable'
import { EmptyState } from '../components/EmptyState'
import { ErrorState } from '../components/ErrorState'
import { KpiCard } from '../components/KpiCard'
import { ChartSkeleton, KpiSkeleton } from '../components/Skeleton'
import { useDateRange } from '../state/DateRangeContext'
import { fontFamily, getChartTheme, rateLineColors } from '../theme/tokens'
import { useThemeMode } from '../theme/ThemeContext'
import {
  formatCompact,
  formatInt,
  formatPct,
} from '../utils/format'

// 图内标签用短名避免窄漏斗溢出；完整口径（用户人次、日累加）见卡片副标题与脚注。
const STAGE_LABELS: Record<string, string> = {
  view: '浏览人次',
  cart: '加购人次',
  buy: '成交人次',
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

export default function OverviewPage() {
  const { from, to } = useDateRange()
  const { mode } = useThemeMode()
  const theme = getChartTheme(mode)
  const url = useMemo(
    () => apiUrl('/overview', { date_from: from, date_to: to }),
    [from, to],
  )
  const { data, loading, error, reload } = useApi<OverviewResponse>(url)

  const trafficOption = useMemo<EChartsCoreOption>(() => {
    if (!data) return {}
    const dates = data.traffic.map((point) => point.date)
    return {
      color: theme.categorical.slice(0, 3),
      animationDuration: 320,
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
        icon: 'circle',
        itemWidth: 8,
        itemHeight: 8,
        itemGap: 16,
        textStyle: { color: theme.textSecondary, fontSize: 11.5, fontFamily },
      },
      grid: { left: 4, right: 10, top: 38, bottom: 4, containLabel: true },
      xAxis: {
        type: 'category',
        data: dates,
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
          data: data.traffic.map((point) => point.pv),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: 'UV',
          type: 'line',
          data: data.traffic.map((point) => point.uv),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: 'DAU',
          type: 'line',
          data: data.traffic.map((point) => point.dau),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
      ],
    }
  }, [data, theme])

  const ratesOption = useMemo<EChartsCoreOption>(() => {
    if (!data) return {}
    const dates = data.rates.map((point) => point.date)
    return {
      // 加购率沿用主题绯红；严格 / 全局转化率用蓝绿，便于在墨黑底上区分。
      color: [theme.categorical[0], ...rateLineColors[theme.mode]],
      animationDuration: 320,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross', lineStyle: { color: theme.axis }, crossStyle: { color: theme.axis } },
        valueFormatter: (value: unknown) => formatPct(Number(value)),
        ...tooltipBase(theme),
      },
      legend: {
        top: 0,
        right: 0,
        icon: 'circle',
        itemWidth: 8,
        itemHeight: 8,
        itemGap: 16,
        textStyle: { color: theme.textSecondary, fontSize: 11.5, fontFamily },
      },
      grid: { left: 4, right: 10, top: 38, bottom: 4, containLabel: true },
      xAxis: {
        type: 'category',
        data: dates,
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
          data: data.rates.map((point) => point.add_to_cart_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: '严格转化率',
          type: 'line',
          data: data.rates.map((point) => point.conv_strict_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
        {
          name: '全局转化率',
          type: 'line',
          data: data.rates.map((point) => point.conv_global_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
        },
      ],
    }
  }, [data, theme])

  const funnelOption = useMemo<EChartsCoreOption>(() => {
    if (!data) return {}
    const total = data.funnel.find((stage) => stage.stage === 'view')?.users ?? 0
    return {
      animationDuration: 320,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'item',
        ...tooltipBase(theme),
        formatter: (params: any) => {
          const value = Number(params.value ?? 0)
          const share = total > 0 ? formatPct(value / total) : '—'
          return `${params.name}<br/>${formatInt(value)}（日累加）<br/>占浏览用户人次 ${share}`
        },
      },
      series: [
        {
          type: 'funnel',
          left: '4%',
          right: '4%',
          top: 12,
          bottom: 8,
          minSize: '18%',
          maxSize: '100%',
          sort: 'descending',
          gap: 2,
          label: {
            show: true,
            position: 'inside',
            color: '#0b0b0b',
            fontSize: 11.5,
            fontWeight: 600,
            fontFamily,
            lineHeight: 16,
            formatter: (params: any) =>
              `${params.name}\n${formatInt(Number(params.value ?? 0))}`,
          },
          labelLine: { show: false },
          itemStyle: { borderColor: theme.surface, borderWidth: 2 },
          emphasis: { label: { fontWeight: 700 } },
          data: data.funnel.map((stage: FunnelStage, index: number) => ({
            name: STAGE_LABELS[stage.stage] ?? stage.stage,
            value: stage.users,
            itemStyle: { color: theme.categorical[index % theme.categorical.length] },
            label: { color: index === 0 ? '#f4f1eb' : '#08080a' },
          })),
        },
      ],
    }
  }, [data, theme])

  const topItemColumns: Column<TopItem>[] = [
    {
      key: 'item_id',
      title: '商品 ID',
      render: (row) => <span className="mono">{row.item_id}</span>,
      width: '38%',
    },
    {
      key: 'vcnt',
      title: '浏览',
      align: 'right',
      render: (row) => formatInt(row.vcnt),
    },
    {
      key: 'acnt',
      title: '加购',
      align: 'right',
      render: (row) => formatInt(row.acnt),
    },
    {
      key: 'tcnt',
      title: '成交',
      align: 'right',
      render: (row) => formatInt(row.tcnt),
    },
  ]

  if (loading) {
    return (
      <div className="page-stack">
        <div className="kpi-grid">
          {Array.from({ length: 8 }, (_, index) => (
            <KpiSkeleton key={index} />
          ))}
        </div>
        <div className="grid-2">
          <ChartSkeleton />
          <ChartSkeleton />
        </div>
        <div className="grid-2">
          <ChartSkeleton />
          <ChartSkeleton />
        </div>
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

  const kpis = data.kpis
  const hasTraffic = data.traffic.length > 0
  const hasRates = data.rates.length > 0
  const hasFunnel = data.funnel.length > 0

  return (
    <div className="page-stack">
      <section className="analysis-callout">
        <p className="eyebrow">DATA SOURCE</p>
        <p>
          <strong>本页不是内置演示数据。</strong>{' '}
          数字来自 Retailrocket 事件数据集经 ODS → DWD → DWS → ADS 四层数仓后的日聚合：
          2015-05-03 ~ 2015-09-18，共 139 天、2,756,101 条原始事件。点顶栏「来源」可查看数据集、许可证、校验与商品编号说明。
        </p>
      </section>

      <div className="kpi-grid">
        <KpiCard label="PV · 页面浏览量" value={formatInt(kpis.pv)} hint="区间内浏览事件行数" />
        <KpiCard
          label="日均 UV"
          value={formatInt(kpis.uv_daily_avg)}
          hint="逐日去重浏览用户数的算术平均（非区间去重）"
        />
        <KpiCard
          label="日均 DAU"
          value={formatInt(kpis.dau_daily_avg)}
          hint="逐日活跃用户数的算术平均（非区间去重）"
        />
        <KpiCard
          label="订单数"
          value={formatInt(kpis.orders)}
          hint="逐日 distinct(transaction_id) 之和（跨日可能重复）"
        />
        <KpiCard
          label="加购率"
          value={formatPct(kpis.add_to_cart_rate)}
          hint="加购用户人次 / 浏览用户人次（加权）"
        />
        <KpiCard
          label="严格转化率"
          value={formatPct(kpis.conv_strict_rate)}
          hint="成交且浏览用户人次 / 浏览用户人次"
        />
        <KpiCard
          label="全局转化率"
          value={formatPct(kpis.conv_global_rate)}
          hint="全部成交用户人次 / 浏览用户人次"
        />
        <KpiCard
          label="D1 留存（加权）"
          value={formatPct(kpis.d1_retention)}
          hint="Σ 次日留存用户 / Σ D1 完整窗口规模（加权分母）"
        />
      </div>

      <div className="grid-2">
        <ChartCard
          title="PV / UV / DAU 趋势"
          subtitle="同量纲单轴展示；PV 为事件行数，UV / DAU 为去重访客"
          footer="日粒度（UTC）；首末日为部分日，数值系统性偏小。"
        >
          {hasTraffic ? (
            <Chart option={trafficOption} height={300} ariaLabel="PV、UV、DAU 日趋势折线图" />
          ) : (
            <EmptyState />
          )}
        </ChartCard>

        <ChartCard
          title="用户人次漏斗"
          subtitle="区间内逐日累加人次（SUM）· 非区间去重 UV"
          footer={
            hasFunnel
              ? (() => {
                  const stageValue = (stage: string) =>
                    data.funnel.find((item) => item.stage === stage)?.users ?? null
                  return `口径：浏览用户人次（日累加）${formatInt(
                    stageValue('view'),
                  )} · 加购用户人次 ${formatInt(stageValue('cart'))} · 成交用户人次 ${formatInt(
                    stageValue('buy'),
                  )}，均为逐日去重用户之和，不是区间去重 UV。加购率 ${formatPct(
                    kpis.add_to_cart_rate,
                  )} · 严格转化率 ${formatPct(
                    kpis.conv_strict_rate,
                  )}；漏斗宽度为示意（最小 18%），数值以标签为准。`
                })()
              : undefined
          }
        >
          {hasFunnel ? (
            <Chart option={funnelOption} height={300} ariaLabel="浏览、加购、成交用户人次漏斗图" />
          ) : (
            <EmptyState />
          )}
        </ChartCard>
      </div>

      <div className="grid-2">
        <ChartCard
          title="转化率趋势"
          subtitle="加购率 / 严格转化率 / 全局转化率"
          footer="比率轴自 0 开始；缺口表示该日分母为 0。"
        >
          {hasRates ? (
            <Chart option={ratesOption} height={300} ariaLabel="加购率、严格转化率、全局转化率趋势折线图" />
          ) : (
            <EmptyState />
          )}
        </ChartCard>

        <ChartCard
          title="Top 商品"
          subtitle="区间聚合 · 按成交降序 · Top 10"
          footer="共 10 条，滚动查看全部。"
        >
          <DataTable
            columns={topItemColumns}
            rows={data.top_items}
            rowKey={(row) => row.item_id}
            emptyText="该区间暂无商品数据"
            maxHeight={300}
            caption="Top 商品表"
          />
        </ChartCard>
      </div>
    </div>
  )
}
