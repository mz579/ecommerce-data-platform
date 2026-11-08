import { useMemo, useState } from 'react'
import type { EChartsCoreOption } from 'echarts/core'
import { apiUrl } from '../api/client'
import { useApi } from '../api/useApi'
import type { CategoryRow, ItemBehaviorPoint, ItemRow, ItemsResponse } from '../api/types'
import { Chart } from '../components/Chart'
import { ChartCard } from '../components/ChartCard'
import { EmptyState } from '../components/EmptyState'
import { ErrorState } from '../components/ErrorState'
import { ChartSkeleton } from '../components/Skeleton'
import { useDateRange } from '../state/DateRangeContext'
import { fontFamily, getChartTheme } from '../theme/tokens'
import { useThemeMode } from '../theme/ThemeContext'
import { formatCompact, formatInt } from '../utils/format'

type RankMetric = 'transaction_cnt' | 'view_cnt'

function categoryLabel(categoryId: number): string {
  return categoryId === 0 ? '未分类（0）' : String(categoryId)
}

function tooltipBase(theme: ReturnType<typeof getChartTheme>) {
  return {
    backgroundColor: theme.tooltipBg,
    borderColor: theme.tooltipBorder,
    borderWidth: 1,
    padding: [8, 10],
    textStyle: { color: theme.textPrimary, fontSize: 12, fontFamily },
    extraCssText: 'border-radius:8px;box-shadow:none;',
  }
}

const SPARK_METRICS: Array<{
  key: keyof Omit<ItemBehaviorPoint, 'date'>
  label: string
  colorIndex: number
}> = [
  { key: 'items_viewed', label: '浏览商品数', colorIndex: 0 },
  { key: 'items_carted', label: '加购商品数', colorIndex: 1 },
  { key: 'items_bought', label: '成交商品数', colorIndex: 2 },
]

export default function ItemsPage() {
  const { from, to } = useDateRange()
  const { mode } = useThemeMode()
  const theme = getChartTheme(mode)
  const [metric, setMetric] = useState<RankMetric>('transaction_cnt')
  const url = useMemo(
    () => apiUrl('/items', { date_from: from, date_to: to, limit: 20 }),
    [from, to],
  )
  const { data, loading, error, reload } = useApi<ItemsResponse>(url)

  const rankedItems = useMemo<ItemRow[]>(() => {
    if (!data) return []
    return [...data.items]
      .sort((a, b) => b[metric] - a[metric] || b.transaction_cnt - a.transaction_cnt)
      .slice(0, 20)
  }, [data, metric])

  const categoryRows = useMemo<CategoryRow[]>(() => {
    if (!data) return []
    // category_id=0 是「未分类」桶，量级远超真实类目；
    // 从排名柱图剔除，避免压扁其余类目，改由卡片内独立统计块展示。
    return [...data.categories]
      .filter((row) => row.category_id !== 0)
      .sort((a, b) => b.view_cnt - a.view_cnt)
      .slice(0, 15)
  }, [data])

  const unclassified = useMemo<CategoryRow | null>(
    () => data?.categories.find((row) => row.category_id === 0) ?? null,
    [data],
  )

  const topItemsOption = useMemo<EChartsCoreOption>(() => {
    const chartRows = [...rankedItems].reverse()
    return {
      animationDuration: 320,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        ...tooltipBase(theme),
        formatter: (params: any) => {
          const row = chartRows[params?.[0]?.dataIndex ?? 0]
          if (!row) return ''
          return `商品 ${row.item_id}<br/>成交 ${formatInt(row.transaction_cnt)}<br/>浏览 ${formatInt(
            row.view_cnt,
          )}<br/>加购 ${formatInt(row.add_to_cart_cnt)}`
        },
      },
      grid: { left: 4, right: 44, top: 2, bottom: 2, containLabel: true },
      xAxis: {
        type: 'value',
        min: 0,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: {
          color: theme.textMuted,
          fontSize: 10.5,
          fontFamily,
          formatter: (value: number) => formatCompact(value),
        },
        splitLine: { lineStyle: { color: theme.grid } },
      },
      yAxis: {
        type: 'category',
        data: chartRows.map((row) => String(row.item_id)),
        axisLine: { lineStyle: { color: theme.axis } },
        axisTick: { show: false },
        axisLabel: { color: theme.textMuted, fontSize: 10.5, fontFamily },
      },
      series: [
        {
          name: metric === 'transaction_cnt' ? '成交件数' : '曝光次数',
          type: 'bar',
          barCategoryGap: '52%',
          itemStyle: { color: theme.categorical[0], borderRadius: [0, 4, 4, 0] },
          label: { show: false },
          data: chartRows.map((row, index) => ({
            value: row[metric],
            label: {
              show: index === chartRows.length - 1,
              position: 'right',
              color: theme.textSecondary,
              fontSize: 10.5,
              fontFamily,
              formatter: formatInt(row[metric]),
            },
          })),
        },
      ],
    }
  }, [rankedItems, metric, theme])

  const categoryOption = useMemo<EChartsCoreOption>(() => {
    // 横向柱状：y 轴自下而上绘制，反转为浏览量降序（最大在顶部）。
    const chartRows = [...categoryRows].reverse()
    return {
      animationDuration: 320,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        ...tooltipBase(theme),
        formatter: (params: any) => {
          const row = chartRows[params?.[0]?.dataIndex ?? 0]
          if (!row) return ''
          return `类目 ${categoryLabel(
            row.category_id,
          )}<br/>浏览 ${formatInt(row.view_cnt)}<br/>订单 ${formatInt(
            row.order_cnt,
          )}<br/>浏览用户 ${formatInt(row.view_uv)}`
        },
      },
      grid: { left: 4, right: 64, top: 2, bottom: 2, containLabel: true },
      xAxis: {
        type: 'value',
        min: 0,
        splitNumber: 4,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: {
          color: theme.textMuted,
          fontSize: 10.5,
          fontFamily,
          hideOverlap: true,
          formatter: (value: number) => formatCompact(value),
        },
        splitLine: { lineStyle: { color: theme.grid } },
      },
      yAxis: {
        type: 'category',
        data: chartRows.map((row) => categoryLabel(row.category_id)),
        axisLine: { lineStyle: { color: theme.axis } },
        axisTick: { show: false },
        axisLabel: { color: theme.textMuted, fontSize: 10.5, fontFamily },
      },
      series: [
        {
          name: '类目浏览',
          type: 'bar',
          barCategoryGap: '48%',
          itemStyle: { color: theme.categorical[2], borderRadius: [0, 4, 4, 0] },
          label: { show: false },
          data: chartRows.map((row, index) => ({
            value: row.view_cnt,
            label: {
              show: index === chartRows.length - 1,
              position: 'right',
              color: theme.textSecondary,
              fontSize: 10.5,
              fontFamily,
              formatter: formatInt(row.view_cnt),
            },
          })),
        },
      ],
    }
  }, [categoryRows, theme])

  const sparkOptions = useMemo(() => {
    const points = data?.behavior ?? []
    const dates = points.map((point) => point.date)
    return SPARK_METRICS.map((item, index) => {
      const color = theme.categorical[item.colorIndex]
      const isLast = index === SPARK_METRICS.length - 1
      const option: EChartsCoreOption = {
        animationDuration: 300,
        textStyle: { fontFamily },
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'line', lineStyle: { color: theme.axis } },
          ...tooltipBase(theme),
          valueFormatter: (value: unknown) => formatInt(Number(value)),
        },
        grid: { left: 4, right: 10, top: 8, bottom: isLast ? 6 : 14, containLabel: true },
        xAxis: {
          type: 'category',
          data: dates,
          boundaryGap: false,
          axisLine: { lineStyle: { color: theme.axis } },
          axisTick: { show: false },
          axisLabel: {
            show: isLast,
            color: theme.textMuted,
            fontSize: 10,
            fontFamily,
            hideOverlap: true,
            formatter: (value: string) => value.slice(5),
          },
        },
        yAxis: {
          type: 'value',
          min: 0,
          splitNumber: 2,
          axisLine: { show: false },
          axisTick: { show: false },
          axisLabel: {
            color: theme.textMuted,
            fontSize: 10,
            fontFamily,
            formatter: (value: number) => formatCompact(value),
          },
          splitLine: { lineStyle: { color: theme.grid } },
        },
        series: [
          {
            name: item.label,
            type: 'line',
            data: points.map((point) => point[item.key]),
            showSymbol: false,
            symbol: 'none',
            lineStyle: { width: 2, color },
            itemStyle: { color },
            emphasis: { focus: 'series' },
          },
        ],
      }
      return { item, option, color }
    })
  }, [data, theme])

  if (loading) {
    return (
      <div className="page-stack">
        <div className="grid-2">
          <ChartSkeleton height={540} />
          <ChartSkeleton height={540} />
        </div>
        <ChartSkeleton height={430} />
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

  return (
    <div className="page-stack">
      <div className="grid-2">
        <ChartCard
          title="Top 20 商品"
          subtitle={`区间聚合 · 按${metric === 'transaction_cnt' ? '成交' : '曝光'}降序`}
          action={
            <div className="segmented segmented-sm" role="group" aria-label="Top 商品排序指标">
              <button
                type="button"
                aria-pressed={metric === 'transaction_cnt'}
                onClick={() => setMetric('transaction_cnt')}
              >
                成交
              </button>
              <button
                type="button"
                aria-pressed={metric === 'view_cnt'}
                onClick={() => setMetric('view_cnt')}
              >
                曝光
              </button>
            </div>
          }
          footer="单条数值见 tooltip；仅标注榜首，避免逐柱标注。"
        >
          {rankedItems.length > 0 ? (
            <Chart option={topItemsOption} height={480} ariaLabel="Top 20 商品横向柱状图" />
          ) : (
            <EmptyState />
          )}
        </ChartCard>

        <ChartCard
          title="类目排行"
          subtitle="区间聚合 · 按浏览降序 · Top 15（真实类目）"
          footer="类目标识来自商品属性快照；未分类（0）为商品属性缺失桶，单独列示、不参与类目排名。"
        >
          {unclassified ? (
            <div className="category-unclassified">
              <span className="category-unclassified-name">未分类（0）</span>
              <span>
                浏览 <strong className="num">{formatInt(unclassified.view_cnt)}</strong>
              </span>
              <span className="category-unclassified-tag">不参与排名</span>
            </div>
          ) : null}
          {categoryRows.length > 0 ? (
            <Chart option={categoryOption} height={440} ariaLabel="类目 Top 15 横向柱状图（不含未分类）" />
          ) : (
            <EmptyState />
          )}
        </ChartCard>
      </div>

      <ChartCard
        title="商品行为日趋势"
        subtitle="浏览 / 加购 / 成交去重商品数；量级差异大，拆为独立小图，各自单轴"
        footer="按 UTC 日；每日去重商品数不可跨日相加。"
      >
        {data.behavior.length > 0 ? (
          <div className="spark-stack">
            {sparkOptions.map(({ item, option, color }) => (
              <div className="spark-row" key={item.key}>
                <div className="spark-label">
                  <span className="spark-dot" style={{ background: color }} />
                  {item.label}
                </div>
                <Chart option={option} height={106} ariaLabel={`${item.label}日趋势`} />
              </div>
            ))}
          </div>
        ) : (
          <EmptyState />
        )}
      </ChartCard>
    </div>
  )
}
