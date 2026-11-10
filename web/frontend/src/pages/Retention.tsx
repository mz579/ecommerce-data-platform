import { useMemo } from 'react'
import type { EChartsCoreOption } from 'echarts/core'
import { apiUrl } from '../api/client'
import { useApi } from '../api/useApi'
import type { RetentionCohort, RetentionResponse } from '../api/types'
import { Chart } from '../components/Chart'
import { ChartCard } from '../components/ChartCard'
import { EmptyState } from '../components/EmptyState'
import { ErrorState } from '../components/ErrorState'
import { KpiCard } from '../components/KpiCard'
import { ChartSkeleton, KpiSkeleton } from '../components/Skeleton'
import { useDateRange } from '../state/DateRangeContext'
import { fontFamily, getChartTheme } from '../theme/tokens'
import { useThemeMode } from '../theme/ThemeContext'
import { formatInt, formatPct, shiftDate } from '../utils/format'

const MAX_VISIBLE_COHORTS = 20

function cohortStatus(cohort: RetentionCohort): string {
  return (cohort.window_status ?? 'full').toLowerCase()
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

export default function RetentionPage() {
  const { from, to } = useDateRange()
  const { mode } = useThemeMode()
  const theme = getChartTheme(mode)
  const url = useMemo(
    () => apiUrl('/retention', { date_from: from, date_to: to, max_age: 14 }),
    [from, to],
  )
  const { data, loading, error, reload } = useApi<RetentionResponse>(url)

  const visibleCohorts = useMemo<RetentionCohort[]>(
    () => (data ? data.cohorts.slice(-MAX_VISIBLE_COHORTS) : []),
    [data],
  )

  const ageDays = useMemo(() => {
    let maxAge = 0
    for (const cohort of visibleCohorts) {
      for (const value of cohort.values) maxAge = Math.max(maxAge, value.age_day)
    }
    return Array.from({ length: maxAge + 1 }, (_, index) => index)
  }, [visibleCohorts])

  // age_day=1 的行级窗口状态：优先用 values[].window_status，
  // 其次 d1_series[].window_status；两者都缺失时才回退 cohort 级并做区间边界推断。
  const d1StatusByDate = useMemo(() => {
    const cohorts = data?.cohorts ?? []
    const series = data?.d1_series ?? []
    const maxDate = cohorts.reduce(
      (max, cohort) => (cohort.cohort_date > max ? cohort.cohort_date : max),
      '',
    )
    const seriesStatus = new Map<string, string>()
    for (const point of series) {
      if (point.window_status) seriesStatus.set(point.date, point.window_status.toLowerCase())
    }

    const statusByDate = new Map<string, string>()
    for (const cohort of cohorts) {
      const d1Value = cohort.values.find((value) => value.age_day === 1)
      const explicit =
        d1Value?.window_status?.toLowerCase() ?? seriesStatus.get(cohort.cohort_date)
      let status: string
      if (!d1Value || d1Value.rate === null) {
        status = 'partial'
      } else if (explicit) {
        status = explicit
      } else if (cohortStatus(cohort) === 'partial') {
        status = 'partial'
      } else if (maxDate !== '' && shiftDate(cohort.cohort_date, 1) >= maxDate) {
        // 行级字段缺失时的兜底：D1 日落在区间末日附近，窗口不完整。
        status = 'partial'
      } else {
        status = 'full'
      }
      statusByDate.set(cohort.cohort_date, status)
    }
    for (const point of series) {
      if (point.window_status && !statusByDate.has(point.date)) {
        statusByDate.set(point.date, point.window_status.toLowerCase())
      }
    }
    return statusByDate
  }, [data])

  const heatmapOption = useMemo<EChartsCoreOption>(() => {
    // 数据只保留 [x, y, value] 三个维度，避免 visualMap 误取扩展维度。
    const points: Array<[number, number, number]> = []
    visibleCohorts.forEach((cohort, rowIndex) => {
      cohort.values.forEach((value) => {
        if (value.rate === null) return
        points.push([value.age_day, rowIndex, value.rate])
      })
    })

    const nonBaseRates = visibleCohorts.flatMap((cohort) =>
      cohort.values
        .filter((value) => value.age_day > 0 && value.rate !== null)
        .map((value) => value.rate as number),
    )
    const observedMax = nonBaseRates.length > 0 ? Math.max(...nonBaseRates) : 0.03
    const colorMax = Math.min(1, Math.max(0.01, Math.ceil(observedMax * 100) / 100))

    return {
      animationDuration: 300,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'item',
        ...tooltipBase(theme),
        formatter: (params: any) => {
          const [ageIndex, rowIndex, rate] = params.data as [number, number, number]
          const cohort = visibleCohorts[rowIndex]
          if (!cohort) return ''
          const cell = cohort.values.find((value) => value.age_day === ageIndex)
          const rowStatus = ageIndex === 1 ? d1StatusByDate.get(cohort.cohort_date) : undefined
          const status = (
            cell?.window_status ??
            rowStatus ??
            cohort.window_status ??
            'full'
          ).toLowerCase()
          const suffix = status === 'partial' ? '<br/>窗口不完整（partial）' : ''
          const baseNote = ageIndex === 0 ? '<br/>D0 为 cohort 基数，不参与色阶标定' : ''
          return `${cohort.cohort_date} · D${ageIndex}<br/>留存率 ${formatPct(
            rate,
          )}<br/>cohort 规模 ${formatInt(cohort.cohort_size)}${baseNote}${suffix}`
        },
      },
      grid: { left: 4, right: 62, top: 30, bottom: 8, containLabel: true },
      xAxis: {
        type: 'category',
        data: ageDays.map((age) => `D${age}`),
        position: 'top',
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: theme.textMuted, fontSize: 10.5, fontFamily },
        splitArea: { show: false },
      },
      yAxis: {
        type: 'category',
        data: visibleCohorts.map((cohort) =>
          cohortStatus(cohort) === 'partial' ? `${cohort.cohort_date}*` : cohort.cohort_date,
        ),
        inverse: true,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: theme.textMuted, fontSize: 10, fontFamily },
      },
      visualMap: {
        min: 0,
        max: colorMax,
        calculable: false,
        orient: 'vertical',
        right: 2,
        top: 'middle',
        itemWidth: 10,
        itemHeight: 130,
        precision: 2,
        text: ['高', '低'],
        textGap: 6,
        textStyle: { color: theme.textMuted, fontSize: 10, fontFamily },
        formatter: (value: number) => formatPct(value),
        inRange: { color: theme.sequential as string[] },
      },
      series: [
        {
          type: 'heatmap',
          data: points,
          label: { show: false },
          itemStyle: {
            borderColor: theme.surface,
            borderWidth: 2,
          },
          emphasis: {
            itemStyle: { borderColor: theme.textSecondary, borderWidth: 1 },
          },
        },
      ],
    }
  }, [visibleCohorts, ageDays, d1StatusByDate, theme])

  const d1Option = useMemo<EChartsCoreOption>(() => {
    const series = data?.d1_series ?? []
    const dates = series.map((point) => point.date)
    const weighted = data?.weighted_d1
    // D1 曲线以 d1_series 行级状态为准，缺失时回退 values/cohort 的推断结果。
    const statusByDate = new Map<string, string>()
    for (const point of series) {
      statusByDate.set(
        point.date,
        (point.window_status ?? d1StatusByDate.get(point.date) ?? 'full').toLowerCase(),
      )
    }
    const partialPoints = series.flatMap((point, index) => {
      if (statusByDate.get(point.date) !== 'partial' || point.d1_rate === null) return []
      return [
        {
          value: [point.date, point.d1_rate],
          isLast: index === series.length - 1,
        },
      ]
    })
    return {
      color: [theme.categorical[0]],
      animationDuration: 320,
      textStyle: { fontFamily },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross', lineStyle: { color: theme.axis }, crossStyle: { color: theme.axis } },
        ...tooltipBase(theme),
        formatter: (params: any) => {
          const index =
            params?.find((item: any) => item.seriesIndex === 0)?.dataIndex ??
            params?.[0]?.dataIndex ??
            0
          const point = series[index]
          if (!point) return ''
          const status = statusByDate.get(point.date) ?? 'full'
          return `${point.date}<br/>D1 留存率 ${formatPct(
            point.d1_rate,
          )}<br/>window_status ${status}${
            status === 'partial' ? '（不参与加权）' : ''
          }`
        },
      },
      grid: { left: 4, right: 26, top: 16, bottom: 4, containLabel: true },
      xAxis: {
        type: 'category',
        data: dates,
        boundaryGap: false,
        axisLine: { lineStyle: { color: theme.axis } },
        axisTick: { show: false },
        axisLabel: {
          color: theme.textMuted,
          fontSize: 10.5,
          fontFamily,
          hideOverlap: true,
          formatter: (value: string) => value.slice(5),
        },
      },
      yAxis: {
        type: 'value',
        min: 0,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: {
          color: theme.textMuted,
          fontSize: 10.5,
          fontFamily,
          formatter: (value: number) => formatPct(value),
        },
        splitLine: { lineStyle: { color: theme.grid } },
      },
      series: [
        {
          name: 'D1 留存率（逐 cohort）',
          type: 'line',
          data: series.map((point) => point.d1_rate),
          showSymbol: false,
          symbol: 'none',
          lineStyle: { width: 2.4 },
          emphasis: { focus: 'series' },
          markLine:
            weighted !== null && weighted !== undefined
              ? {
                  symbol: 'none',
                  silent: true,
                  lineStyle: { type: 'dashed', color: theme.categorical[1], width: 1 },
                  label: {
                    show: true,
                    position: 'insideEndTop',
                    color: theme.textSecondary,
                    fontSize: 10,
                    fontFamily,
                    formatter: `加权 D1 ${formatPct(weighted)}`,
                  },
                  data: [{ yAxis: weighted }],
                }
              : undefined,
        },
        {
          name: 'D1 partial',
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
  }, [data, d1StatusByDate, theme])

  const cohortStats = useMemo(() => {
    const cohorts = data?.cohorts ?? []
    const fullCohorts = cohorts.filter(
      (cohort) => d1StatusByDate.get(cohort.cohort_date) === 'full',
    )
    const allSize = cohorts.reduce((sum, cohort) => sum + cohort.cohort_size, 0)
    const weightedSize = fullCohorts.reduce((sum, cohort) => sum + cohort.cohort_size, 0)
    return {
      total: cohorts.length,
      full: fullCohorts.length,
      partial: cohorts.length - fullCohorts.length,
      allSize,
      weightedSize,
    }
  }, [data, d1StatusByDate])

  if (loading) {
    return (
      <div className="page-stack">
        <ChartSkeleton height={680} />
        <div className="grid-2">
          <ChartSkeleton />
          <div className="stack-16">
            <KpiSkeleton />
            <KpiSkeleton />
          </div>
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

  const hasCohorts = visibleCohorts.length > 0
  const heatmapHeight = Math.min(520, Math.max(280, visibleCohorts.length * 20 + 60))
  const maxAgeDay = ageDays[ageDays.length - 1] ?? 0
  // 当前 ADS 只有 D0 / D1：两列热力图会显得又高又空，直接用 D1 曲线表达。
  const singleAgeDay = maxAgeDay <= 1
  const heatmapFooter = [
    'D0 恒为 100%，仅作 cohort 基数参考；色阶按非 D0 留存率标定。',
    '* 为 cohort 级窗口不完整；D1 行级状态见曲线 * 与 tooltip。',
    maxAgeDay < 14 ? `当前 ADS 仅提供 D0-D${maxAgeDay} 年龄日。` : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div className="page-stack">
      {!singleAgeDay ? (
        <ChartCard
          title="Cohort 留存热力图"
          subtitle={
            hasCohorts
              ? `行 = cohort 日期，列 = age_day 0-${maxAgeDay}；展示区间内最近 ${visibleCohorts.length} 个 cohort（共 ${data.cohorts.length} 个）`
              : '行 = cohort 日期，列 = age_day'
          }
          footer={heatmapFooter}
        >
          {hasCohorts ? (
            <Chart
              option={heatmapOption}
              height={heatmapHeight}
              ariaLabel="Cohort 留存热力图"
            />
          ) : (
            <EmptyState />
          )}
        </ChartCard>
      ) : null}

      <div className="grid-2">
        <ChartCard
          title="D1 留存曲线（逐 cohort）"
          subtitle={
            singleAgeDay
              ? '当前 ADS 只提供 D1，使用曲线代替两列热力图；D0 恒为 100%，不再单独占一列'
              : '逐 cohort 的次日留存率；虚线为区间加权 D1（Σretained / Σcohort_size）'
          }
          footer="曲线上的 * 表示该日 D1 行 window_status=partial（优先取 d1_series.window_status，缺失回退 cohort 级），不参与加权；曲线末端下降不代表真实留存恶化。"
        >
          {data.d1_series.length > 0 ? (
            <Chart
              option={d1Option}
              height={300}
              ariaLabel="D1 留存率趋势折线图，partial 日期以星号标记"
            />
          ) : (
            <EmptyState />
          )}
        </ChartCard>

        <div className="stack-16">
          <KpiCard
            label="D1 留存（加权）"
            value={formatPct(data.weighted_d1)}
            hint="Σ 次日留存用户 / Σ D1 完整窗口规模（加权分母）"
          />

          <div className="card card-pad">
            <div className="card-title">Cohort 概况</div>
            <div className="stat-list">
              <div className="stat-row">
                <span>区间 cohort 数</span>
                <strong>{formatInt(cohortStats.total)}</strong>
              </div>
              <div className="stat-row">
                <span>完整窗口（D1 行级）</span>
                <strong>{formatInt(cohortStats.full)}</strong>
              </div>
              <div className="stat-row">
                <span>部分窗口（D1 行级）</span>
                <strong>{formatInt(cohortStats.partial)}</strong>
              </div>
              <div className="stat-row">
                <span>全部 cohort 规模</span>
                <strong>{formatInt(cohortStats.allSize)}</strong>
              </div>
              <div className="stat-row">
                <span>D1 完整窗口规模（加权分母）</span>
                <strong>{formatInt(cohortStats.weightedSize)}</strong>
              </div>
            </div>
            <p className="stat-note">
              完整/部分窗口按 age_day=1 的行级 window_status 统计（缺失时回退 cohort 级）；
              D1 完整窗口规模即加权分母。
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
