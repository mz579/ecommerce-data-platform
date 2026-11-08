import { useEffect, useRef } from 'react'
import { init, type ECharts, type EChartsCoreOption } from 'echarts/core'
import '../charts/register'

interface ChartProps {
  option: EChartsCoreOption
  height?: number
  ariaLabel?: string
  className?: string
}

/** ECharts 轻封装：容器尺寸变化自适应，销毁时释放实例。 */
export function Chart({ option, height = 300, ariaLabel, className }: ChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<ECharts | null>(null)

  useEffect(() => {
    const element = containerRef.current
    if (!element) return undefined

    const chart = init(element, undefined, { renderer: 'canvas' })
    chartRef.current = chart

    const observer = new ResizeObserver(() => {
      if (chartRef.current && !chartRef.current.isDisposed()) chartRef.current.resize()
    })
    observer.observe(element)

    return () => {
      observer.disconnect()
      chart.dispose()
      chartRef.current = null
    }
  }, [])

  useEffect(() => {
    const chart = chartRef.current
    if (!chart || chart.isDisposed()) return
    chart.setOption(option, { notMerge: true })
  }, [option])

  return (
    <div
      ref={containerRef}
      className={className ? `chart-canvas ${className}` : 'chart-canvas'}
      style={{ height }}
      role="img"
      aria-label={ariaLabel}
    />
  )
}
