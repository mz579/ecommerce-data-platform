import type { CSSProperties } from 'react'

interface SkeletonProps {
  width?: number | string
  height?: number | string
  radius?: number
  style?: CSSProperties
  className?: string
}

export function Skeleton({ width = '100%', height = 12, radius = 6, style, className }: SkeletonProps) {
  return (
    <span
      className={className ? `skeleton ${className}` : 'skeleton'}
      style={{ width, height, borderRadius: radius, ...style }}
      aria-hidden="true"
    />
  )
}

export function KpiSkeleton() {
  return (
    <div className="card kpi-card" aria-hidden="true">
      <Skeleton width={72} height={11} />
      <Skeleton width="55%" height={30} style={{ marginTop: 12 }} />
      <Skeleton width="70%" height={10} style={{ marginTop: 10 }} />
    </div>
  )
}

export function ChartSkeleton({ height = 300 }: { height?: number }) {
  return (
    <div className="card chart-card" aria-hidden="true">
      <div className="card-head">
        <div className="card-title-wrap">
          <Skeleton width={120} height={13} />
          <Skeleton width={200} height={10} style={{ marginTop: 8 }} />
        </div>
      </div>
      <Skeleton width="100%" height={height - 60} radius={8} style={{ marginTop: 12 }} />
    </div>
  )
}

export function PageSkeleton() {
  return (
    <div className="page-stack" aria-label="加载中">
      <div className="kpi-grid">
        {Array.from({ length: 4 }, (_, index) => (
          <KpiSkeleton key={index} />
        ))}
      </div>
      <div className="grid-2">
        <ChartSkeleton />
        <ChartSkeleton />
      </div>
    </div>
  )
}
