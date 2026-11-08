import type { ReactNode } from 'react'

interface ChartCardProps {
  title: string
  subtitle?: ReactNode
  action?: ReactNode
  footer?: ReactNode
  children: ReactNode
  className?: string
}

export function ChartCard({ title, subtitle, action, footer, children, className }: ChartCardProps) {
  return (
    <section className={className ? `card chart-card ${className}` : 'card chart-card'}>
      <header className="card-head">
        <div className="card-title-wrap">
          <h2 className="card-title">{title}</h2>
          {subtitle ? <p className="card-sub">{subtitle}</p> : null}
        </div>
        {action ? <div className="card-action">{action}</div> : null}
      </header>
      <div className="card-body">{children}</div>
      {footer ? <div className="card-foot">{footer}</div> : null}
    </section>
  )
}
