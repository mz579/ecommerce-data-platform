interface EmptyStateProps {
  text?: string
  hint?: string
}

export function EmptyState({ text = '该区间暂无数据', hint }: EmptyStateProps) {
  return (
    <div className="empty-state">
      <span>{text}</span>
      {hint ? <span className="empty-hint">{hint}</span> : null}
    </div>
  )
}
