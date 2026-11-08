interface ErrorStateProps {
  message: string
  onRetry: () => void
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="error-state" role="alert">
      <span className="error-text">{message}</span>
      <button type="button" className="btn btn-small" onClick={onRetry}>
        重试
      </button>
    </div>
  )
}
