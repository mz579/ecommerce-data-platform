import { useEffect } from 'react'
import { prefersReducedMotion } from '../utils/transition'

interface NoirRevealProps {
  jobId?: string
  /** 结果内容是否已就绪；减少动态模式下先压住页面，就绪后再淡出。 */
  ready?: boolean
  onDone: () => void
}

/**
 * 分析完成后的「案卷展开」：两扇门板先合拢压住页面，再向两侧退开，
 * 把已经渲染好的结果页让出来。动画在结果页上播放，避免先播开场再硬切。
 */
export function NoirReveal({ jobId, ready = true, onDone }: NoirRevealProps) {
  const reduced = prefersReducedMotion()

  useEffect(() => {
    if (!ready) return
    const timer = window.setTimeout(onDone, reduced ? 280 : 1700)
    return () => window.clearTimeout(timer)
  }, [onDone, reduced, ready])

  return (
    <div
      className={`noir-reveal${reduced ? ' is-reduced' : ''}${
        reduced && ready ? ' is-ready' : ''
      }`}
      role="status"
      aria-live="polite"
      aria-label="分析完成，正在打开结果"
    >
      <div className="noir-reveal__panel noir-reveal__panel--left">
        <svg className="noir-reveal__figure" viewBox="0 0 260 560" aria-hidden="true">
          <g fill="#B3002E">
            <ellipse cx="130" cy="96" rx="56" ry="9" />
            <path d="M96 93c0-30 11-46 34-46s34 16 34 46z" />
            <path d="M117 100h26v24h-26z" />
            <path d="M130 120c-26 0-45 11-52 30l-14 96 6 314h120l6-314-14-96c-7-19-26-30-52-30z" />
          </g>
          <path d="M130 126l17 42-17 34-17-34z" fill="#D7D6D4" />
        </svg>
      </div>

      <div className="noir-reveal__panel noir-reveal__panel--right">
        <svg
          className="noir-reveal__figure noir-reveal__figure--right"
          viewBox="0 0 260 560"
          aria-hidden="true"
        >
          <g fill="#08080A">
            <path d="M130 44c-27 0-43 18-43 45 0 11 2 20 7 27h72c5-7 7-16 7-27 0-27-16-45-43-45z" />
            <path d="M90 90h80c7 15 7 36-5 50H95c-12-14-12-35-5-50z" />
            <path d="M117 138h26v20h-26z" />
            <path d="M130 156c-25 0-44 11-50 29l-12 92 8 283h108l8-283-12-92c-6-18-25-29-50-29z" />
          </g>
          <path d="M130 162l16 40-16 32-16-32z" fill="#D7D6D4" />
        </svg>
      </div>

      <div className="noir-reveal__title">
        <p className="noir-reveal__latin">
          <span>ANALYSIS</span>
          <span>COMPLETE</span>
        </p>
        <span className="noir-reveal__diamond" aria-hidden="true" />
        <p className="noir-reveal__cn">分析完成</p>
        {jobId ? (
          <p className="noir-reveal__case">CASE FILE · {jobId.slice(0, 8).toUpperCase()}</p>
        ) : null}
      </div>

      <div className="noir-reveal__grain" aria-hidden="true" />
    </div>
  )
}
