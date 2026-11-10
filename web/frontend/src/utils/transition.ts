import { flushSync } from 'react-dom'
import type { NavigateFunction, NavigateOptions } from 'react-router-dom'

type StartViewTransition = (callback: () => void | Promise<void>) => { finished: Promise<void> }

export function prefersReducedMotion(): boolean {
  return window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ?? false
}

/**
 * 路由切换先用浏览器的 View Transitions 做一次短交叉淡入；
 * 不支持或用户要求减少动态时退回普通导航。
 */
export function navigateWithTransition(
  navigate: NavigateFunction,
  to: string,
  options?: NavigateOptions,
): void {
  const doc = document as unknown as { startViewTransition?: StartViewTransition }
  const start = doc.startViewTransition
  if (typeof start !== 'function' || prefersReducedMotion()) {
    navigate(to, options)
    return
  }
  start.call(document, () => {
    flushSync(() => navigate(to, options))
  })
}
