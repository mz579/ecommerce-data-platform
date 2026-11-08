import { useCallback, useEffect, useState } from 'react'
import { fetchApi } from './client'
import { useRefreshToken } from '../state/RefreshContext'

export interface ApiResult<T> {
  data: T | null
  loading: boolean
  error: string | null
  reload: () => void
}

/**
 * 极简数据请求 hook：url 变化或点击全局刷新时重新拉取。
 * 请求失败保留错误文案，由调用方在卡片内展示重试。
 */
export function useApi<T>(url: string | null): ApiResult<T> {
  const refreshToken = useRefreshToken()
  const [tick, setTick] = useState(0)
  const [state, setState] = useState<{ data: T | null; loading: boolean; error: string | null }>(
    { data: null, loading: Boolean(url), error: null },
  )

  const reload = useCallback(() => setTick((value) => value + 1), [])

  useEffect(() => {
    if (!url) {
      setState({ data: null, loading: false, error: null })
      return undefined
    }

    const controller = new AbortController()
    setState({ data: null, loading: true, error: null })

    fetchApi<T>(url, controller.signal)
      .then((data) => {
        if (controller.signal.aborted) return
        setState({ data, loading: false, error: null })
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return
        const message = error instanceof Error ? error.message : '请求失败，请稍后重试'
        setState({ data: null, loading: false, error: message })
      })

    return () => controller.abort()
  }, [url, tick, refreshToken])

  return { ...state, reload }
}
