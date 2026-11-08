import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  type ReactNode,
} from 'react'
import { useSearchParams } from 'react-router-dom'
import { apiUrl } from '../api/client'
import { useApi } from '../api/useApi'
import type { MetaResponse } from '../api/types'
import { inclusiveDays, shiftDate } from '../utils/format'

export type RangePreset = '7' | '30' | '90' | 'all' | 'custom'

const VALID_PRESETS: RangePreset[] = ['7', '30', '90', 'all', 'custom']

interface DateRangeValue {
  from: string | null
  to: string | null
  days: number | null
  preset: RangePreset
  meta: MetaResponse | null
  metaLoading: boolean
  metaError: string | null
  reloadMeta: () => void
  setPreset: (preset: Exclude<RangePreset, 'custom'>) => void
  setCustom: (from: string, to: string) => void
}

const DateRangeContext = createContext<DateRangeValue | null>(null)

export function DateRangeProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()
  const metaResult = useApi<MetaResponse>(apiUrl('/meta'))
  const meta = metaResult.data

  const presetParam = searchParams.get('range') as RangePreset | null
  const fromParam = searchParams.get('from')
  const toParam = searchParams.get('to')

  const preset: RangePreset =
    presetParam && VALID_PRESETS.includes(presetParam)
      ? presetParam
      : fromParam && toParam
        ? 'custom'
        : 'all'

  let from: string | null = null
  let to: string | null = null

  if (preset === 'custom' && fromParam && toParam) {
    from = fromParam
    to = toParam
  } else if (meta) {
    if (preset === 'all') {
      from = meta.date_min
      to = meta.date_max
    } else {
      const span = Number(preset)
      if (Number.isFinite(span) && span > 0) {
        to = meta.date_max
        from = shiftDate(meta.date_max, -(span - 1))
        if (from < meta.date_min) from = meta.date_min
      }
    }
  }

  const days = from && to ? inclusiveDays(from, to) : null

  const setPreset = useCallback(
    (next: Exclude<RangePreset, 'custom'>) => {
      const params = new URLSearchParams(searchParams)
      params.set('range', next)
      params.delete('from')
      params.delete('to')
      setSearchParams(params, { replace: true })
    },
    [searchParams, setSearchParams],
  )

  const setCustom = useCallback(
    (nextFrom: string, nextTo: string) => {
      const params = new URLSearchParams(searchParams)
      params.set('range', 'custom')
      params.set('from', nextFrom)
      params.set('to', nextTo)
      setSearchParams(params, { replace: true })
    },
    [searchParams, setSearchParams],
  )

  const value = useMemo<DateRangeValue>(
    () => ({
      from,
      to,
      days,
      preset,
      meta,
      metaLoading: metaResult.loading,
      metaError: metaResult.error,
      reloadMeta: metaResult.reload,
      setPreset,
      setCustom,
    }),
    [
      from,
      to,
      days,
      preset,
      meta,
      metaResult.loading,
      metaResult.error,
      metaResult.reload,
      setPreset,
      setCustom,
    ],
  )

  return <DateRangeContext.Provider value={value}>{children}</DateRangeContext.Provider>
}

export function useDateRange(): DateRangeValue {
  const value = useContext(DateRangeContext)
  if (!value) throw new Error('useDateRange 必须在 DateRangeProvider 内使用')
  return value
}
