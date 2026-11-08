import { useEffect, useMemo, useState } from 'react'
import { useDateRange, type RangePreset } from '../state/DateRangeContext'

const PRESETS: Array<{ value: Exclude<RangePreset, 'custom'>; label: string }> = [
  { value: '7', label: '近 7 天' },
  { value: '30', label: '近 30 天' },
  { value: '90', label: '近 90 天' },
  { value: 'all', label: '全部' },
]

export function DateRangePicker() {
  const { preset, setPreset, setCustom, from, to, days, meta, metaLoading } = useDateRange()
  const [customOpen, setCustomOpen] = useState(false)
  const [draftFrom, setDraftFrom] = useState('')
  const [draftTo, setDraftTo] = useState('')

  useEffect(() => {
    if (preset === 'custom') {
      setCustomOpen(true)
      setDraftFrom(from ?? '')
      setDraftTo(to ?? '')
    } else {
      setCustomOpen(false)
    }
  }, [preset, from, to])

  const validation = useMemo(() => {
    if (!draftFrom || !draftTo) return { valid: false, message: '' }
    if (draftFrom > draftTo) return { valid: false, message: '开始日期不能晚于结束日期' }
    if (meta && (draftFrom < meta.date_min || draftTo > meta.date_max)) {
      return { valid: false, message: `日期需在 ${meta.date_min} ~ ${meta.date_max} 内` }
    }
    return { valid: true, message: '' }
  }, [draftFrom, draftTo, meta])

  const disabled = metaLoading

  return (
    <div className="date-range">
      <div className="segmented" role="group" aria-label="日期范围预设">
        {PRESETS.map((item) => (
          <button
            key={item.value}
            type="button"
            aria-pressed={preset === item.value}
            disabled={disabled}
            onClick={() => setPreset(item.value)}
          >
            {item.label}
          </button>
        ))}
        <button
          type="button"
          aria-pressed={preset === 'custom'}
          disabled={disabled}
          onClick={() => {
            if (customOpen && preset === 'custom') {
              setCustomOpen(false)
              return
            }
            setDraftFrom(from ?? meta?.date_min ?? '')
            setDraftTo(to ?? meta?.date_max ?? '')
            setCustomOpen(true)
          }}
        >
          自定义
        </button>
      </div>

      {from && to ? (
        <span className="range-readout num">
          {from} ~ {to} · {days} 天
        </span>
      ) : null}

      {customOpen ? (
        <div className="custom-range">
          <label className="field">
            <span className="field-label">开始</span>
            <input
              className="input"
              type="date"
              value={draftFrom}
              min={meta?.date_min}
              max={meta?.date_max}
              onChange={(event) => setDraftFrom(event.target.value)}
              aria-label="开始日期"
            />
          </label>
          <span className="range-sep">~</span>
          <label className="field">
            <span className="field-label">结束</span>
            <input
              className="input"
              type="date"
              value={draftTo}
              min={meta?.date_min}
              max={meta?.date_max}
              onChange={(event) => setDraftTo(event.target.value)}
              aria-label="结束日期"
            />
          </label>
          <button
            type="button"
            className="btn btn-small btn-primary"
            disabled={!validation.valid}
            onClick={() => setCustom(draftFrom, draftTo)}
          >
            应用
          </button>
          {validation.message ? <span className="field-error">{validation.message}</span> : null}
        </div>
      ) : null}
    </div>
  )
}
