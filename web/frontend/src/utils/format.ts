/** 数字与日期格式化：千分位；比率保留 2 位小数；无数据显示 —。 */

const intFormatter = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 0 })
const oneDecimalFormatter = new Intl.NumberFormat('zh-CN', {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
})

export const EMPTY = '—'

export function formatInt(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY
  return intFormatter.format(value)
}

export function formatDecimal(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY
  return new Intl.NumberFormat('zh-CN', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value)
}

/** rate 为小数（0.0269 -> 2.69%）。 */
export function formatPct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY
  return `${(value * 100).toFixed(2)}%`
}

/** 坐标轴用的紧凑中文单位。 */
export function formatCompact(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY
  const abs = Math.abs(value)
  if (abs >= 1e8) return `${oneDecimalFormatter.format(value / 1e8)}亿`
  if (abs >= 1e4) return `${oneDecimalFormatter.format(value / 1e4)}万`
  if (abs >= 1e3) return intFormatter.format(value)
  return String(Math.round(value))
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return EMPTY
  return value.slice(0, 10)
}

/** API 时间戳按 UTC 展示，精确到分钟。 */
export function formatDateTime(value: string | null | undefined): string {
  if (!value) return EMPTY
  const normalized = /Z$|[+-]\d{2}:?\d{2}$/.test(value) ? value : `${value}Z`
  const date = new Date(normalized)
  if (Number.isNaN(date.getTime())) return value.replace('T', ' ').slice(0, 16)
  const parts = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'UTC',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date)
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? ''
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}`
}

/** 含端点的天数。 */
export function inclusiveDays(from: string, to: string): number {
  const start = Date.parse(`${from}T00:00:00Z`)
  const end = Date.parse(`${to}T00:00:00Z`)
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return 0
  return Math.round((end - start) / 86400000) + 1
}

/** 用 data max 往前推 N 天（含端点）。 */
export function shiftDate(date: string, days: number): string {
  const start = new Date(`${date}T00:00:00Z`)
  start.setUTCDate(start.getUTCDate() + days)
  return start.toISOString().slice(0, 10)
}

export function formatSignedInt(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return EMPTY
  if (value === 0) return '0'
  return `${value > 0 ? '+' : '-'}${intFormatter.format(Math.abs(value))}`
}
