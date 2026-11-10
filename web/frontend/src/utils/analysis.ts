import type { AnalysisField, AnalysisStatus } from '../api/analysisTypes'

export const FIELD_LABELS: Record<AnalysisField, string> = {
  user_id: '用户编号',
  item_id: '商品编号',
  event_type: '行为类型',
  event_time: '发生时间',
  transaction_id: '订单编号',
  category_id: '商品分类',
  amount: '金额',
  product_name: '商品名称',
}

export const FIELD_HINTS: Record<AnalysisField, string> = {
  user_id: '每一行是谁产生的，例如访客编号',
  item_id: '这一行说的是哪个商品',
  event_type: '用户做了什么：浏览、加购或下单',
  event_time: '这个行为发生的时间',
  transaction_id: '可选；用来统计订单数',
  category_id: '可选；用来按商品分类分析',
  amount: '可选；用来统计成交金额',
  product_name: '可选；结果里显示商品名称',
}

export const REQUIRED_FIELDS: AnalysisField[] = [
  'user_id',
  'item_id',
  'event_type',
  'event_time',
]

export const OPTIONAL_FIELDS: AnalysisField[] = [
  'transaction_id',
  'category_id',
  'amount',
  'product_name',
]

export const ALL_FIELDS: AnalysisField[] = [...REQUIRED_FIELDS, ...OPTIONAL_FIELDS]

export const STATUS_LABELS: Record<AnalysisStatus, string> = {
  received: '已接收',
  detecting: '正在识别',
  awaiting_mapping: '等待确认',
  queued: '排队中',
  parsing: '正在读取',
  normalizing: '正在整理',
  aggregating: '正在计算',
  done: '已完成',
  failed: '失败',
  cancelled: '已取消',
}

export const STAGE_ORDER: AnalysisStatus[] = [
  'received',
  'detecting',
  'awaiting_mapping',
  'queued',
  'parsing',
  'normalizing',
  'aggregating',
  'done',
]

export const STAGE_LABELS: Record<string, string> = {
  received: '接收文件',
  detecting: '看懂表格',
  awaiting_mapping: '确认列的含义',
  queued: '排队等待',
  parsing: '读取每一行',
  normalizing: '整理并去重',
  aggregating: '计算统计结果',
  done: '完成',
  failed: '分析失败',
  cancelled: '已取消',
}

export function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  if (value >= 1024 * 1024 * 1024) return `${(value / 1024 / 1024 / 1024).toFixed(2)} GB`
  if (value >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)} MB`
  if (value >= 1024) return `${(value / 1024).toFixed(0)} KB`
  return `${Math.round(value)} B`
}

export function formatDuration(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value) || value < 0) return '—'
  const total = Math.round(value)
  if (total < 60) return `${total} 秒`
  const minutes = Math.floor(total / 60)
  const seconds = total % 60
  if (minutes < 60) return `${minutes} 分 ${seconds} 秒`
  const hours = Math.floor(minutes / 60)
  return `${hours} 小时 ${minutes % 60} 分`
}

export function confidenceTone(confidence: number): 'high' | 'medium' | 'low' {
  if (confidence >= 0.75) return 'high'
  if (confidence >= 0.4) return 'medium'
  return 'low'
}
