/**
 * 设计 token（与 SPEC §4.1 对齐）。
 * 页面 CSS 变量定义在 styles.css；此文件供 ECharts 与少量内联样式使用。
 */
export type ThemeMode = 'light' | 'dark'

export const categoricalLight = [
  '#b3002e',
  '#1f1d1b',
  '#7a7269',
  '#d46a6a',
  '#4a4540',
  '#9e8e85',
  '#8c1d2f',
  '#c9bfb5',
] as const

export const categoricalDark = [
  '#c41e3a',
  '#d7d6d4',
  '#8a7f7c',
  '#e0727f',
  '#6e1a2a',
  '#a8a29a',
  '#f2ede4',
  '#5a5550',
] as const

/** 转化率图里的两条折线：与绯红主题区分开的蓝 / 绿。 */
export const rateLineColors = {
  light: ['#1d4ed8', '#047857'],
  dark: ['#3b82f6', '#34d399'],
} as const

export const statusColors = {
  good: '#7d9468',
  warning: '#c9a24a',
  serious: '#c06a3e',
  critical: '#c33241',
  neutral: '#8a8179',
} as const

export interface ChartTheme {
  mode: ThemeMode
  page: string
  surface: string
  textPrimary: string
  textSecondary: string
  textMuted: string
  grid: string
  axis: string
  border: string
  categorical: readonly string[]
  /** 热力图单色蓝阶（低 → 高） */
  sequential: readonly string[]
  tooltipBg: string
  tooltipBorder: string
  status: typeof statusColors
}

const lightTheme: ChartTheme = {
  mode: 'light',
  page: '#d9d5ce',
  surface: '#e8e4dc',
  textPrimary: '#08080a',
  textSecondary: '#332e2a',
  textMuted: '#5c544c',
  grid: '#c7c0b6',
  axis: '#9a9187',
  border: 'rgba(8,8,10,0.16)',
  categorical: categoricalLight,
  sequential: ['#e3d9d3', '#c9a9a5', '#b5707a', '#8c1d2f', '#4a0a16'],
  tooltipBg: '#e8e4dc',
  tooltipBorder: 'rgba(8,8,10,0.32)',
  status: statusColors,
}

const darkTheme: ChartTheme = {
  mode: 'dark',
  page: '#08080a',
  surface: '#0f0f12',
  textPrimary: '#f4f1eb',
  textSecondary: '#d0c9bf',
  textMuted: '#9a938a',
  grid: '#302e36',
  axis: '#4a4650',
  border: 'rgba(215,214,212,0.14)',
  categorical: categoricalDark,
  // 墨黑底上的绯红单色阶：低值贴近表面，高值提亮
  sequential: ['#2a0a12', '#4a101c', '#7a1a2e', '#b3002e', '#e88a96'],
  tooltipBg: '#0f0f12',
  tooltipBorder: '#d7d6d4',
  status: statusColors,
}

export function getChartTheme(mode: ThemeMode): ChartTheme {
  return mode === 'dark' ? darkTheme : lightTheme
}

export const fontFamily =
  '"Segoe UI", "Noto Sans SC", "Microsoft YaHei", system-ui, sans-serif'
