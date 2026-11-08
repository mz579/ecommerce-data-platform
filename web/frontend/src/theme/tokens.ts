/**
 * 设计 token（与 SPEC §4.1 对齐）。
 * 页面 CSS 变量定义在 styles.css；此文件供 ECharts 与少量内联样式使用。
 */
export type ThemeMode = 'light' | 'dark'

export const categoricalLight = [
  '#2a78d6',
  '#eb6834',
  '#1baf7a',
  '#eda100',
  '#e87ba4',
  '#008300',
  '#6250d6',
  '#e34948',
] as const

export const categoricalDark = [
  '#3987e5',
  '#d95926',
  '#199e70',
  '#c98500',
  '#d55181',
  '#008300',
  '#9085e9',
  '#e66767',
] as const

export const statusColors = {
  good: '#0ca30c',
  warning: '#fab219',
  serious: '#ec835a',
  critical: '#d03b3b',
  neutral: '#898781',
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
  page: '#f9f9f7',
  surface: '#fcfcfb',
  textPrimary: '#0b0b0b',
  textSecondary: '#52514e',
  textMuted: '#898781',
  grid: '#e1e0d9',
  axis: '#c3c2b7',
  border: 'rgba(11,11,11,0.10)',
  categorical: categoricalLight,
  sequential: ['#cde2fb', '#9bc4ee', '#5f96cf', '#2f5f9e', '#0d366b'],
  tooltipBg: '#fcfcfb',
  tooltipBorder: 'rgba(11,11,11,0.16)',
  status: statusColors,
}

const darkTheme: ChartTheme = {
  mode: 'dark',
  page: '#0d0d0d',
  surface: '#1a1a19',
  textPrimary: '#f0efec',
  textSecondary: '#c3c2b7',
  textMuted: '#898781',
  grid: '#2c2c2a',
  axis: '#383835',
  border: 'rgba(240,239,236,0.12)',
  categorical: categoricalDark,
  // 深色下保持同一蓝色单色相：低值贴近表面，高值提亮
  sequential: ['#1b2c3f', '#21486f', '#2e6ca8', '#4f93d4', '#8bc0f2'],
  tooltipBg: '#1a1a19',
  tooltipBorder: '#383835',
  status: statusColors,
}

export function getChartTheme(mode: ThemeMode): ChartTheme {
  return mode === 'dark' ? darkTheme : lightTheme
}

export const fontFamily =
  'system-ui, -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif'
