import { useState } from 'react'
import { useLocation } from 'react-router-dom'
import { useDateRange } from '../state/DateRangeContext'
import { useRefresh } from '../state/RefreshContext'
import { useThemeMode } from '../theme/ThemeContext'
import { DateRangePicker } from './DateRangePicker'
import { IconFile, IconMoon, IconRefresh, IconSun } from './Icons'
import { ProvenanceModal } from './ProvenanceModal'
import { Skeleton } from './Skeleton'

const PAGE_META: Record<string, { title: string; en: string }> = {
  '/': { title: '总览', en: 'Overview' },
  '/items': { title: '商品', en: 'Items' },
  '/retention': { title: '留存', en: 'Retention' },
  '/realtime': { title: '批流对账', en: 'Reconciliation' },
  '/quality': { title: '质量', en: 'Quality' },
  '/analyze': { title: '分析', en: 'Analyze' },
  '/model': { title: '模型', en: 'Model' },
}

// 这些页面不接收日期筛选，顶栏不展示日期控件，改用静态口径说明。
const STATIC_WINDOW: Record<string, string> = {
  '/realtime': '内部验证视图 · 实时数据源未接入',
  '/quality': '运维视图 · 不随日期筛选',
  '/analyze': '上传分析 · 独立 DuckDB 沙箱',
  '/model': '架构视图 · 不随日期筛选',
}

function pageMeta(pathname: string): { title: string; en: string } {
  if (pathname.startsWith('/analyze/')) {
    return pathname.endsWith('/result')
      ? { title: '分析结果', en: 'Result' }
      : { title: '分析任务', en: 'Case File' }
  }
  return PAGE_META[pathname] ?? { title: '未找到页面', en: 'Not Found' }
}

function staticWindow(pathname: string): string | null {
  if (pathname.startsWith('/analyze/')) return '上传分析 · 独立 DuckDB 沙箱'
  return STATIC_WINDOW[pathname] ?? null
}

export function TopBar() {
  const location = useLocation()
  const { meta, metaLoading, metaError, reloadMeta } = useDateRange()
  const { mode, setMode } = useThemeMode()
  const refresh = useRefresh()
  const [provenanceOpen, setProvenanceOpen] = useState(false)
  const page = pageMeta(location.pathname)
  const fixedWindow = staticWindow(location.pathname)
  const showRefresh =
    location.pathname !== '/model' &&
    location.pathname !== '/analyze' &&
    !location.pathname.startsWith('/analyze/')

  return (
    <>
      <header className="topbar">
        <div className="topbar-heading">
          <div className="topbar-title-row">
            <h1 className="topbar-title">{page.title}</h1>
            <span className="topbar-en">{page.en}</span>
          </div>
          {fixedWindow ? (
            <p className="topbar-meta">{fixedWindow}</p>
          ) : metaLoading ? (
            <Skeleton width={260} height={10} style={{ marginTop: 8 }} />
          ) : meta ? (
            <p className="topbar-meta num">
              数据窗口 {meta.date_min} ~ {meta.date_max} · {meta.full_days} 个完整日
            </p>
          ) : metaError ? (
            <p className="topbar-meta topbar-error">
              元数据加载失败，默认展示全部区间 ·
              <button type="button" className="link-button" onClick={reloadMeta}>
                重试
              </button>
            </p>
          ) : (
            <p className="topbar-meta">数据窗口加载中</p>
          )}
        </div>

        <div className="topbar-controls">
          {fixedWindow ? null : <DateRangePicker />}

          <button
            type="button"
            className="btn btn-ghost"
            onClick={() => setProvenanceOpen(true)}
            title="数据来源与口径"
          >
            <IconFile size={16} />
            <span className="btn-label">来源</span>
          </button>

          {showRefresh ? (
            <button
              type="button"
              className="btn btn-ghost"
              onClick={refresh}
              title="刷新当前页面数据"
            >
              <IconRefresh size={16} />
              <span className="btn-label">刷新</span>
            </button>
          ) : null}

          <div className="segmented theme-toggle" role="group" aria-label="主题切换">
            <button
              type="button"
              aria-pressed={mode === 'light'}
              onClick={() => setMode('light')}
              title="纸本主题"
            >
              <IconSun size={15} />
              <span className="sr-only">纸本主题</span>
            </button>
            <button
              type="button"
              aria-pressed={mode === 'dark'}
              onClick={() => setMode('dark')}
              title="夜巡主题"
            >
              <IconMoon size={15} />
              <span className="sr-only">夜巡主题</span>
            </button>
          </div>
        </div>
      </header>

      <ProvenanceModal open={provenanceOpen} onClose={() => setProvenanceOpen(false)} />
    </>
  )
}
