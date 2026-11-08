import { useLocation } from 'react-router-dom'
import { useDateRange } from '../state/DateRangeContext'
import { useRefresh } from '../state/RefreshContext'
import { useThemeMode } from '../theme/ThemeContext'
import { DateRangePicker } from './DateRangePicker'
import { IconMoon, IconRefresh, IconSun } from './Icons'
import { Skeleton } from './Skeleton'

const PAGE_META: Record<string, { title: string; en: string }> = {
  '/': { title: '总览', en: 'Overview' },
  '/items': { title: '商品', en: 'Items' },
  '/retention': { title: '留存', en: 'Retention' },
  '/realtime': { title: '实时', en: 'Realtime' },
  '/quality': { title: '质量', en: 'Quality' },
}

// 这些页面不接收日期筛选，顶栏不展示日期控件，改用静态口径说明。
const STATIC_WINDOW: Record<string, string> = {
  '/realtime': '对账窗口 · 最近 7 个已对账日期',
  '/quality': '运维视图 · 不随日期筛选',
}

export function TopBar() {
  const location = useLocation()
  const { meta, metaLoading, metaError, reloadMeta } = useDateRange()
  const { mode, setMode } = useThemeMode()
  const refresh = useRefresh()
  const page = PAGE_META[location.pathname] ?? { title: '未找到页面', en: 'Not Found' }
  const staticWindow = STATIC_WINDOW[location.pathname] ?? null

  return (
    <header className="topbar">
      <div className="topbar-heading">
        <div className="topbar-title-row">
          <h1 className="topbar-title">{page.title}</h1>
          <span className="topbar-en">{page.en}</span>
        </div>
        {staticWindow ? (
          <p className="topbar-meta">{staticWindow}</p>
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
        {staticWindow ? null : <DateRangePicker />}

        <button type="button" className="btn btn-ghost" onClick={refresh} title="刷新当前页面数据">
          <IconRefresh size={16} />
          <span className="btn-label">刷新</span>
        </button>

        <div className="segmented theme-toggle" role="group" aria-label="主题切换">
          <button
            type="button"
            aria-pressed={mode === 'light'}
            onClick={() => setMode('light')}
            title="浅色主题"
          >
            <IconSun size={15} />
            <span className="sr-only">浅色主题</span>
          </button>
          <button
            type="button"
            aria-pressed={mode === 'dark'}
            onClick={() => setMode('dark')}
            title="深色主题"
          >
            <IconMoon size={15} />
            <span className="sr-only">深色主题</span>
          </button>
        </div>
      </div>
    </header>
  )
}
