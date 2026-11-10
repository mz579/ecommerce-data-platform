import { NavLink } from 'react-router-dom'
import { useDateRange } from '../state/DateRangeContext'
import {
  IconAnalyze,
  IconItems,
  IconModel,
  IconOverview,
  IconQuality,
  IconRetention,
  NestMark,
} from './Icons'
import { StatusPill } from './StatusPill'

const NAV_ITEMS = [
  { to: '/', label: '总览', en: 'Overview', icon: IconOverview, end: true },
  { to: '/items', label: '商品', en: 'Items', icon: IconItems, end: false },
  { to: '/retention', label: '留存', en: 'Retention', icon: IconRetention, end: false },
  { to: '/quality', label: '质量', en: 'Quality', icon: IconQuality, end: false },
  { to: '/analyze', label: '分析', en: 'Analyze', icon: IconAnalyze, end: false },
  { to: '/model', label: '模型', en: 'Model', icon: IconModel, end: true },
]

export function Sidebar() {
  const { meta } = useDateRange()

  return (
    <aside className="sidebar">
      <div className="sidebar-brand">
        <span className="brand-mark" data-magpie-nest>
          <NestMark />
        </span>
        <div className="brand-copy">
          <div className="brand-name">电商数仓</div>
          <div className="brand-sub">DATA PLATFORM</div>
        </div>
      </div>

      <nav className="sidebar-nav" aria-label="主导航">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => (isActive ? 'nav-item active' : 'nav-item')}
              title={item.label}
            >
              <span className="nav-icon">
                <Icon size={18} />
              </span>
              <span className="nav-copy">
                <span className="nav-label">{item.label}</span>
                <span className="nav-en">{item.en}</span>
              </span>
            </NavLink>
          )
        })}
      </nav>

      <div className="sidebar-foot">
        <div className="foot-row">
          <span>最新门禁</span>
          {meta?.latest_quality ? <StatusPill status={meta.latest_quality.status} /> : <span>—</span>}
        </div>
        <div className="foot-row">
          <span>最近发布</span>
          <span className="foot-value">
            {meta?.latest_publish ? `#${meta.latest_publish.publish_id}` : '—'}
          </span>
        </div>
        <div className="foot-row">
          <span>数据截至</span>
          <span className="foot-value num">{meta?.date_max ?? '—'}</span>
        </div>
      </div>
    </aside>
  )
}
