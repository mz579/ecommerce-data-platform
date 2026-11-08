type Tone = 'good' | 'warning' | 'serious' | 'critical' | 'neutral'

interface StatusMeta {
  tone: Tone
  label?: string
  title?: string
}

const STATUS_META: Record<string, StatusMeta> = {
  PASS: { tone: 'good', title: '通过' },
  FAIL: { tone: 'critical', title: '失败' },
  SUCCESS: { tone: 'good', title: '成功' },
  FAILED: { tone: 'critical', title: '失败' },
  RUNNING: { tone: 'warning', title: '运行中' },
  MATCH: { tone: 'good', title: '离线与实时一致' },
  DIFF: { tone: 'critical', title: '离线与实时存在差异' },
  SKIP: { tone: 'neutral', title: '跳过对账' },
  PUBLISHED: { tone: 'good', label: '已发布', title: 'PUBLISHED' },
  SKIPPED: { tone: 'neutral', label: '已跳过', title: 'SKIPPED' },
  FINAL: { tone: 'good', label: '已定稿', title: 'FINAL' },
  PARTIAL: { tone: 'warning', label: '部分', title: '窗口不完整' },
  FULL: { tone: 'good', label: '全量', title: 'FULL' },
  DATES: { tone: 'neutral', label: '按日', title: 'DATES' },
  PENDING: { tone: 'warning', label: '待定', title: 'PENDING' },
  累积中: { tone: 'warning', title: '窗口尚未定稿' },
}

function normalize(status: string | null | undefined): StatusMeta {
  if (!status) return { tone: 'neutral', label: '—', title: '无状态' }
  const meta = STATUS_META[status]
  if (meta) return meta
  return { tone: 'neutral', label: status, title: status }
}

interface StatusPillProps {
  status: string | null | undefined
  /** 同时显示中文说明（如 PASS 通过） */
  withDescription?: boolean
}

/** 状态永远由「圆点 + 文字」共同表达，不依赖颜色单独表意。 */
export function StatusPill({ status, withDescription = false }: StatusPillProps) {
  const meta = normalize(status)
  const text = meta.label ?? status ?? '—'
  const description = meta.title && meta.title !== text ? meta.title : undefined
  return (
    <span className={`status status-${meta.tone}`} title={description ?? text}>
      <span className="status-dot" aria-hidden="true" />
      <span className="status-text">{text}</span>
      {withDescription && description ? (
        <span className="status-desc">{description}</span>
      ) : null}
    </span>
  )
}
