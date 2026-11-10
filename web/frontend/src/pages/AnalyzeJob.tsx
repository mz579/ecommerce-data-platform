import { useCallback, useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { deleteAnalysisJob, getAnalysisJob, startAnalysisJob } from '../api/analysis'
import type {
  AnalysisField,
  AnalysisJob,
  AnalysisStatus,
  DetectedField,
  StartAnalysisPayload,
} from '../api/analysisTypes'
import { ApiError } from '../api/client'
import { DataTable, type Column } from '../components/DataTable'
import { IconArrowRight, IconCheck, IconFile } from '../components/Icons'
import { navigateWithTransition } from '../utils/transition'
import {
  ALL_FIELDS,
  FIELD_HINTS,
  FIELD_LABELS,
  OPTIONAL_FIELDS,
  REQUIRED_FIELDS,
  STAGE_LABELS,
  STAGE_ORDER,
  STATUS_LABELS,
  formatBytes,
  formatDuration,
} from '../utils/analysis'
import { formatDateTime, formatInt } from '../utils/format'

type MappingState = Record<AnalysisField, string>

const EMPTY_MAPPING: MappingState = {
  user_id: '',
  item_id: '',
  event_type: '',
  event_time: '',
  transaction_id: '',
  category_id: '',
  amount: '',
  product_name: '',
}

const MAPPING_GROUPS: { key: string; title: string; hint: string; fields: AnalysisField[] }[] = [
  {
    key: 'required',
    title: '必须选的 4 项',
    hint: '没有这四列，分析无法开始。',
    fields: REQUIRED_FIELDS,
  },
  {
    key: 'optional',
    title: '可以不选的 4 项',
    hint: '文件里没有就保持「不使用」，只是少几个指标。',
    fields: OPTIONAL_FIELDS,
  },
]

/** 把样本里的事件值翻译成日常说法，原件仍保留在标签里方便对照。 */
function eventValueLabel(value: string): string {
  const key = value.trim().toLowerCase().replace(/[\s_-]+/g, '')
  if (['view', 'pv', 'pageview', 'browse', 'click', '浏览', '查看', '曝光'].includes(key)) {
    return '浏览'
  }
  if (['addtocart', 'cart', 'addcart', '加购', '加入购物车'].includes(key)) {
    return '加购'
  }
  if (
    ['transaction', 'purchase', 'order', 'pay', 'buy', '购买', '下单', '支付', '成交'].includes(
      key,
    )
  ) {
    return '下单'
  }
  return '其他'
}

/** 下拉里的列名后面跟一个俗名，方便不知道英文列名的人判断。 */
function dtypeLabel(dtype: string): string {
  if (dtype === 'datetime') return '时间'
  if (dtype === 'int' || dtype === 'float') return '数字'
  if (dtype === 'empty') return '空列'
  return '文本'
}

function columnOptionLabel(column: string, columnLabels: Record<string, string>): string {
  const label = columnLabels[column]
  return label ? `${column} · ${label}` : column
}

function timezoneShortLabel(zone: string): string {
  if (zone === 'UTC') return '国际标准时间（UTC）'
  if (zone === 'Asia/Shanghai') return '北京时间（UTC+8）'
  return `本机时区（${zone}）`
}

const RUNNING_STATUSES: AnalysisStatus[] = [
  'received',
  'detecting',
  'queued',
  'parsing',
  'normalizing',
  'aggregating',
]

const TERMINAL_STATUSES: AnalysisStatus[] = ['done', 'failed', 'cancelled']

interface PreviewRow {
  index: number
  cells: string[]
}

function isRunningStatus(status: AnalysisStatus): boolean {
  return RUNNING_STATUSES.includes(status)
}

function delimiterLabel(value: string): string {
  if (value === '\t') return '制表符'
  if (value === ';') return '分号'
  if (value === '|') return '竖线'
  return '逗号'
}

export default function AnalyzeJobPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()

  const [job, setJob] = useState<AnalysisJob | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [mapping, setMapping] = useState<MappingState>({ ...EMPTY_MAPPING })
  const [timezone, setTimezone] = useState('UTC')
  const [adjusting, setAdjusting] = useState(false)
  const [forceMapping, setForceMapping] = useState(false)
  const [starting, setStarting] = useState(false)
  const [startError, setStartError] = useState<string | null>(null)
  const [cancelling, setCancelling] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [notice, setNotice] = useState<string | null>(null)
  const [remaining, setRemaining] = useState<number | null>(null)
  const [pollKey, setPollKey] = useState(0)

  const initRef = useRef<string | null>(null)
  const sawRunningRef = useRef(false)
  const navigatedRef = useRef(false)

  // 切换任务时清空上一份任务的本地状态。
  useEffect(() => {
    initRef.current = null
    setJob(null)
    setLoading(true)
    setError(null)
    setAdjusting(false)
    setForceMapping(false)
    setStarting(false)
    setCancelling(false)
    setDeleting(false)
    setNotice(null)
    setStartError(null)
    setRemaining(null)
    sawRunningRef.current = false
    navigatedRef.current = false
  }, [jobId])

  // 每秒轮询直到进入终态；404 直接停止。
  useEffect(() => {
    if (!jobId) return
    let active = true
    let timer: number | undefined

    const tick = async () => {
      try {
        const data = await getAnalysisJob(jobId)
        if (!active) return
        setJob(data)
        setLoading(false)
        setError(null)
        if (!TERMINAL_STATUSES.includes(data.status)) {
          timer = window.setTimeout(tick, 1000)
        }
      } catch (caught) {
        if (!active) return
        setLoading(false)
        if (caught instanceof ApiError && caught.status === 404) {
          setError('任务不存在或已被清理')
          return
        }
        setError(caught instanceof Error ? caught.message : '任务状态加载失败')
        timer = window.setTimeout(tick, 3000)
      }
    }

    tick()
    return () => {
      active = false
      if (timer) window.clearTimeout(timer)
    }
  }, [jobId, pollKey])

  // 初次拿到预览后，用检测结果预填映射（不覆盖用户后续修改）。
  useEffect(() => {
    if (!job?.preview) return
    if (initRef.current === job.job_id) return
    initRef.current = job.job_id

    const detected = job.preview.detected
    const next = { ...EMPTY_MAPPING }
    for (const field of ALL_FIELDS) {
      const saved = job.mapping?.[field]
      next[field] = typeof saved === 'string' && saved ? saved : (detected[field]?.column ?? '')
    }
    setMapping(next)
    const savedTz = job.mapping?.timezone
    if (typeof savedTz === 'string' && savedTz) setTimezone(savedTz)
  }, [job])

  // 服务端 ETA 每次轮询都会刷新；本地每秒递减，保证倒计时连续。
  useEffect(() => {
    if (!job || TERMINAL_STATUSES.includes(job.status)) {
      setRemaining(null)
      return
    }
    const eta = job.progress.eta_seconds
    setRemaining(typeof eta === 'number' ? eta : null)
    const timer = window.setInterval(() => {
      setRemaining((value) => (value === null ? null : Math.max(0, value - 1)))
    }, 1000)
    return () => window.clearInterval(timer)
  }, [job?.progress.eta_seconds, job?.status, job?.updated_at])

  useEffect(() => {
    if (job && TERMINAL_STATUSES.includes(job.status)) setCancelling(false)
  }, [job])

  // 完成：只有亲眼看到任务运行过，才在结果页播放「案卷展开」；
  // 直接打开一个已完成任务时安静跳转。导航本身用 View Transitions 软化。
  useEffect(() => {
    if (!job || !jobId) return
    if (isRunningStatus(job.status)) {
      sawRunningRef.current = true
      return
    }
    if (job.status !== 'done' || navigatedRef.current) return
    navigatedRef.current = true
    const reveal = sawRunningRef.current
    if (reveal) {
      try {
        sessionStorage.setItem(`analysis-reveal:${jobId}`, '1')
      } catch {
        /* 隐私模式下 sessionStorage 可能不可用，退回 location.state */
      }
    }
    navigateWithTransition(navigate, `/analyze/${jobId}/result`, {
      replace: true,
      state: reveal ? { reveal: true } : null,
    })
  }, [job, jobId, navigate])

  const preview = job?.preview ?? null
  const columnLabels: Record<string, string> = {}
  if (preview) {
    for (const column of preview.columns) {
      const field = ALL_FIELDS.find(
        (candidate) => preview.detected[candidate]?.column === column.name,
      )
      columnLabels[column.name] = field ? FIELD_LABELS[field] : dtypeLabel(column.dtype)
    }
  }

  // 预览刚到时映射 state 还没写入，先用检测结果顶一帧，避免摘要闪成“必须选择”。
  const mappingReady = Boolean(job && initRef.current === job.job_id)
  const mappingView: MappingState = { ...mapping }
  if (preview && !mappingReady) {
    for (const field of ALL_FIELDS) {
      const saved = job?.mapping?.[field]
      mappingView[field] =
        typeof saved === 'string' && saved ? saved : (preview.detected[field]?.column ?? '')
    }
  }
  const columnNames = preview ? preview.columns.map((column) => column.name) : []
  const missingOptional = OPTIONAL_FIELDS.filter((field) => !mappingView[field])

  const isRunning = job ? isRunningStatus(job.status) : false
  const isFailure = job ? job.status === 'failed' || job.status === 'cancelled' : false
  const showMapping = Boolean(
    preview &&
      (job?.status === 'awaiting_mapping' ||
        (forceMapping && (job?.status === 'failed' || job?.status === 'cancelled'))),
  )

  const duplicates: string[] = []
  for (const field of ALL_FIELDS) {
    const column = mapping[field]
    if (!column) continue
    const count = ALL_FIELDS.filter((other) => mapping[other] === column).length
    if (count > 1 && !duplicates.includes(column)) duplicates.push(column)
  }

  const weakFields = preview
    ? ALL_FIELDS.filter((field) => {
        const detected = preview.detected[field]
        return (
          Boolean(mapping[field]) &&
          mapping[field] === detected?.column &&
          (detected?.confidence ?? 0) < 0.4
        )
      })
    : []

  const canStart =
    Boolean(preview) &&
    REQUIRED_FIELDS.every((field) => Boolean(mapping[field])) &&
    duplicates.length === 0

  const handleMappingChange = useCallback((field: AnalysisField, column: string) => {
    setMapping((current) => ({ ...current, [field]: column }))
  }, [])

  const handleStart = useCallback(async () => {
    if (!job || !preview || !canStart) return
    setStarting(true)
    setStartError(null)
    setNotice(null)
    const payloadMapping: Record<string, string | null> = {}
    for (const field of ALL_FIELDS) payloadMapping[field] = mapping[field] || null
    const payload: StartAnalysisPayload = {
      mapping: payloadMapping,
      timezone,
      has_header: preview.has_header,
    }
    try {
      const updated = await startAnalysisJob(job.job_id, payload)
      setJob(updated)
      setForceMapping(false)
      setPollKey((key) => key + 1)
    } catch (caught) {
      setStartError(caught instanceof Error ? caught.message : '启动分析失败')
    } finally {
      setStarting(false)
    }
  }, [job, preview, canStart, mapping, timezone])

  const handleCancel = useCallback(async () => {
    if (!job || !jobId) return
    setCancelling(true)
    setNotice(null)
    try {
      const response = await deleteAnalysisJob(job.job_id)
      if (response === null) {
        navigate('/analyze')
        return
      }
      setNotice('已请求取消，正在等待当前阶段停止…')
      setPollKey((key) => key + 1)
    } catch (caught) {
      setCancelling(false)
      setStartError(caught instanceof Error ? caught.message : '取消失败')
    }
  }, [job, jobId, navigate])

  const handleDelete = useCallback(async () => {
    if (!job) return
    if (!window.confirm(`删除「${job.filename}」的分析任务？`)) return
    setDeleting(true)
    try {
      await deleteAnalysisJob(job.job_id)
      navigate('/analyze')
    } catch (caught) {
      setDeleting(false)
      setStartError(caught instanceof Error ? caught.message : '删除失败')
    }
  }, [job, navigate])

  const retryMapping = useCallback(() => {
    setStartError(null)
    setForceMapping(true)
    initRef.current = null
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [])

  if (!jobId) {
    return (
      <div className="page-stack">
        <div className="card card-pad">
          <p>缺少任务 ID。</p>
          <Link className="link-button" to="/analyze">
            返回分析首页
          </Link>
        </div>
      </div>
    )
  }

  if (loading && !job) {
    return (
      <div className="page-stack analyze-job">
        <div className="card card-pad analysis-loading">
          <span className="skeleton" style={{ width: '38%', height: 18 }} />
          <span className="skeleton" style={{ width: '62%', height: 11 }} />
          <span className="skeleton" style={{ width: '100%', height: 280 }} />
        </div>
      </div>
    )
  }

  if (!job) {
    return (
      <div className="page-stack">
        <div className="card card-pad">
          <p className="eyebrow">CASE FILE</p>
          <h1 className="analysis-job-title">任务不可用</h1>
          <p className="inline-error">{error ?? '未能读取任务'}</p>
          <Link className="btn btn-primary" to="/analyze">
            返回分析首页
          </Link>
        </div>
      </div>
    )
  }

  const previewRows: PreviewRow[] = (preview?.sample_rows ?? []).map((cells, index) => ({
    index,
    cells,
  }))
  const previewColumns: Column<PreviewRow>[] = preview
    ? [
        {
          key: '__row',
          title: '#',
          width: 48,
          render: (_row, index) => index + 1,
        },
        ...preview.columns.map((column, index) => ({
          key: `col-${index}`,
          title: column.name,
          render: (row: PreviewRow) => row.cells[index] || '—',
        })),
      ]
    : []

  const containerLabel = preview
    ? `${preview.kind === 'xlsx' ? 'Excel' : 'CSV'}${preview.container === 'zip' ? ' · ZIP' : ''}`
    : job.file_type.toUpperCase()

  const statusBadge = (
    <span className={`analysis-status analysis-status--${job.status}`}>
      {STATUS_LABELS[job.status]}
    </span>
  )

  if (showMapping && preview) {
    return (
      <>
        <div className="page-stack analyze-job">
          <header className="analysis-job-head">
            <div>
              <p className="eyebrow">CASE FILE · {job.job_id.slice(0, 8).toUpperCase()}</p>
              <h1 className="analysis-job-title">{job.filename}</h1>
              <p className="analysis-job-meta">
                {containerLabel} · {formatBytes(job.size_bytes)} · 约{' '}
                {formatInt(preview.rows_estimated)} 行 · {job.sample ? '示例数据' : '用户上传'}
              </p>
            </div>
            <div className="analysis-job-head__side">
              {statusBadge}
              <Link className="btn btn-ghost" to="/analyze">
                返回上传
              </Link>
            </div>
          </header>

          <div className="analysis-job-grid">
            <section className="card analysis-panel mapping-panel" aria-labelledby="mapping-title">
              <div className="analysis-panel__head">
                <div>
                  <p className="eyebrow">COLUMN GUIDE</p>
                  <h2 className="analysis-panel__title" id="mapping-title">
                    这些列是什么意思？
                  </h2>
                </div>
                <span className="analysis-panel__step">第 1 步 / 共 2 步</span>
              </div>

              {adjusting ? (
                <>
                  <p className="analysis-panel__lead">
                    把每一项改成你文件里对应的列。下拉里每一项后面都标了大概的用途，
                    例如 visitorid · 用户编号。带 * 的 4 项必须选；其余 4 项文件里没有就跳过，
                    分析会少几个指标，但不会失败。
                  </p>

                  {weakFields.length > 0 ? (
                    <p className="analysis-inline-note">
                      有 {weakFields.length} 项系统拿不太准（
                      {weakFields.map((field) => FIELD_LABELS[field]).join('、')}），
                      建议对照下方的数据预览核对一下。
                    </p>
                  ) : null}

                  {MAPPING_GROUPS.map((group) => (
                    <div className="mapping-group" key={group.key}>
                      <div className="mapping-group__head">
                        <p className="mapping-group__title">{group.title}</p>
                        <p className="mapping-group__hint">{group.hint}</p>
                      </div>
                      <div className="mapping-grid">
                        {group.fields.map((field) => (
                          <MappingField
                            key={field}
                            field={field}
                            value={mappingView[field]}
                            columns={columnNames}
                            columnLabels={columnLabels}
                            detected={preview.detected[field]}
                            duplicate={duplicates.includes(mappingView[field])}
                            onChange={handleMappingChange}
                          />
                        ))}
                      </div>
                    </div>
                  ))}

                  <div className="mapping-options">
                    <label className="field">
                      <span>时间按哪个时区理解</span>
                      <select
                        value={timezone}
                        onChange={(event) => setTimezone(event.target.value)}
                      >
                        <TimezoneOptions />
                      </select>
                      <small>文件里的时间没写时区时，按这里算；已经带时区的时间不受影响。</small>
                    </label>
                    <div className="mapping-fact">
                      <span>第一行是列名吗</span>
                      <strong>{preview.has_header ? '是' : '不是'}</strong>
                      <small>系统根据文件内容自动判断</small>
                    </div>
                  </div>

                  {preview.event_values.length > 0 ? (
                    <div className="event-values">
                      <p className="event-values__label">文件里出现的行为</p>
                      <div className="event-values__chips">
                        {preview.event_values.map((value) => {
                          const label = eventValueLabel(value)
                          return (
                            <span className="event-chip" key={value}>
                              {value}
                              {label !== value ? (
                                <span className="event-chip__cn"> · {label}</span>
                              ) : null}
                            </span>
                          )
                        })}
                      </div>
                      <p className="event-values__hint">
                        「浏览」「加购」「下单」会分开统计，其他行为统一算作「其他」。
                        这里只看了文件前 200 行，后面出现的行为也会自动识别。
                      </p>
                    </div>
                  ) : null}
                </>
              ) : (
                <>
                  <p className="analysis-panel__lead">
                    系统已经识别好每一列的含义。看一眼，确认无误就点「开始分析」；
                    需要修改时点「调整字段」。
                  </p>

                  <div className="mapping-summary">
                    {REQUIRED_FIELDS.map((field) => (
                      <MappingSummaryRow
                        key={field}
                        field={field}
                        value={mappingView[field]}
                        columns={columnNames}
                        columnLabels={columnLabels}
                        detected={preview.detected[field]}
                        onChange={handleMappingChange}
                      />
                    ))}
                    {OPTIONAL_FIELDS.filter((field) => Boolean(mappingView[field])).map(
                      (field) => (
                        <MappingSummaryRow
                          key={field}
                          field={field}
                          value={mappingView[field]}
                          columns={columnNames}
                          columnLabels={columnLabels}
                          detected={preview.detected[field]}
                          onChange={handleMappingChange}
                        />
                      ),
                    )}
                  </div>

                  {missingOptional.length > 0 ? (
                    <p className="mapping-summary__note">
                      文件里没有{missingOptional.map((field) => FIELD_LABELS[field]).join('、')}
                      ，将跳过对应指标。
                    </p>
                  ) : null}

                  <p className="mapping-summary__meta">
                    时间按{timezoneShortLabel(timezone)}理解 ·{' '}
                    {preview.has_header ? '第一行是列名' : '第一行是数据，没有列名'}
                    <button
                      type="button"
                      className="mapping-summary__link"
                      onClick={() => setAdjusting(true)}
                    >
                      更改
                    </button>
                  </p>

                  {preview.event_values.length > 0 ? (
                    <p className="mapping-summary__events">
                      文件里的行为：
                      {preview.event_values
                        .map((value) => {
                          const label = eventValueLabel(value)
                          return label !== value ? `${value}（${label}）` : value
                        })
                        .join('、')}
                    </p>
                  ) : null}
                </>
              )}

              {duplicates.length > 0 ? (
                <p className="inline-error">
                  有一列被重复选择了：{duplicates.join('、')}。请为每一项选不同的列。
                </p>
              ) : null}
              {startError ? <p className="inline-error">{startError}</p> : null}

              <div className="analysis-actions">
                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={handleStart}
                  disabled={!canStart || starting}
                >
                  {starting
                    ? '正在启动…'
                    : job.status === 'failed' || job.status === 'cancelled'
                      ? '重新开始分析'
                      : '开始分析'}
                  <IconArrowRight size={15} />
                </button>
                <button
                  type="button"
                  className="btn btn-ghost"
                  onClick={() => setAdjusting((value) => !value)}
                >
                  {adjusting ? '收起调整' : '调整字段'}
                </button>
                <button
                  type="button"
                  className="btn btn-ghost"
                  onClick={handleDelete}
                  disabled={deleting}
                >
                  {deleting ? '正在删除…' : '删除任务'}
                </button>
              </div>
            </section>

            <aside className="analysis-side">
              <section className="card analysis-panel">
                <div className="analysis-panel__head">
                  <div>
                    <p className="eyebrow">SOURCE</p>
                    <h2 className="analysis-panel__title">数据包</h2>
                  </div>
                  <IconFile size={18} />
                </div>
                <dl className="file-facts">
                  <div>
                    <dt>文件格式</dt>
                    <dd>{containerLabel}</dd>
                  </div>
                  {preview.selected_member ? (
                    <div>
                      <dt>压缩包内文件</dt>
                      <dd className="file-facts__wide">{preview.selected_member}</dd>
                    </div>
                  ) : null}
                  {preview.sheet ? (
                    <div>
                      <dt>工作表</dt>
                      <dd>{preview.sheet}</dd>
                    </div>
                  ) : null}
                  <div>
                    <dt>文字编码</dt>
                    <dd>{preview.encoding.toUpperCase()}</dd>
                  </div>
                  <div>
                    <dt>列分隔符</dt>
                    <dd>{delimiterLabel(preview.delimiter)}</dd>
                  </div>
                  <div>
                    <dt>大约行数</dt>
                    <dd>{formatInt(preview.rows_estimated)}</dd>
                  </div>
                  <div>
                    <dt>创建时间</dt>
                    <dd>{formatDateTime(job.created_at)}</dd>
                  </div>
                </dl>
                <p className="analysis-side__note">
                  原始文件和分析结果只保存在这台电脑上，7 天后自动清理。
                </p>
              </section>

              <section className="card analysis-panel">
                <div className="analysis-panel__head">
                  <div>
                    <p className="eyebrow">PIPELINE</p>
                    <h2 className="analysis-panel__title">分析流程</h2>
                  </div>
                  <span className="analysis-panel__step">第 2 步 / 共 2 步</span>
                </div>
                <StageList status={job.status} />
              </section>
            </aside>
          </div>

          <section className="card analysis-panel" aria-labelledby="preview-title">
            <div className="analysis-panel__head">
              <div>
                <p className="eyebrow">PREVIEW</p>
                <h2 className="analysis-panel__title" id="preview-title">
                  数据预览
                </h2>
              </div>
              <span className="analysis-panel__step">前 {previewRows.length} 行</span>
            </div>
            <DataTable
              columns={previewColumns}
              rows={previewRows}
              rowKey={(row) => row.index}
              caption="上传文件预览"
              emptyText="没有可预览的行"
            />
          </section>
        </div>
      </>
    )
  }

  if (isFailure) {
    const cancelled = job.status === 'cancelled'
    return (
      <div className="page-stack analyze-job">
        <header className="analysis-job-head">
          <div>
            <p className="eyebrow">CASE FILE · {job.job_id.slice(0, 8).toUpperCase()}</p>
            <h1 className="analysis-job-title">{job.filename}</h1>
            <p className="analysis-job-meta">
              {containerLabel} · {formatBytes(job.size_bytes)} · {job.sample ? '示例数据' : '用户上传'}
            </p>
          </div>
          <div className="analysis-job-head__side">
            {statusBadge}
            <Link className="btn btn-ghost" to="/analyze">
              返回上传
            </Link>
          </div>
        </header>

        <section className="card analysis-panel analysis-result-error">
          <p className="eyebrow">{cancelled ? 'ANALYSIS CANCELLED' : 'ANALYSIS FAILED'}</p>
          <h2>{cancelled ? '这次分析已取消' : '这次分析没有完成'}</h2>
          <p className="analysis-result-error__message">
            {cancelled
              ? '任务已经停止，原始文件仍在本机任务目录中，可以调整映射后重新分析。'
              : (job.error ?? '请检查文件格式或字段映射后重试。')}
          </p>
          {job.progress.message ? (
            <p className="muted">最后阶段：{job.progress.message}</p>
          ) : null}
          {startError ? <p className="inline-error">{startError}</p> : null}
          <div className="analysis-actions">
            {preview ? (
              <button type="button" className="btn btn-primary" onClick={retryMapping}>
                {cancelled ? '重新分析' : '调整映射并重试'}
                <IconArrowRight size={15} />
              </button>
            ) : (
              <Link className="btn btn-primary" to="/analyze">
                重新上传
              </Link>
            )}
            <button
              type="button"
              className="btn btn-ghost"
              onClick={handleDelete}
              disabled={deleting}
            >
              {deleting ? '正在删除…' : '删除任务'}
            </button>
            <Link className="btn btn-ghost" to="/analyze">
              分析首页
            </Link>
          </div>
        </section>
      </div>
    )
  }

  const percent = Math.max(0, Math.min(100, job.progress.percent))
  const stageText = STAGE_LABELS[job.progress.stage] ?? job.progress.message

  return (
    <>
      <div className="page-stack analyze-job">
        <header className="analysis-job-head">
          <div>
            <p className="eyebrow">CASE FILE · {job.job_id.slice(0, 8).toUpperCase()}</p>
            <h1 className="analysis-job-title">{job.filename}</h1>
            <p className="analysis-job-meta">
              {containerLabel} · {formatBytes(job.size_bytes)}
              {preview ? ` · 约 ${formatInt(preview.rows_estimated)} 行` : ''} ·{' '}
              {job.sample ? '示例数据' : '用户上传'}
            </p>
          </div>
          <div className="analysis-job-head__side">
            {statusBadge}
            <Link className="btn btn-ghost" to="/analyze">
              返回上传
            </Link>
          </div>
        </header>

        <div className="analysis-progress-grid">
          <section className="card analysis-panel progress-panel" aria-live="polite">
            <div className="analysis-panel__head">
              <div>
                <p className="eyebrow">ANALYSIS IN PROGRESS</p>
                <h2 className="analysis-panel__title">{stageText}</h2>
              </div>
              <span className="analysis-panel__step">
                {job.status === 'done' ? '完成' : '单线程沙箱'}
              </span>
            </div>

            <div className="progress-number">
              <strong>{Math.round(percent)}</strong>
              <span>%</span>
            </div>
            <div
              className="progress-track progress-track--large"
              role="progressbar"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={Math.round(percent)}
              aria-label="分析进度"
            >
              <span style={{ width: `${percent}%` }} />
            </div>
            <p className="progress-panel__message">{job.progress.message}</p>

            <dl className="progress-stats">
              <div>
                <dt>已处理</dt>
                <dd>
                  {formatInt(job.progress.rows_processed)}
                  {job.progress.rows_total !== null
                    ? ` / ${formatInt(job.progress.rows_total)}`
                    : ''}
                </dd>
              </div>
              <div>
                <dt>预计剩余</dt>
                <dd>{remaining !== null ? formatDuration(remaining) : '正在估算'}</dd>
              </div>
              <div>
                <dt>已用时</dt>
                <dd>{formatDuration(job.progress.elapsed_seconds)}</dd>
              </div>
            </dl>

            {notice ? <p className="analysis-inline-note">{notice}</p> : null}
            {startError ? <p className="inline-error">{startError}</p> : null}

            <div className="analysis-actions">
              <button
                type="button"
                className="btn btn-ghost"
                onClick={handleCancel}
                disabled={cancelling || !isRunning}
              >
                {cancelling ? '正在取消…' : '取消分析'}
              </button>
              <span className="analysis-actions__hint">
                取消后原始文件会保留，可调整映射后重试。
              </span>
            </div>
          </section>

          <aside className="analysis-side">
            <section className="card analysis-panel">
              <div className="analysis-panel__head">
                <div>
                  <p className="eyebrow">PIPELINE</p>
                  <h2 className="analysis-panel__title">分析流程</h2>
                </div>
                {isRunning ? <span className="live-dot" aria-label="运行中" /> : null}
              </div>
              <StageList status={job.status} />
              <p className="analysis-side__note">
                解析和聚合在独立 DuckDB 中进行，不写入 MySQL 四层数仓，也不影响 139 天基准。
              </p>
            </section>

            <section className="card analysis-panel">
              <p className="eyebrow">SCOPE</p>
              <h2 className="analysis-panel__title">本次口径</h2>
              <ul className="analysis-bullets">
                <li>PV = view 事件行数；UV = 去重访客。</li>
                <li>漏斗按去重用户计算，不按事件次数。</li>
                <li>订单优先按订单号去重；没有订单号时按 purchase 事件计数。</li>
                <li>D1 留存只统计拥有完整次日窗口的日期。</li>
              </ul>
            </section>
          </aside>
        </div>
      </div>
    </>
  )
}

function TimezoneOptions() {
  const browser = Intl.DateTimeFormat().resolvedOptions().timeZone
  const zones = Array.from(new Set(['UTC', 'Asia/Shanghai', browser]))
  return (
    <>
      {zones.map((zone) => (
        <option value={zone} key={zone}>
          {timezoneLabel(zone, browser)}
        </option>
      ))}
    </>
  )
}

function timezoneLabel(zone: string, browser: string): string {
  if (zone === 'UTC') return 'UTC · 国际标准时间'
  if (zone === 'Asia/Shanghai') return '北京时间（UTC+8）'
  return zone === browser ? `${zone} · 本机时区` : zone
}

interface MappingFieldProps {
  field: AnalysisField
  value: string
  columns: string[]
  columnLabels: Record<string, string>
  detected: DetectedField | undefined
  duplicate: boolean
  onChange: (field: AnalysisField, value: string) => void
}

function MappingField({
  field,
  value,
  columns,
  columnLabels,
  detected,
  duplicate,
  onChange,
}: MappingFieldProps) {
  const required = REQUIRED_FIELDS.includes(field)
  const auto = Boolean(value) && detected?.column === value
  const confidence = detected?.confidence ?? 0
  const confident = confidence >= 0.9
  const tone = auto
    ? confident
      ? 'high'
      : 'medium'
    : value
      ? 'manual'
      : required
        ? 'required'
        : 'empty'
  const badge = auto
    ? confident
      ? '已匹配'
      : '请核对'
    : value
      ? '已选择'
      : required
        ? '必须选择'
        : '可跳过'
  const badgeTitle = auto
    ? `自动匹配度 ${Math.round(confidence * 100)}%${detected?.reason ? ` · ${detected.reason}` : ''}`
    : value
      ? '这一列是你手动选择的'
      : required
        ? '必须选择一列才能开始分析'
        : '文件里没有这一列时保持「不使用」即可'

  return (
    <div
      className={[
        'mapping-field',
        required ? 'is-required' : '',
        duplicate ? 'is-conflict' : '',
      ]
        .filter(Boolean)
        .join(' ')}
    >
      <div className="mapping-field__head">
        <label htmlFor={`mapping-${field}`}>
          {FIELD_LABELS[field]}
          {required ? <span className="mapping-field__required">*</span> : null}
        </label>
        <span className={`confidence confidence--${tone}`} title={badgeTitle}>
          {badge}
        </span>
      </div>
      <select
        id={`mapping-${field}`}
        value={value}
        onChange={(event) => onChange(field, event.target.value)}
      >
        <option value="">{required ? '— 请选择一列 —' : '— 不使用（跳过）—'}</option>
        {value && !columns.includes(value) ? (
          <option value={value}>{columnOptionLabel(value, columnLabels)}</option>
        ) : null}
        {columns.map((column) => (
          <option value={column} key={column}>
            {columnOptionLabel(column, columnLabels)}
          </option>
        ))}
      </select>
      <p className="mapping-field__hint">{FIELD_HINTS[field]}</p>
    </div>
  )
}

interface MappingSummaryRowProps {
  field: AnalysisField
  value: string
  columns: string[]
  columnLabels: Record<string, string>
  detected: DetectedField | undefined
  onChange: (field: AnalysisField, value: string) => void
}

/** 摘要行：默认只读；只有必填项缺失或不确定时才出现下拉。 */
function MappingSummaryRow({
  field,
  value,
  columns,
  columnLabels,
  detected,
  onChange,
}: MappingSummaryRowProps) {
  const required = REQUIRED_FIELDS.includes(field)
  const auto = Boolean(value) && detected?.column === value
  const confidence = detected?.confidence ?? 0
  const confident = auto && confidence >= 0.9
  const uncertain = auto && confidence < 0.9
  const needsSelect = required && (!value || uncertain)
  const tone = !value ? 'required' : confident ? 'high' : uncertain ? 'medium' : 'manual'
  const badge = !value ? '必须选择' : confident ? '已匹配' : uncertain ? '请核对' : '已选择'
  const badgeTitle = !value
    ? '必须选择一列才能开始分析'
    : confident
      ? `自动匹配度 ${Math.round(confidence * 100)}%`
      : uncertain
        ? `自动匹配度 ${Math.round(confidence * 100)}%，建议核对`
        : '这一列是你手动选择的'

  return (
    <div className={`mapping-summary__row${needsSelect ? ' has-select' : ''}`}>
      <span className="mapping-summary__field">
        {FIELD_LABELS[field]}
        {required ? <span className="mapping-field__required">*</span> : null}
      </span>
      {needsSelect ? (
        <select
          value={value}
          onChange={(event) => onChange(field, event.target.value)}
          aria-label={FIELD_LABELS[field]}
        >
          <option value="">— 请选择一列 —</option>
          {value && !columns.includes(value) ? (
            <option value={value}>{columnOptionLabel(value, columnLabels)}</option>
          ) : null}
          {columns.map((column) => (
            <option value={column} key={column}>
              {columnOptionLabel(column, columnLabels)}
            </option>
          ))}
        </select>
      ) : (
        <span className="mapping-summary__value" title={value}>
          {value}
          {!required ? <span className="mapping-summary__optional">可选</span> : null}
        </span>
      )}
      <span className={`confidence confidence--${tone}`} title={badgeTitle}>
        {badge}
      </span>
    </div>
  )
}

function StageList({ status }: { status: AnalysisStatus }) {
  const currentIndex = STAGE_ORDER.indexOf(status)
  return (
    <ol className="stage-list">
      {STAGE_ORDER.map((stage, index) => {
        const state =
          status === 'done'
            ? 'done'
            : index < currentIndex
              ? 'done'
              : index === currentIndex
                ? 'active'
                : 'pending'
        return (
          <li
            className={`stage-item stage-item--${state}`}
            key={stage}
            aria-current={state === 'active' ? 'step' : undefined}
          >
            <span className="stage-marker">
              {state === 'done' ? <IconCheck size={11} /> : <span>{index + 1}</span>}
            </span>
            <span className="stage-label">{STAGE_LABELS[stage]}</span>
          </li>
        )
      })}
    </ol>
  )
}
