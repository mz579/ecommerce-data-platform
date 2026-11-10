import { useCallback, useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import {
  createAnalysisSample,
  deleteAnalysisJob,
  listAnalysisJobs,
  uploadAnalysisFile,
} from '../api/analysis'
import type { AnalysisJob } from '../api/analysisTypes'
import { IconArrowRight, IconFile, IconSpark } from '../components/Icons'
import { UploadDropzone } from '../components/UploadDropzone'
import { formatDateTime, formatInt } from '../utils/format'
import { formatBytes, formatDuration, STATUS_LABELS } from '../utils/analysis'

const RUNNING: AnalysisJob['status'][] = [
  'received',
  'detecting',
  'queued',
  'parsing',
  'normalizing',
  'aggregating',
]

function jobHref(job: AnalysisJob): string {
  return job.status === 'done'
    ? `/analyze/${job.job_id}/result`
    : `/analyze/${job.job_id}`
}

export default function AnalyzePage() {
  const navigate = useNavigate()
  const [jobs, setJobs] = useState<AnalysisJob[]>([])
  const [jobsLoading, setJobsLoading] = useState(true)
  const [jobsError, setJobsError] = useState<string | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState(0)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [sampleLoading, setSampleLoading] = useState(false)

  const loadJobs = useCallback(() => {
    setJobsLoading(true)
    setJobsError(null)
    const controller = new AbortController()
    listAnalysisJobs(8, controller.signal)
      .then((data) => setJobs(data.jobs))
      .catch((error: unknown) => {
        if (controller.signal.aborted) return
        setJobsError(error instanceof Error ? error.message : '最近任务加载失败')
      })
      .finally(() => {
        if (!controller.signal.aborted) setJobsLoading(false)
      })
    return () => controller.abort()
  }, [])

  useEffect(() => loadJobs(), [loadJobs])

  const handleFile = useCallback(
    async (file: File) => {
      setUploadError(null)
      setUploadProgress(0)
      setUploading(true)
      try {
        const job = await uploadAnalysisFile(file, setUploadProgress)
        navigate(`/analyze/${job.job_id}`)
      } catch (error: unknown) {
        setUploadError(error instanceof Error ? error.message : '上传失败，请重试')
      } finally {
        setUploading(false)
      }
    },
    [navigate],
  )

  const handleSample = useCallback(async () => {
    setUploadError(null)
    setSampleLoading(true)
    try {
      const job = await createAnalysisSample()
      navigate(`/analyze/${job.job_id}`)
    } catch (error: unknown) {
      setUploadError(error instanceof Error ? error.message : '示例任务创建失败')
    } finally {
      setSampleLoading(false)
    }
  }, [navigate])

  const handleDelete = useCallback(
    async (job: AnalysisJob) => {
      if (!window.confirm(`删除「${job.filename}」的分析任务？`)) return
      try {
        await deleteAnalysisJob(job.job_id)
        loadJobs()
      } catch (error: unknown) {
        window.alert(error instanceof Error ? error.message : '删除失败')
      }
    },
    [loadJobs],
  )

  return (
    <div className="page-stack analyze-page">
      <section className="analyze-hero" aria-labelledby="analyze-title">
        <div className="analyze-hero__panel analyze-hero__panel--dark">
          <div className="analyze-hero__bars" aria-hidden="true">
            {Array.from({ length: 12 }, (_, index) => (
              <i key={index} />
            ))}
          </div>
          <svg className="analyze-hero__figure" viewBox="0 0 260 560" aria-hidden="true">
            <g fill="#B3002E">
              <ellipse cx="130" cy="96" rx="56" ry="9" />
              <path d="M96 93c0-30 11-46 34-46s34 16 34 46z" />
              <path d="M117 100h26v24h-26z" />
              <path d="M130 120c-26 0-45 11-52 30l-14 96 6 314h120l6-314-14-96c-7-19-26-30-52-30z" />
            </g>
            <path d="M130 126l17 42-17 34-17-34z" fill="#D7D6D4" />
          </svg>
        </div>

        <div className="analyze-hero__panel analyze-hero__panel--accent" data-magpie-perch>
          <svg
            className="analyze-hero__figure analyze-hero__figure--right"
            viewBox="0 0 260 560"
            aria-hidden="true"
          >
            <g fill="#D7183F">
              <path d="M130 44c-27 0-43 18-43 45 0 11 2 20 7 27h72c5-7 7-16 7-27 0-27-16-45-43-45z" />
              <path d="M90 90h80c7 15 7 36-5 50H95c-12-14-12-35-5-50z" />
              <path d="M117 138h26v20h-26z" />
              <path d="M130 156c-25 0-44 11-50 29l-12 92 8 283h108l8-283-12-92c-6-18-25-29-50-29z" />
            </g>
            <path d="M130 162l16 40-16 32-16-32z" fill="#08080A" />
          </svg>
        </div>

        <div className="analyze-hero__title" id="analyze-title">
          <p className="analyze-hero__latin">
            <span>DATA</span>
            <span>CASE FILE</span>
          </p>
          <p className="analyze-hero__cn">数据档案</p>
          <p className="analyze-hero__sub">上传 · 识别 · 分析 · 结果</p>
        </div>
      </section>

      <section className="analyze-intake" aria-labelledby="intake-title">
        <div className="card analyze-intake__main">
          <div className="section-head">
            <div>
              <p className="eyebrow">UPLOAD / 上传</p>
              <h2 className="section-title" id="intake-title">
                拖入数据包
              </h2>
            </div>
            <span className="analyze-intake__formats">CSV / TSV / Excel / ZIP</span>
          </div>

          <UploadDropzone
            onFile={handleFile}
            uploading={uploading}
            progress={uploadProgress}
            error={uploadError}
          />

          <div className="analyze-intake__actions">
            <button
              type="button"
              className="btn btn-ghost analyze-intake__sample"
              onClick={handleSample}
              disabled={uploading || sampleLoading}
            >
              <IconSpark size={15} />
              {sampleLoading ? '正在准备示例…' : '使用示例数据'}
            </button>
            <span className="analyze-intake__privacy">仅保存在本机 · 7 天后自动清理</span>
          </div>
        </div>

        <aside className="card analyze-intake__notes">
          <p className="eyebrow">WHAT HAPPENS NEXT</p>
          <h3 className="analyze-intake__notes-title">分析流程</h3>
          <ol className="analyze-intake__steps">
            <li>
              <span className="step-no">01</span>
              <div>
                <strong>识别字段</strong>
                <p>按列名与样本值推断用户、商品、事件与时间列。</p>
              </div>
            </li>
            <li>
              <span className="step-no">02</span>
              <div>
                <strong>确认映射</strong>
                <p>必填四项可手动调整；缺失的可选字段自动降级。</p>
              </div>
            </li>
            <li>
              <span className="step-no">03</span>
              <div>
                <strong>规范化事件</strong>
                <p>view / add_to_cart / transaction，写入独立 DuckDB。</p>
              </div>
            </li>
            <li>
              <span className="step-no">04</span>
              <div>
                <strong>汇总结果</strong>
                <p>PV/UV、漏斗、留存、商品与数据质量一次生成。</p>
              </div>
            </li>
          </ol>
        </aside>
      </section>

      <section className="analyze-recent" aria-labelledby="recent-title">
        <div className="section-head">
          <div>
            <p className="eyebrow">RECENT CASES</p>
            <h2 className="section-title" id="recent-title">
              最近分析
            </h2>
          </div>
          <button type="button" className="btn btn-ghost" onClick={loadJobs} disabled={jobsLoading}>
            刷新列表
          </button>
        </div>

        {jobsError ? <p className="inline-error">{jobsError}</p> : null}
        {jobsLoading && jobs.length === 0 ? (
          <div className="recent-grid">
            {Array.from({ length: 3 }, (_, index) => (
              <div className="card recent-job is-skeleton" key={index}>
                <span className="skeleton" style={{ width: '60%', height: 14 }} />
                <span className="skeleton" style={{ width: '35%', height: 10 }} />
                <span className="skeleton" style={{ width: '80%', height: 10 }} />
              </div>
            ))}
          </div>
        ) : jobs.length === 0 ? (
          <div className="card card-pad analyze-empty">
            <IconFile size={22} />
            <div>
              <p>还没有分析任务。上传一个数据包，或先用示例数据走一遍。</p>
              <p className="muted">示例数据来自本项目 Retailrocket 数仓的 14 天事件，275,404 行。</p>
            </div>
          </div>
        ) : (
          <div className="recent-grid">
            {jobs.map((job) => (
              <article className="card recent-job" key={job.job_id}>
                <div className="recent-job__head">
                  <Link className="recent-job__link" to={jobHref(job)}>
                    <span className="recent-job__icon">
                      <IconFile size={18} />
                    </span>
                    <span className="recent-job__name">{job.filename}</span>
                  </Link>
                  <span className={`analysis-status analysis-status--${job.status}`}>
                    {STATUS_LABELS[job.status]}
                  </span>
                </div>
                <p className="recent-job__meta">
                  {formatBytes(job.size_bytes)} ·{' '}
                  {job.preview ? `${formatInt(job.preview.rows_estimated)} 行` : '等待识别'} ·{' '}
                  {formatDateTime(job.created_at)}
                </p>
                {RUNNING.includes(job.status) ? (
                  <div className="recent-job__progress">
                    <div className="progress-track">
                      <span style={{ width: `${job.progress.percent}%` }} />
                    </div>
                    <span>{job.progress.percent}%</span>
                  </div>
                ) : job.status === 'done' ? (
                  <p className="recent-job__done">
                    已完成 · 耗时 {formatDuration(job.elapsed_seconds)}
                  </p>
                ) : job.status === 'failed' ? (
                  <p className="recent-job__error">{job.error ?? '分析失败'}</p>
                ) : null}
                <div className="recent-job__foot">
                  <Link className="link-button" to={jobHref(job)}>
                    {job.status === 'done' ? '查看结果' : '打开任务'}
                    <IconArrowRight size={13} />
                  </Link>
                  <button
                    type="button"
                    className="link-button link-button--danger"
                    onClick={() => handleDelete(job)}
                  >
                    删除
                  </button>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>
    </div>
  )
}
