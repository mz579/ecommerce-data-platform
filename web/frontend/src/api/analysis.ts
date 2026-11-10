import { ApiError } from './client'
import type {
  AnalysisJob,
  AnalysisListResponse,
  AnalysisResult,
  StartAnalysisPayload,
} from './analysisTypes'

const BASE = '/api/analysis'

async function errorFrom(response: Response): Promise<ApiError> {
  let detail = ''
  try {
    const body = (await response.json()) as { detail?: unknown; message?: unknown }
    const value = body.detail ?? body.message
    if (typeof value === 'string') detail = value
  } catch {
    /* 非 JSON 错误体 */
  }
  return new ApiError(
    `接口返回 ${response.status}${detail ? `：${detail}` : ''}`,
    response.status,
  )
}

async function getJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, {
    signal,
    headers: { Accept: 'application/json' },
    credentials: 'same-origin',
  })
  if (!response.ok) throw await errorFrom(response)
  return (await response.json()) as T
}

export function listAnalysisJobs(limit = 6, signal?: AbortSignal): Promise<AnalysisListResponse> {
  return getJson<AnalysisListResponse>(`${BASE}/jobs?limit=${limit}`, signal)
}

export function getAnalysisJob(jobId: string, signal?: AbortSignal): Promise<AnalysisJob> {
  return getJson<AnalysisJob>(`${BASE}/jobs/${jobId}`, signal)
}

export function getAnalysisResult(jobId: string, signal?: AbortSignal): Promise<AnalysisResult> {
  return getJson<AnalysisResult>(`${BASE}/jobs/${jobId}/result`, signal)
}

export async function createAnalysisSample(): Promise<AnalysisJob> {
  const response = await fetch(`${BASE}/sample`, {
    method: 'POST',
    headers: { Accept: 'application/json' },
    credentials: 'same-origin',
  })
  if (!response.ok) throw await errorFrom(response)
  return (await response.json()) as AnalysisJob
}

export async function startAnalysisJob(
  jobId: string,
  payload: StartAnalysisPayload,
): Promise<AnalysisJob> {
  const response = await fetch(`${BASE}/jobs/${jobId}/start`, {
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
    credentials: 'same-origin',
    body: JSON.stringify(payload),
  })
  if (!response.ok) throw await errorFrom(response)
  return (await response.json()) as AnalysisJob
}

export async function deleteAnalysisJob(
  jobId: string,
): Promise<{ status: 'cancelling' } | null> {
  const response = await fetch(`${BASE}/jobs/${jobId}`, {
    method: 'DELETE',
    headers: { Accept: 'application/json' },
    credentials: 'same-origin',
  })
  if (response.status === 204) return null
  if (response.status === 202) return { status: 'cancelling' }
  throw await errorFrom(response)
}

/** 用 XHR 以便展示上传进度（fetch 不暴露 upload progress）。 */
export function uploadAnalysisFile(
  file: File,
  onProgress?: (ratio: number) => void,
): Promise<AnalysisJob> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open('POST', `${BASE}/uploads`)
    xhr.responseType = 'json'
    xhr.timeout = 10 * 60 * 1000

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable && onProgress) onProgress(event.loaded / event.total)
    }

    xhr.onload = () => {
      const body = xhr.response as AnalysisJob | { detail?: string } | null
      if (xhr.status >= 200 && xhr.status < 300 && body) {
        resolve(body as AnalysisJob)
        return
      }
      const detail =
        body && typeof body === 'object' && 'detail' in body && typeof body.detail === 'string'
          ? `：${body.detail}`
          : ''
      reject(new ApiError(`上传失败（${xhr.status}）${detail}`, xhr.status))
    }
    xhr.onerror = () => reject(new ApiError('无法连接后端接口，请确认服务已启动'))
    xhr.ontimeout = () => reject(new ApiError('上传超时，请重试'))

    const form = new FormData()
    form.append('file', file)
    xhr.send(form)
  })
}
