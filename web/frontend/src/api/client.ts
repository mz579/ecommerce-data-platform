import { mockApi } from './mock'

/** 默认构建访问同源 /api；npm run dev:mock 时读取 .env.mock 切换为本地 mock。 */
const USE_MOCK = import.meta.env.VITE_USE_MOCK === 'true'

export class ApiError extends Error {
  status?: number

  constructor(message: string, status?: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

export interface QueryParams {
  [key: string]: string | number | boolean | null | undefined
}

/** 拼装 /api URL，自动忽略空值。 */
export function apiUrl(path: string, params?: QueryParams): string {
  const base = path.startsWith('/api') ? path : `/api${path}`
  if (!params) return base
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    search.set(key, String(value))
  }
  const query = search.toString()
  return query ? `${base}?${query}` : base
}

export async function fetchApi<T>(url: string, signal?: AbortSignal): Promise<T> {
  if (USE_MOCK) {
    return mockApi<T>(url, signal)
  }

  let response: Response
  try {
    response = await fetch(url, {
      signal,
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new ApiError('无法连接后端接口，请确认服务已启动')
  }

  if (!response.ok) {
    let detail = ''
    try {
      const body = (await response.json()) as { detail?: unknown; message?: unknown }
      const value = body.detail ?? body.message
      if (typeof value === 'string') detail = value
    } catch {
      /* 非 JSON 错误体 */
    }
    throw new ApiError(
      `接口返回 ${response.status}${detail ? `：${detail}` : ''}`,
      response.status,
    )
  }

  try {
    return (await response.json()) as T
  } catch {
    throw new ApiError('接口响应不是合法 JSON')
  }
}

export function isMockMode(): boolean {
  return USE_MOCK
}
