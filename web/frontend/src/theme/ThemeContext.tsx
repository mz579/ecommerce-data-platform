import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react'
import type { ThemeMode } from './tokens'

interface ThemeValue {
  mode: ThemeMode
  toggle: () => void
  setMode: (mode: ThemeMode) => void
}

const STORAGE_KEY = 'ecom-theme'

const ThemeContext = createContext<ThemeValue>({
  mode: 'light',
  toggle: () => undefined,
  setMode: () => undefined,
})

function readInitialMode(): ThemeMode {
  try {
    return localStorage.getItem(STORAGE_KEY) === 'dark' ? 'dark' : 'light'
  } catch {
    return 'light'
  }
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setMode] = useState<ThemeMode>(readInitialMode)

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', mode)
    document
      .querySelector('meta[name="theme-color"]')
      ?.setAttribute('content', mode === 'dark' ? '#0d0d0d' : '#f9f9f7')
    try {
      localStorage.setItem(STORAGE_KEY, mode)
    } catch {
      /* 忽略存储失败 */
    }
  }, [mode])

  const toggle = useCallback(() => {
    setMode((value) => (value === 'dark' ? 'light' : 'dark'))
  }, [])

  const value = useMemo(() => ({ mode, toggle, setMode }), [mode, toggle])
  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}

export function useThemeMode(): ThemeValue {
  return useContext(ThemeContext)
}
