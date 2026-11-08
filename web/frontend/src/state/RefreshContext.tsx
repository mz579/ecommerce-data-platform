import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'

interface RefreshValue {
  token: number
  refresh: () => void
}

const RefreshContext = createContext<RefreshValue>({ token: 0, refresh: () => undefined })

export function RefreshProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState(0)
  const refresh = useCallback(() => setToken((value) => value + 1), [])
  const value = useMemo(() => ({ token, refresh }), [token, refresh])
  return <RefreshContext.Provider value={value}>{children}</RefreshContext.Provider>
}

export function useRefreshToken(): number {
  return useContext(RefreshContext).token
}

export function useRefresh(): () => void {
  return useContext(RefreshContext).refresh
}
