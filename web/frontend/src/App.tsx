import { BrowserRouter } from 'react-router-dom'
import { Sidebar } from './components/Sidebar'
import { TopBar } from './components/TopBar'
import { DateRangeProvider } from './state/DateRangeContext'
import { RefreshProvider } from './state/RefreshContext'
import { ThemeProvider } from './theme/ThemeContext'
import { AppRoutes } from './router'

export default function App() {
  return (
    <ThemeProvider>
      <RefreshProvider>
        <BrowserRouter>
          <DateRangeProvider>
            <div className="app-shell">
              <Sidebar />
              <div className="app-main">
                <TopBar />
                <main className="page-content">
                  <AppRoutes />
                </main>
              </div>
            </div>
          </DateRangeProvider>
        </BrowserRouter>
      </RefreshProvider>
    </ThemeProvider>
  )
}
