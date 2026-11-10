import { lazy, Suspense } from 'react'
import { Link, Route, Routes } from 'react-router-dom'
import { PageSkeleton } from './components/Skeleton'

const OverviewPage = lazy(() => import('./pages/Overview'))
const ItemsPage = lazy(() => import('./pages/Items'))
const RetentionPage = lazy(() => import('./pages/Retention'))
const RealtimePage = lazy(() => import('./pages/Realtime'))
const QualityPage = lazy(() => import('./pages/Quality'))
const AnalyzePage = lazy(() => import('./pages/Analyze'))
const AnalyzeJobPage = lazy(() => import('./pages/AnalyzeJob'))
const AnalyzeResultPage = lazy(() => import('./pages/AnalyzeResult'))
const DataModelPage = lazy(() => import('./pages/DataModel'))

function NotFoundPage() {
  return (
    <div className="card card-pad not-found">
      <h2 className="card-title">页面不存在</h2>
      <p className="card-sub">请从左侧导航选择要查看的页面。</p>
      <Link className="btn btn-primary" to="/">
        返回总览
      </Link>
    </div>
  )
}

export function AppRoutes() {
  return (
    <Suspense fallback={<PageSkeleton />}>
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/items" element={<ItemsPage />} />
        <Route path="/retention" element={<RetentionPage />} />
        <Route path="/realtime" element={<RealtimePage />} />
        <Route path="/quality" element={<QualityPage />} />
        <Route path="/analyze" element={<AnalyzePage />} />
        <Route path="/analyze/:jobId" element={<AnalyzeJobPage />} />
        <Route path="/analyze/:jobId/result" element={<AnalyzeResultPage />} />
        <Route path="/model" element={<DataModelPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </Suspense>
  )
}
