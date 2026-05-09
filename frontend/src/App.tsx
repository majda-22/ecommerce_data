import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import './App.css'

type View = 'analytics' | 'pipeline' | 'orders' | 'inventory' | 'settings'
type Mode = 'realtime' | 'history'
type LoadState = 'live' | 'preview' | 'error'

type StatusRow = {
  order_status: string
  total_orders: number
  gold_updated_at?: string
}

type LatencyRow = {
  table_name: string
  avg_latency_seconds: number
  max_latency_seconds: number
  events_measured: number
  gold_updated_at?: string
}

type CategoryRow = {
  category: string
  total_events: number
}

type ReviewScoreRow = {
  review_score: number
}

type CdcEvent = {
  source_table: string
  operation: string
  kafka_timestamp?: string
  bronze_ingested_at?: string
}

type RevenueSummary = {
  total_revenue: number
  payment_events: number
}

type PaymentMethod = {
  payment_type: string
  total_events: number
  total_value: number
}

type EventDistribution = {
  operation: string
  total_events: number
}

type TimelinePoint = {
  event_minute: string
  total_events: number
}

type FreshnessRow = {
  table_name: string
  last_updated?: string
  minutes_since_update: number
  total_events: number
}

type DlqRow = {
  error_reason: string
  invalid_records: number
  last_seen?: string
}

type OrderFlowRow = {
  stage: string
  total_orders: number
}

type StateRow = {
  state: string
  total_orders: number
}

type ProductSummary = {
  active_products: number
  active_sellers: number
  top_category: string
}

type SellerRow = {
  seller_id: string
  item_events: number
  gross_value: number
}

type ApiData = {
  statuses: StatusRow[]
  latency: LatencyRow[]
  categories: CategoryRow[]
  reviewScore: ReviewScoreRow
  cdcEvents: CdcEvent[]
  revenue: RevenueSummary
  payments: PaymentMethod[]
  eventDistribution: EventDistribution[]
  timeline: TimelinePoint[]
  freshness: FreshnessRow[]
  dlq: DlqRow[]
  orderFlow: OrderFlowRow[]
  states: StateRow[]
  products: ProductSummary
  sellers: SellerRow[]
}

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000'

const fallbackData: ApiData = {
  statuses: [],
  latency: [],
  categories: [],
  reviewScore: { review_score: 0 },
  cdcEvents: [],
  revenue: { total_revenue: 0, payment_events: 0 },
  payments: [],
  eventDistribution: [],
  timeline: [],
  freshness: [],
  dlq: [],
  orderFlow: [],
  states: [],
  products: {
    active_products: 0,
    active_sellers: 0,
    top_category: 'unknown',
  },
  sellers: [],
}

const navigation: Array<{ id: View; label: string; icon: string }> = [
  { id: 'analytics', label: 'Overview', icon: 'O' },
  { id: 'pipeline', label: 'Live Pulse', icon: 'P' },
  { id: 'orders', label: 'Order Flow', icon: 'F' },
  { id: 'inventory', label: 'Inventory', icon: 'I' },
  { id: 'settings', label: 'System Settings', icon: 'S' },
]

const sliceColors = ['#7fad91', '#b8a9d9', '#a63a54', '#ef7f82', '#c9a9c0', '#44364f']

function formatNumber(value: number) {
  return new Intl.NumberFormat('en-US').format(Math.round(Number(value || 0)))
}

function compact(value: number) {
  return new Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(Number(value || 0))
}

function money(value: number) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(Number(value || 0))
}

function titleCase(value: string) {
  return value
    .replaceAll('_', ' ')
    .split(' ')
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ')
}

function timeAgo(value?: string) {
  if (!value) return 'waiting'
  const parsed = new Date(value).getTime()
  if (Number.isNaN(parsed)) return 'waiting'
  const seconds = Math.max(0, Math.floor((Date.now() - parsed) / 1000))
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  return `${Math.floor(minutes / 60)}h ago`
}

function operationBadge(operation: string) {
  if (operation === 'DELETE') return 'badge red'
  if (operation === 'UPDATE') return 'badge amber'
  return 'badge green'
}

function conicGradient(values: Array<{ value: number; color: string }>) {
  const total = values.reduce((sum, item) => sum + item.value, 0)
  if (!total) return 'conic-gradient(#f1ddfb 0 100%)'

  let cursor = 0
  const stops = values.map((item) => {
    const start = cursor
    cursor += (item.value / total) * 100
    return `${item.color} ${start}% ${cursor}%`
  })

  return `conic-gradient(${stops.join(', ')})`
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`Request failed: ${url}`)
  }
  return response.json() as Promise<T>
}

async function settle<T>(request: () => Promise<T>): Promise<PromiseSettledResult<T>> {
  try {
    return { status: 'fulfilled', value: await request() }
  } catch (reason) {
    return { status: 'rejected', reason }
  }
}

function useApiData(mode: Mode) {
  const [data, setData] = useState<ApiData>(fallbackData)
  const [loadState, setLoadState] = useState<LoadState>('preview')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const hasLoadedRealData = useRef(false)
  const isLoading = useRef(false)

  const load = useCallback(async () => {
    if (isLoading.current) return
    isLoading.current = true
    setIsRefreshing(true)

    let fulfilled = 0
    const totalRequests = 15

    const applyResult = async <T,>(
      request: () => Promise<T>,
      apply: (current: ApiData, value: T) => ApiData,
      hasValue: (value: T) => boolean = () => true,
    ) => {
      const result = await settle(request)
      if (result.status !== 'fulfilled') return

      fulfilled += 1
      if (hasValue(result.value)) {
        setData((current) => apply(current, result.value))
        hasLoadedRealData.current = true
        setLastUpdated(new Date())
        setLoadState('error')
      }
    }

    try {
      await applyResult(
        () => fetchJson<StatusRow[]>(`${API_BASE}/api/business/orders-by-status`),
        (current, statuses) => ({ ...current, statuses }),
        (statuses) => statuses.length > 0,
      )
      await applyResult(
        () => fetchJson<RevenueSummary>(`${API_BASE}/api/business/revenue-summary`),
        (current, revenue) => ({ ...current, revenue }),
      )
      await applyResult(
        () => fetchJson<LatencyRow[]>(`${API_BASE}/api/pipeline/latency`),
        (current, latency) => ({ ...current, latency }),
        (latency) => latency.length > 0,
      )
      await applyResult(
        () => fetchJson<CategoryRow[]>(`${API_BASE}/api/business/top-categories`),
        (current, categories) => ({ ...current, categories }),
        (categories) => categories.length > 0,
      )
      await applyResult(
        () => fetchJson<ReviewScoreRow>(`${API_BASE}/api/business/review-score`),
        (current, reviewScore) => ({ ...current, reviewScore }),
      )
      await applyResult(
        () => fetchJson<CdcEvent[]>(`${API_BASE}/api/cdc/recent`),
        (current, cdcEvents) => ({ ...current, cdcEvents }),
        (cdcEvents) => cdcEvents.length > 0,
      )
      await applyResult(
        () => fetchJson<PaymentMethod[]>(`${API_BASE}/api/business/payment-methods`),
        (current, payments) => ({ ...current, payments }),
        (payments) => payments.length > 0,
      )
      await applyResult(
        () => fetchJson<EventDistribution[]>(`${API_BASE}/api/pipeline/event-distribution`),
        (current, eventDistribution) => ({ ...current, eventDistribution }),
        (eventDistribution) => eventDistribution.length > 0,
      )
      await applyResult(
        () => fetchJson<TimelinePoint[]>(`${API_BASE}/api/pipeline/events-timeline`),
        (current, timeline) => ({ ...current, timeline }),
        (timeline) => timeline.length > 0,
      )
      await applyResult(
        () => fetchJson<FreshnessRow[]>(`${API_BASE}/api/pipeline/table-freshness`),
        (current, freshness) => ({ ...current, freshness }),
        (freshness) => freshness.length > 0,
      )
      await applyResult(
        () => fetchJson<DlqRow[]>(`${API_BASE}/api/pipeline/dlq-summary`),
        (current, dlq) => ({ ...current, dlq }),
      )
      await applyResult(
        () => fetchJson<OrderFlowRow[]>(`${API_BASE}/api/business/order-flow`),
        (current, orderFlow) => ({ ...current, orderFlow }),
        (orderFlow) => orderFlow.length > 0,
      )
      await applyResult(
        () => fetchJson<StateRow[]>(`${API_BASE}/api/business/orders-by-state`),
        (current, states) => ({ ...current, states }),
        (states) => states.length > 0,
      )
      await applyResult(
        () => fetchJson<ProductSummary>(`${API_BASE}/api/business/products-summary`),
        (current, products) => ({ ...current, products }),
      )
      await applyResult(
        () => fetchJson<SellerRow[]>(`${API_BASE}/api/business/top-sellers`),
        (current, sellers) => ({ ...current, sellers }),
        (sellers) => sellers.length > 0,
      )

      if (fulfilled) {
        setLoadState(fulfilled === totalRequests ? 'live' : 'error')
      } else {
        setData((current) => (hasLoadedRealData.current ? current : fallbackData))
        setLoadState(hasLoadedRealData.current ? 'error' : 'preview')
      }
    } finally {
      setIsRefreshing(false)
      isLoading.current = false
    }
  }, [])

  useEffect(() => {
    const firstLoad = window.setTimeout(load, 0)
    const timer = mode === 'realtime' ? window.setInterval(load, 30000) : undefined
    return () => {
      window.clearTimeout(firstLoad)
      if (timer) window.clearInterval(timer)
    }
  }, [load, mode])

  return { data, loadState, lastUpdated, isRefreshing, refresh: load }
}

function Sidebar({
  view,
  onViewChange,
  loadState,
}: {
  view: View
  onViewChange: (view: View) => void
  loadState: LoadState
}) {
  const live = loadState === 'live'

  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">SF</div>
        <div>
          <div className="brand-name">ShopFlow</div>
          <div className="brand-subtitle">CDC Lakehouse</div>
        </div>
      </div>

      <nav className="nav-list" aria-label="Dashboard sections">
        {navigation.map((item) => (
          <button
            key={item.id}
            type="button"
            className={`nav-item ${view === item.id ? 'active' : ''}`}
            onClick={() => onViewChange(item.id)}
          >
            <span className="nav-icon">{item.icon}</span>
            <span>{item.label}</span>
          </button>
        ))}
      </nav>

      <div className="sidebar-status">
        <span className={`status-dot ${live ? 'ok' : 'warn'}`} />
        <div>
          <strong>Pipeline {live ? 'Live' : loadState === 'error' ? 'Partial' : 'Preview'}</strong>
          <span>{live ? 'Synced with Trino' : 'Waiting for API data'}</span>
        </div>
      </div>

      <div className="sidebar-footer">
        <span>API: {API_BASE}</span>
        <span>Poll: 30s</span>
      </div>
    </aside>
  )
}

function Topbar({
  view,
  mode,
  onModeChange,
  lastUpdated,
  isRefreshing,
  onRefresh,
  onOpenSettings,
}: {
  view: View
  mode: Mode
  onModeChange: (mode: Mode) => void
  lastUpdated: Date | null
  isRefreshing: boolean
  onRefresh: () => void
  onOpenSettings: () => void
}) {
  const label = {
    analytics: 'Analytics',
    pipeline: 'Operations',
    orders: 'Order Flow',
    inventory: 'Inventory',
    settings: 'System Settings',
  }[view]

  return (
    <header className="topbar">
      <div className="breadcrumb">
        <span>Dashboard</span>
        <span>/</span>
        <strong>{label}</strong>
      </div>
      <div className="top-actions">
        <button
          type="button"
          className={`tab ${mode === 'realtime' ? 'active' : ''}`}
          onClick={() => onModeChange('realtime')}
        >
          Real-time
        </button>
        <button
          type="button"
          className={`tab ${mode === 'history' ? 'active' : ''}`}
          onClick={() => onModeChange('history')}
        >
          History
        </button>
        <span className="date-pill">{lastUpdated ? `Updated ${timeAgo(lastUpdated.toISOString())}` : 'Waiting'}</span>
        <button type="button" className="icon-button" aria-label="Refresh data" onClick={onRefresh}>
          {isRefreshing ? '...' : 'R'}
        </button>
        <button type="button" className="avatar" aria-label="Open settings" onClick={onOpenSettings}>S</button>
      </div>
    </header>
  )
}

function KpiCard({
  label,
  value,
  trend,
  tone = 'berry',
}: {
  label: string
  value: string
  trend?: string
  tone?: 'berry' | 'green' | 'salmon' | 'violet'
}) {
  return (
    <article className={`kpi-card ${tone}`}>
      <div className="kpi-icon">API</div>
      <span>{label}</span>
      <strong>{value}</strong>
      {trend ? <em>{trend}</em> : null}
    </article>
  )
}

function EmptyState({ label = 'Waiting for API data' }: { label?: string }) {
  return <p className="empty-note">{label}</p>
}

function LineChart({ points }: { points: TimelinePoint[] }) {
  const chartPoints = points.length
    ? points
    : Array.from({ length: 7 }, (_, index) => ({
      event_minute: `empty-${index}`,
      total_events: 0,
    }))
  const values = chartPoints.map((point) => Number(point.total_events || 0))
  const max = Math.max(...values, 1)
  const coordinates = values.map((value, index) => {
    const x = values.length === 1 ? 0 : (index / (values.length - 1)) * 640
    const y = 230 - (value / max) * 190
    return `${x},${y}`
  })
  const path = coordinates.map((coordinate, index) => `${index === 0 ? 'M' : 'L'} ${coordinate}`).join(' ')
  const areaPath = `${path} L 640,260 L 0,260 Z`

  return (
    <div className="line-chart" aria-label="CDC events over time chart">
      <div className="chart-grid">
        <span>{formatNumber(max)}</span>
        <span>{formatNumber(max * 0.66)}</span>
        <span>{formatNumber(max * 0.33)}</span>
        <span>0</span>
      </div>
      <svg viewBox="0 0 640 260" role="presentation" preserveAspectRatio="none">
        <defs>
          <linearGradient id="lineFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#E8748A" stopOpacity="0.2" />
            <stop offset="100%" stopColor="#E8748A" stopOpacity="0" />
          </linearGradient>
        </defs>
        <path className="area" d={areaPath} />
        <path className="line" d={path} />
      </svg>
      <div className="chart-days">
        {chartPoints.slice(-7).map((point, index) => {
          const parsed = new Date(point.event_minute)
          const label = Number.isNaN(parsed.getTime())
            ? `T-${chartPoints.length - index - 1}`
            : parsed.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
          return <span key={point.event_minute}>{label}</span>
        })}
      </div>
    </div>
  )
}

function Donut({
  total,
  slices,
  labels,
}: {
  total: number
  slices: Array<{ label: string; value: number }>
  labels?: boolean
}) {
  const gradient = conicGradient(
    slices.map((slice, index) => ({
      value: Number(slice.value || 0),
      color: sliceColors[index % sliceColors.length],
    })),
  )

  return (
    <div className="donut-wrap">
      <div className="donut" style={{ background: gradient }}>
        <strong>{formatNumber(total)}</strong>
        <span>Total</span>
      </div>
      {labels ? (
        <div className="legend-grid">
          {slices.slice(0, 6).map((slice, index) => (
            <span key={slice.label}>
              <i style={{ background: sliceColors[index % sliceColors.length] }} /> {titleCase(slice.label)}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  )
}

function AnalyticsView({ data, mode }: { data: ApiData; mode: Mode }) {
  const totalOrders = data.statuses.reduce((sum, item) => sum + Number(item.total_orders || 0), 0)
  const canceled = data.statuses.find((item) => item.order_status === 'canceled')?.total_orders ?? 0
  const cancellationRate = totalOrders ? (canceled / totalOrders) * 100 : 0
  const avgLatency = data.latency[0]?.avg_latency_seconds ?? 0
  const categoryTotal = data.categories.reduce((sum, item) => sum + Number(item.total_events || 0), 0)
  const statusSlices = data.statuses.map((status) => ({
    label: status.order_status,
    value: Number(status.total_orders || 0),
  }))

  return (
    <main className="content">
      <section className="kpi-grid">
        <KpiCard label="Total Orders" value={formatNumber(totalOrders)} trend={`${mode} API`} tone="violet" />
        <KpiCard label="Total Revenue" value={money(data.revenue.total_revenue)} trend={`${formatNumber(data.revenue.payment_events)} payments`} />
        <KpiCard label="Avg Latency" value={`${Number(avgLatency).toFixed(1)}s`} trend="Spark to gold" tone="green" />
        <KpiCard label="Cancellation Rate" value={`${cancellationRate.toFixed(1)}%`} trend="From gold table" tone="salmon" />
      </section>

      <section className="dashboard-grid analytics-grid">
        <article className="panel wide">
          <div className="panel-title">
            <div>
              <h2>CDC Events Over Time</h2>
              <p>Minute-level events from the bronze Iceberg table</p>
            </div>
            <span className="live-pill">Live</span>
          </div>
          <LineChart points={data.timeline} />
        </article>

        <article className="panel">
          <h2>Orders by Status</h2>
          <Donut total={totalOrders} slices={statusSlices} labels />
        </article>

        <article className="panel">
          <h2>Top Categories</h2>
          {data.categories.length ? data.categories.map((item) => {
            const value = categoryTotal ? Math.round((Number(item.total_events || 0) / categoryTotal) * 100) : 0
            return (
              <div className="progress-row" key={item.category}>
                <span>{titleCase(item.category)}</span>
                <strong>{value}%</strong>
                <i style={{ width: `${value}%` }} />
              </div>
            )
          }) : <EmptyState />}
        </article>

        <article className="panel">
          <h2>Review Score</h2>
          <div className="score-display">
            <strong>{Number(data.reviewScore.review_score || 0).toFixed(2)}</strong>
            <span>{'*'.repeat(Math.max(1, Math.round(Number(data.reviewScore.review_score || 0))))}</span>
          </div>
        </article>

        <article className="panel">
          <h2>Live CDC Feed</h2>
          <div className="feed-list">
            {data.cdcEvents.length ? data.cdcEvents.slice(0, 4).map((event, index) => (
              <div key={`${event.source_table}-${event.operation}-${event.kafka_timestamp ?? index}`} className="feed-item">
                <span className={operationBadge(event.operation)}>{event.operation}</span>
                <strong>{event.source_table}</strong>
                <small>{timeAgo(event.kafka_timestamp ?? event.bronze_ingested_at)}</small>
              </div>
            )) : <EmptyState />}
          </div>
        </article>
      </section>
    </main>
  )
}

function PipelineView({ data, onRefresh, isRefreshing }: { data: ApiData; onRefresh: () => void; isRefreshing: boolean }) {
  const totalEvents = data.eventDistribution.reduce((sum, item) => sum + Number(item.total_events || 0), 0)
  const byOperation = Object.fromEntries(data.eventDistribution.map((item) => [item.operation, Number(item.total_events || 0)]))
  const avg = data.latency[0]?.avg_latency_seconds ?? 0
  const max = data.latency[0]?.max_latency_seconds ?? 0
  const sla = avg ? Math.max(0, Math.min(100, 100 - avg / 10)) : 0

  return (
    <main className="content">
      <section className="page-heading">
        <div>
          <h1>Pipeline Monitoring</h1>
          <p>Real-time telemetry from Kafka, Spark, Iceberg, and Trino.</p>
        </div>
        <button type="button" className="primary-button" onClick={onRefresh}>
          {isRefreshing ? 'Refreshing...' : 'Force Sync'}
        </button>
      </section>

      <section className="metric-strip">
        <div><span>Total CDC Events</span><strong>{compact(totalEvents)}</strong></div>
        <div><span>Inserts</span><strong className="green-text">{compact(byOperation.INSERT ?? 0)}</strong></div>
        <div><span>Updates</span><strong>{compact(byOperation.UPDATE ?? 0)}</strong></div>
        <div><span>Deletes</span><strong className="red-text">{compact(byOperation.DELETE ?? 0)}</strong></div>
        <div><span>Avg Latency</span><strong>{Number(avg).toFixed(1)}s</strong></div>
        <div><span>SLA Score</span><strong>{sla.toFixed(1)}%</strong></div>
      </section>

      <section className="dashboard-grid pipeline-grid">
        <article className="panel wide">
          <h2>CDC Events Per Minute</h2>
          <LineChart points={data.timeline} />
        </article>
        <article className="panel">
          <h2>Event Type Distribution</h2>
          <div className="event-stack">
            {data.eventDistribution.length ? data.eventDistribution.slice(0, 4).map((event, index) => {
              const percent = totalEvents ? Math.round((Number(event.total_events || 0) / totalEvents) * 100) : 0
              return (
                <div className="mini-ring" style={{ borderColor: sliceColors[index % sliceColors.length] }} key={event.operation}>
                  {percent}%
                </div>
              )
            }) : <EmptyState />}
            {data.eventDistribution.slice(0, 4).map((event) => (
              <strong key={`${event.operation}-label`}>
                {event.operation}<br /><span>{formatNumber(event.total_events)}</span>
              </strong>
            ))}
          </div>
        </article>
        <article className="panel">
          <h2>Latency by Table</h2>
          {data.latency.length ? data.latency.map((row) => {
            const width = max ? Math.min(100, (Number(row.avg_latency_seconds || 0) / max) * 100) : 0
            return (
              <div className="latency-row" key={row.table_name}>
                <span>{row.table_name}</span>
                <strong>{Number(row.avg_latency_seconds || 0).toFixed(1)}s</strong>
                <i style={{ width: `${width}%` }} />
              </div>
            )
          }) : <EmptyState />}
        </article>
        <article className="panel">
          <h2>Table Freshness</h2>
          {data.freshness.length ? data.freshness.slice(0, 5).map((row) => (
            <div className={`fresh-row ${Number(row.minutes_since_update || 0) > 10 ? 'stale' : ''}`} key={row.table_name}>
              <span />
              <strong>{row.table_name}</strong>
              <em>{timeAgo(row.last_updated)}</em>
            </div>
          )) : <EmptyState />}
        </article>
        <article className="panel danger-panel">
          <div className="panel-title">
            <h2>Invalid Records / DLQ</h2>
            <span className="error-pill">{formatNumber(data.dlq.reduce((sum, item) => sum + Number(item.invalid_records || 0), 0))} Active</span>
          </div>
          {(data.dlq.length ? data.dlq : [{ error_reason: 'No invalid records', invalid_records: 0 }]).map((item) => (
            <div className="dlq-card" key={item.error_reason}>
              <strong>{item.error_reason}</strong>
              <p>{formatNumber(item.invalid_records)} records {item.last_seen ? `last seen ${timeAgo(item.last_seen)}` : ''}</p>
            </div>
          ))}
        </article>
      </section>
    </main>
  )
}

function OrdersView({ data }: { data: ApiData }) {
  const flowRows = data.orderFlow.length
    ? data.orderFlow
    : ['Created', 'Approved', 'Processing', 'Shipped', 'Delivered'].map((stage) => ({ stage, total_orders: 0 }))
  const total = data.orderFlow.find((item) => item.stage === 'Created')?.total_orders
    ?? data.statuses.reduce((sum, item) => sum + Number(item.total_orders || 0), 0)
  const delivered = data.orderFlow.find((item) => item.stage === 'Delivered')?.total_orders ?? 0
  const shipped = data.orderFlow.find((item) => item.stage === 'Shipped')?.total_orders ?? 0
  const onTime = total ? (delivered / total) * 100 : 0
  const stateMax = Math.max(...data.states.map((row) => Number(row.total_orders || 0)), 1)

  return (
    <main className="content">
      <section className="order-kpis">
        <KpiCard label="Orders Created" value={formatNumber(total)} trend="From silver.orders" />
        <KpiCard label="Orders Delivered" value={formatNumber(delivered)} trend={`${onTime.toFixed(1)}% of flow`} tone="green" />
        <KpiCard label="Orders Shipped" value={formatNumber(shipped)} trend="Carrier handoff" tone="violet" />
      </section>
      <article className="panel flow-panel">
        <h2>Order Pipeline Flow</h2>
        <div className="flow-steps">
          {flowRows.map((item, index) => (
            <div key={item.stage} className={`flow-step ${index >= flowRows.length - 2 ? 'success' : ''}`}>
              <strong>{formatNumber(item.total_orders)}</strong>
              <span>{item.stage}</span>
            </div>
          ))}
        </div>
      </article>
      <section className="dashboard-grid three">
        <article className="panel">
          <h2>Orders by State</h2>
          <div className="state-grid">
            {data.states.length ? data.states.slice(0, 10).map((row) => {
              const bucket = Math.min(4, Math.floor((Number(row.total_orders || 0) / stateMax) * 5))
              return <span key={row.state} className={`heat heat-${bucket}`}>{row.state}</span>
            }) : <EmptyState />}
          </div>
        </article>
        <article className="panel">
          <h2>Delivery Completion</h2>
          <div className="empty-chart">
            <span>{onTime.toFixed(1)}% delivered</span>
            <strong>{formatNumber(total - delivered)} open orders</strong>
          </div>
        </article>
        <article className="panel">
          <h2>Status Ring Summary</h2>
          <Donut
            total={total}
            slices={data.statuses.map((status) => ({ label: status.order_status, value: status.total_orders }))}
            labels
          />
        </article>
      </section>
    </main>
  )
}

function InventoryView({ data }: { data: ApiData }) {
  const topCategoryCount = data.categories[0]?.total_events ?? 0
  const categoryTotal = data.categories.reduce((sum, item) => sum + Number(item.total_events || 0), 0)
  const topShare = categoryTotal ? (topCategoryCount / categoryTotal) * 100 : 0

  return (
    <main className="content">
      <section className="kpi-grid">
        <KpiCard label="Active Products" value={formatNumber(data.products.active_products)} trend="Distinct products" />
        <KpiCard label="Active Sellers" value={formatNumber(data.products.active_sellers)} trend="Distinct sellers" tone="green" />
        <KpiCard label="Top Category" value={titleCase(data.products.top_category)} trend={`${topShare.toFixed(1)}% of product CDC`} tone="violet" />
        <KpiCard label="Avg Review Score" value={Number(data.reviewScore.review_score || 0).toFixed(2)} trend="From review CDC" />
      </section>
      <section className="dashboard-grid inventory-grid">
        <article className="panel wide">
          <h2>Category Performance</h2>
          <div className="table-list">
            {data.categories.length ? data.categories.map((row) => (
              <div className="table-row" key={row.category}>
                <span>{titleCase(row.category)}</span>
                <span>{formatNumber(row.total_events)} events</span>
                <span>{categoryTotal ? `${((row.total_events / categoryTotal) * 100).toFixed(1)}%` : '0%'}</span>
                <span>{Number(data.reviewScore.review_score || 0).toFixed(2)}</span>
                <span>CDC</span>
              </div>
            )) : <EmptyState />}
          </div>
        </article>
        <article className="panel">
          <h2>Top Sellers</h2>
          {data.sellers.length ? data.sellers.map((seller, index) => (
            <div className="seller-row" key={seller.seller_id}>
              <strong>{index + 1}</strong>
              <span>{seller.seller_id.slice(0, 8)}</span>
              <em>{money(seller.gross_value)}</em>
            </div>
          )) : <EmptyState />}
        </article>
        <article className="panel wide">
          <h2>Price vs. Review Signal</h2>
          <LineChart
            points={data.sellers.map((seller, index) => ({
              event_minute: `${index}`,
              total_events: Math.round(Number(seller.gross_value || 0)),
            }))}
          />
        </article>
        <article className="panel">
          <h2>Payment Method Breakdown</h2>
          <Donut
            total={data.payments.reduce((sum, item) => sum + Number(item.total_events || 0), 0)}
            slices={data.payments.map((payment) => ({ label: payment.payment_type, value: payment.total_events }))}
            labels
          />
        </article>
      </section>
    </main>
  )
}

function SettingsView({ data, loadState }: { data: ApiData; loadState: LoadState }) {
  const [copied, setCopied] = useState(false)
  const totalBronze = data.eventDistribution.reduce((sum, item) => sum + Number(item.total_events || 0), 0)
  const totalGold = data.statuses.reduce((sum, item) => sum + Number(item.total_orders || 0), 0)

  async function copyManifest() {
    const manifest = JSON.stringify({
      api_base: API_BASE,
      polling_seconds: 30,
      status: loadState,
      warehouse: 's3://shopflow-lakehouse/warehouse',
      api_backed_widgets: true,
    }, null, 2)
    await navigator.clipboard.writeText(manifest)
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1500)
  }

  return (
    <main className="content">
      <section className="system-banner">
        <span>{loadState === 'live' ? 'OK' : '!'}</span>
        <div>
          <h1>{loadState === 'live' ? 'All API Panels Connected' : 'API Data Partially Available'}</h1>
          <p>Every dashboard panel is fed by FastAPI endpoints with local preview fallback.</p>
        </div>
        <code>Status: {loadState}</code>
      </section>
      <section className="settings-grid">
        <article className="panel">
          <h2>Service Connections</h2>
          {[
            ['FastAPI', loadState !== 'preview' ? 'Connected' : 'Preview'],
            ['Trino', loadState === 'live' ? 'Connected' : 'Partial'],
            ['Iceberg Bronze', totalBronze ? `${formatNumber(totalBronze)} events` : 'Waiting'],
            ['Iceberg Gold', totalGold ? `${formatNumber(totalGold)} orders` : 'Waiting'],
            ['Frontend Poller', '30s active'],
          ].map(([service, status]) => (
            <div className="service-card" key={service}>
              <strong>{service}</strong>
              <span>{status}</span>
            </div>
          ))}
        </article>
        <article className="panel code-panel">
          <div className="panel-title">
            <h2>Pipeline Configuration</h2>
            <button type="button" className="ghost-button" onClick={copyManifest}>
              {copied ? 'Copied' : 'Copy Manifest'}
            </button>
          </div>
          <pre>{`{
  "api_base": "${API_BASE}",
  "polling_seconds": 30,
  "layers": ["bronze", "silver", "gold"],
  "warehouse": "s3://shopflow-lakehouse/warehouse"
}`}</pre>
        </article>
        <article className="panel wide">
          <h2>Iceberg Data Activity</h2>
          <div className="table-list">
            {data.freshness.slice(0, 6).map((row) => (
              <div className="table-row" key={row.table_name}>
                <span>{row.table_name}</span>
                <span>{formatNumber(row.total_events)} events</span>
                <span>{timeAgo(row.last_updated)}</span>
                <span>{Number(row.minutes_since_update || 0).toFixed(1)}m</span>
                <span>Bronze</span>
              </div>
            ))}
          </div>
        </article>
        <article className="panel">
          <h2>Layer Usage</h2>
          {[
            ['Bronze Layer', totalBronze],
            ['Silver Orders', data.orderFlow[0]?.total_orders ?? 0],
            ['Gold Orders', totalGold],
            ['DLQ Records', data.dlq.reduce((sum, row) => sum + Number(row.invalid_records || 0), 0)],
          ].map(([layer, value]) => {
            const width = totalBronze ? Math.min(100, (Number(value) / totalBronze) * 100) : 0
            return (
              <div className="storage-row" key={layer}>
                <span>{layer}</span>
                <strong>{formatNumber(Number(value))}</strong>
                <i style={{ width: `${width}%` }} />
              </div>
            )
          })}
          <div className="capacity">{compact(totalBronze)}</div>
        </article>
      </section>
    </main>
  )
}

function App() {
  const [view, setView] = useState<View>('analytics')
  const [mode, setMode] = useState<Mode>('realtime')
  const { data, loadState, lastUpdated, isRefreshing, refresh } = useApiData(mode)
  const viewNode = useMemo(() => {
    if (view === 'pipeline') return <PipelineView data={data} onRefresh={refresh} isRefreshing={isRefreshing} />
    if (view === 'orders') return <OrdersView data={data} />
    if (view === 'inventory') return <InventoryView data={data} />
    if (view === 'settings') return <SettingsView data={data} loadState={loadState} />
    return <AnalyticsView data={data} mode={mode} />
  }, [data, isRefreshing, loadState, mode, refresh, view])

  return (
    <div className="app-shell">
      <Sidebar view={view} onViewChange={setView} loadState={loadState} />
      <div className="workspace">
        <Topbar
          view={view}
          mode={mode}
          onModeChange={setMode}
          lastUpdated={lastUpdated}
          isRefreshing={isRefreshing}
          onRefresh={refresh}
          onOpenSettings={() => setView('settings')}
        />
        {viewNode}
      </div>
    </div>
  )
}

export default App
