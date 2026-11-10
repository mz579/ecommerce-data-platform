import { useCallback, useEffect, useRef, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { prefersReducedMotion } from '../utils/transition'

const BIRD_W = 84
const BIRD_H = 56
const NEST_SCALE = 0.46

interface Point {
  x: number
  y: number
  scale: number
}

interface PerchBounds {
  minX: number
  maxX: number
  feetY: number
  scale: number
}

function fmt(point: Point): string {
  return `translate3d(${point.x}px, ${point.y}px, 0) scale(${point.scale})`
}

function feetPoint(feetX: number, feetY: number, scale: number): Point {
  return {
    x: feetX - (BIRD_W * scale) / 2,
    y: feetY - BIRD_H * scale,
    scale,
  }
}

function randomBetween(min: number, max: number): number {
  return min + Math.random() * (max - min)
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms))
}

function nestPoint(): Point | null {
  const nest = document.querySelector<HTMLElement>('[data-magpie-nest]')
  if (!nest) return null
  const rect = nest.getBoundingClientRect()
  if (rect.width < 2) return null
  return feetPoint(rect.left + rect.width / 2, rect.bottom - 3, NEST_SCALE)
}

function perchPoint(): Point | null {
  const perch = document.querySelector<HTMLElement>('[data-magpie-perch]')
  if (!perch) return null
  const rect = perch.getBoundingClientRect()
  if (rect.width < 40 || rect.height < 40) return null
  const scale = rect.width < 260 ? 0.56 : rect.width < 380 ? 0.72 : 0.9
  const feetY = rect.top + BIRD_H * scale + 16
  const feetX = rect.left + rect.width * 0.82
  return feetPoint(feetX, feetY, scale)
}

function perchBounds(point: Point, scale: number): PerchBounds | null {
  const perch = document.querySelector<HTMLElement>('[data-magpie-perch]')
  if (!perch) return null
  const rect = perch.getBoundingClientRect()
  if (rect.width < 40) return null
  return {
    minX: rect.left + rect.width * 0.74,
    maxX: rect.left + rect.width - BIRD_W * scale - 12,
    feetY: point.y + BIRD_H * scale,
    scale,
  }
}

async function waitForPerch(timeout = 2200): Promise<boolean> {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    if (document.querySelector('[data-magpie-perch]')) return true
    await wait(60)
  }
  return false
}

/**
 * 喜鹊彩蛋：平时停在分析页的绯红面板上，离开页面就飞回侧栏的鸟窝。
 * 回到分析页时随机选一种落回方式；点它会让它飞出去玩一圈再回来。
 * 数据属性和固定定位让它跨路由存活，不打断 SPA 导航。
 */
export function MagpiePet() {
  const location = useLocation()
  const birdRef = useRef<HTMLDivElement>(null)
  const animRef = useRef<Animation | null>(null)
  const tokenRef = useRef(0)
  const routeTokenRef = useRef(0)
  const busyRef = useRef(false)
  const placedRef = useRef(false)
  const homeRef = useRef<'nest' | 'panel'>('nest')
  const boundsRef = useRef<PerchBounds | null>(null)
  const perchRectRef = useRef<{ left: number; top: number } | null>(null)
  const nextIdleRef = useRef(Date.now() + 4000)
  const [placed, setPlaced] = useState(false)
  const [flying, setFlying] = useState(false)
  const [home, setHome] = useState<'nest' | 'panel'>('nest')
  const [idle, setIdle] = useState<'peck' | 'hop' | null>(null)
  const idleTimerRef = useRef<number | undefined>(undefined)

  const placeInstant = useCallback((target: Point) => {
    const node = birdRef.current
    if (!node) return
    animRef.current?.cancel()
    node.style.transform = fmt(target)
    node.style.opacity = '1'
    placedRef.current = true
    setPlaced(true)
  }, [])

  const moveTo = useCallback(
    (target: Point, options?: { duration?: number; arc?: number; walk?: boolean }) => {
      const node = birdRef.current
      const token = ++tokenRef.current
      animRef.current?.cancel()
      if (!node) return Promise.resolve()

      if (!placedRef.current) {
        placeInstant(target)
        return Promise.resolve()
      }

      const rect = node.getBoundingClientRect()
      const from: Point = {
        x: rect.left,
        y: rect.top,
        scale: rect.width > 1 ? rect.width / BIRD_W : 1,
      }

      if (prefersReducedMotion()) {
        placeInstant(target)
        return Promise.resolve()
      }

      const distance = Math.hypot(target.x - from.x, target.y - from.y)
      const duration = options?.duration ?? Math.max(480, Math.min(1500, distance * 1.5))
      const arc = options?.arc ?? Math.min(120, 36 + distance * 0.1)
      const mid: Point = {
        x: (from.x + target.x) / 2,
        y: Math.min(from.y, target.y) - arc,
        scale: (from.scale + target.scale) / 2,
      }

      node.style.transform = fmt(from)
      const anim = node.animate(
        [
          { transform: fmt(from) },
          { transform: options?.walk ? fmt(mid) : `${fmt(mid)} rotate(-7deg)`, offset: 0.55 },
          { transform: fmt(target) },
        ],
        { duration, easing: 'cubic-bezier(0.4, 0.02, 0.3, 1)', fill: 'forwards' },
      )
      animRef.current = anim
      if (!options?.walk) setFlying(true)

      // 页面被切到后台时浏览器可能冻结动画时间线，finished 迟迟不触发；
      // 加一道超时兜底，保证喜鹊最终一定落位，不会永远悬在半空。
      return new Promise<void>((resolve) => {
        let settled = false
        const settle = () => {
          if (settled) return
          settled = true
          if (token !== tokenRef.current) {
            resolve()
            return
          }
          anim.cancel()
          node.style.transform = fmt(target)
          node.style.opacity = '1'
          animRef.current = null
          setFlying(false)
          resolve()
        }
        anim.finished.then(settle).catch(() => undefined)
        window.setTimeout(settle, duration + 400)
      })
    },
    [placeInstant],
  )

  // 内容加载完、滚动条出现等都会让面板轻微位移；按差值整体平移，
  // 保留喜鹊当前走到的位置，而不是每帧重新算一个默认落点。
  const syncPerch = useCallback(() => {
    if (homeRef.current !== 'panel' || busyRef.current || animRef.current) return
    const perch = document.querySelector<HTMLElement>('[data-magpie-perch]')
    const node = birdRef.current
    if (!perch || !node) return
    const rect = perch.getBoundingClientRect()
    const previous = perchRectRef.current
    perchRectRef.current = { left: rect.left, top: rect.top }
    if (!previous) return
    const dx = rect.left - previous.left
    const dy = rect.top - previous.top
    if (Math.abs(dx) < 0.5 && Math.abs(dy) < 0.5) return
    const current = node.getBoundingClientRect()
    node.style.transform = fmt({
      x: current.left + dx,
      y: current.top + dy,
      scale: current.width > 1 ? current.width / BIRD_W : 1,
    })
    const bounds = boundsRef.current
    if (bounds) {
      boundsRef.current = {
        ...bounds,
        minX: bounds.minX + dx,
        maxX: bounds.maxX + dx,
        feetY: bounds.feetY + dy,
      }
    }
  }, [])

  const doIdle = useCallback(() => {
    setIdle(Math.random() < 0.5 ? 'peck' : 'hop')
    window.clearTimeout(idleTimerRef.current)
    idleTimerRef.current = window.setTimeout(() => setIdle(null), 900)
  }, [])

  const play = useCallback(async () => {
    if (busyRef.current || prefersReducedMotion()) {
      doIdle()
      return
    }
    const node = birdRef.current
    if (!node) return
    busyRef.current = true
    const home = homeRef.current === 'panel' ? perchPoint() : nestPoint()
    const away = feetPoint(
      randomBetween(window.innerWidth * 0.3, window.innerWidth * 0.78),
      randomBetween(window.innerHeight * 0.32, window.innerHeight * 0.66),
      0.85,
    )
    try {
      await moveTo(away, { duration: 720 })
      await wait(760)
      doIdle()
      await wait(520)
      if (home) await moveTo(home, { duration: 760 })
    } finally {
      busyRef.current = false
    }
  }, [doIdle, moveTo])

  // 路由变化：进分析页随机选一种落回面板的方式；离开则飞回鸟窝。
  useEffect(() => {
    const routeToken = ++routeTokenRef.current
    let cancelled = false
    const enteringAnalyze = location.pathname === '/analyze'

    async function settle() {
      if (!enteringAnalyze) {
        homeRef.current = 'nest'
        setHome('nest')
        boundsRef.current = null
        const nest = nestPoint()
        if (!nest) return
        if (!placedRef.current) {
          placeInstant(nest)
          return
        }
        busyRef.current = true
        await moveTo(nest, { duration: 900 })
        busyRef.current = false
        return
      }

      const ready = await waitForPerch()
      if (cancelled || routeToken !== routeTokenRef.current || !ready) return

      const nest = nestPoint()
      const perch = perchPoint()
      if (!perch) return
      homeRef.current = 'panel'
      setHome('panel')
      boundsRef.current = perchBounds(perch, perch.scale)
      const perchElement = document.querySelector<HTMLElement>('[data-magpie-perch]')
      const perchRect = perchElement?.getBoundingClientRect()
      perchRectRef.current = perchRect
        ? { left: perchRect.left, top: perchRect.top }
        : null
      busyRef.current = true
      nextIdleRef.current = Date.now() + 5000

      if (!placedRef.current) {
        // 首次加载就停在窝里，然后飞上红色面板。
        if (nest) placeInstant(nest)
        await moveTo(perch, { duration: 1000 })
        busyRef.current = false
        return
      }

      const behavior = Math.floor(Math.random() * 3)
      if (nest) await moveTo(nest, { duration: 380, arc: 26 })
      if (cancelled || routeToken !== routeTokenRef.current) return

      if (behavior === 0) {
        // 掉下去，再飞回矩阵。
        const nestNow = nestPoint()
        if (nestNow) {
          const fall = { x: nestNow.x, y: nestNow.y + 130, scale: nestNow.scale * 1.15 }
          await moveTo(fall, { duration: 360, arc: 0 })
          await wait(260)
        }
      } else if (behavior === 2) {
        // 飞到别处玩一下，再回矩阵。
        const detour = feetPoint(
          randomBetween(window.innerWidth * 0.35, window.innerWidth * 0.7),
          randomBetween(window.innerHeight * 0.3, window.innerHeight * 0.55),
          0.9,
        )
        await moveTo(detour, { duration: 620 })
        await wait(240)
      }
      if (cancelled || routeToken !== routeTokenRef.current) return
      await moveTo(perch, { duration: 860 })
      busyRef.current = false
    }

    settle()
    return () => {
      cancelled = true
      routeTokenRef.current += 1
    }
  }, [location.pathname, moveTo, placeInstant])

  // 光标待机：偶尔在面板上走两步、啄一下或跳一下。
  useEffect(() => {
    const timer = window.setInterval(() => {
      if (busyRef.current || flying || prefersReducedMotion()) return
      if (Date.now() < nextIdleRef.current) return
      nextIdleRef.current = Date.now() + randomBetween(4500, 9000)
      if (homeRef.current !== 'panel') {
        if (Math.random() < 0.35) doIdle()
        return
      }
      const bounds = boundsRef.current
      if (!bounds) return
      const targetX = randomBetween(Math.min(bounds.minX, bounds.maxX), Math.max(bounds.minX, bounds.maxX))
      const target = feetPoint(
        targetX + (BIRD_W * bounds.scale) / 2,
        bounds.feetY,
        bounds.scale,
      )
      void moveTo(target, { duration: 1400, arc: 10, walk: true }).then(() => {
        if (Math.random() < 0.45) doIdle()
      })
    }, 1200)
    return () => window.clearInterval(timer)
  }, [doIdle, flying, moveTo])

  // 面板随页面滚动或布局变化：喜鹊跟着面板走，而不是浮在原地。
  useEffect(() => {
    let frame = 0
    const onScroll = () => {
      if (frame) return
      frame = window.requestAnimationFrame(() => {
        frame = 0
        syncPerch()
      })
    }
    const onResize = () => {
      if (busyRef.current) return
      if (homeRef.current === 'panel') {
        syncPerch()
        return
      }
      const nest = nestPoint()
      if (nest) placeInstant(nest)
    }
    const layoutTimer = window.setInterval(syncPerch, 1600)
    window.addEventListener('scroll', onScroll, true)
    window.addEventListener('resize', onResize)
    return () => {
      window.clearInterval(layoutTimer)
      window.removeEventListener('scroll', onScroll, true)
      window.removeEventListener('resize', onResize)
      if (frame) window.cancelAnimationFrame(frame)
    }
  }, [placeInstant, syncPerch])

  useEffect(() => () => window.clearTimeout(idleTimerRef.current), [])

  return (
    <div
      ref={birdRef}
      className={`magpie-pet${flying ? ' is-flying' : ''}${idle ? ` is-${idle}` : ''}`}
      style={{ opacity: placed ? 1 : 0 }}
    >
      <button
        type="button"
        className="magpie-pet__hit"
        aria-label="喜鹊，点一下让它飞出去玩"
        title="点我"
        onClick={play}
      >
        <svg viewBox="0 0 96 64" aria-hidden="true" focusable="false">
          {home === 'nest' && !flying ? (
            <g className="magpie__nest-pose">
              <ellipse cx="48" cy="43" rx="15" ry="11" fill="#08080A" />
              <path d="M34 44 C40 38 56 37 63 43 C56 50 40 50 34 44 Z" fill="#141014" />
              <circle cx="62" cy="31" r="8.5" fill="#08080A" />
              <path d="M68 29 L79 32 L68 35 Z" fill="#08080A" />
              <circle cx="65" cy="29" r="1.4" fill="#D7D6D4" />
            </g>
          ) : (
            <>
              <g className="magpie__tail">
                <path d="M38 40 L4 58 L9 63 L44 50 Z" fill="#08080A" />
              </g>
              <g className="magpie__legs">
                <path d="M44 50 L44 61 M56 50 L56 61" stroke="#08080A" strokeWidth="2.4" strokeLinecap="round" />
                <path d="M40 61 L47 61 M52 61 L59 61" stroke="#08080A" strokeWidth="2.2" strokeLinecap="round" />
              </g>
              <path
                d="M34 33 C41 20 58 16 68 25 C77 33 76 46 67 51 C58 56 44 55 38 49 C33 44 31 38 34 33 Z"
                fill="#08080A"
              />
              <g className="magpie__wing">
                <path
                  d="M45 31 C53 27 63 30 67 37 C64 44 54 47 47 43 C41 40 40 34 45 31 Z"
                  fill="#141014"
                />
                <path d="M47 33 C53 32 60 34 63 38" stroke="#2a2628" strokeWidth="1.2" strokeLinecap="round" />
              </g>
              <g className="magpie__head">
                <circle cx="71" cy="21" r="10" fill="#08080A" />
                <path d="M79 19 L92 23 L79 26 Z" fill="#08080A" />
                <circle cx="74.5" cy="19.5" r="1.5" fill="#D7D6D4" />
              </g>
            </>
          )}
        </svg>
      </button>
    </div>
  )
}
