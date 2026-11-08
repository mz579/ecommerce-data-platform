import type { SVGProps } from 'react'

type IconProps = SVGProps<SVGSVGElement> & { size?: number }

function base({ size = 18, ...props }: IconProps) {
  return {
    width: size,
    height: size,
    viewBox: '0 0 20 20',
    fill: 'none',
    stroke: 'currentColor',
    strokeWidth: 1.6,
    strokeLinecap: 'round' as const,
    strokeLinejoin: 'round' as const,
    'aria-hidden': true,
    focusable: false,
    ...props,
  }
}

export function IconOverview(props: IconProps) {
  return (
    <svg {...base(props)}>
      <rect x="3" y="3" width="5.5" height="5.5" rx="1.2" />
      <rect x="11.5" y="3" width="5.5" height="5.5" rx="1.2" />
      <rect x="3" y="11.5" width="5.5" height="5.5" rx="1.2" />
      <rect x="11.5" y="11.5" width="5.5" height="5.5" rx="1.2" />
    </svg>
  )
}

export function IconItems(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M3.5 6.5 10 3l6.5 3.5v7L10 17l-6.5-3.5z" />
      <path d="M3.5 6.5 10 10l6.5-3.5M10 10v7" />
    </svg>
  )
}

export function IconRetention(props: IconProps) {
  return (
    <svg {...base(props)}>
      <circle cx="7" cy="7" r="2.6" />
      <path d="M2.8 16.5c.4-2.7 2.1-4.3 4.2-4.3s3.8 1.6 4.2 4.3" />
      <path d="M13.2 4.8a2.4 2.4 0 0 1 0 4.4M14.4 12.4c1.6.4 2.6 1.8 2.9 4.1" />
    </svg>
  )
}

export function IconRealtime(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M2.5 10h3l1.8-4.5 3.4 9L13 10h4.5" />
    </svg>
  )
}

export function IconQuality(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M10 2.8 16 5v4.6c0 3.6-2.2 6.4-6 7.6-3.8-1.2-6-4-6-7.6V5z" />
      <path d="m7.2 9.8 1.9 1.9 3.7-3.8" />
    </svg>
  )
}

export function IconRefresh(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M16 6.5A6.5 6.5 0 1 0 16.3 13" />
      <path d="M16 3.2v3.6h-3.6" />
    </svg>
  )
}

export function IconSun(props: IconProps) {
  return (
    <svg {...base(props)}>
      <circle cx="10" cy="10" r="3.2" />
      <path d="M10 2.2v1.4M10 16.4v1.4M2.2 10h1.4M16.4 10h1.4M4.5 4.5l1 1M14.5 14.5l1 1M15.5 4.5l-1 1M5.5 14.5l-1 1" />
    </svg>
  )
}

export function IconMoon(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M16.2 11.9A6.6 6.6 0 0 1 8.1 3.8a6.6 6.6 0 1 0 8.1 8.1z" />
    </svg>
  )
}

/** 品牌标识：极简柱形，不使用渐变。 */
export function BrandMark() {
  return (
    <svg width="18" height="18" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <rect x="3" y="11" width="3" height="6" rx="1" fill="currentColor" opacity="0.7" />
      <rect x="8.5" y="6" width="3" height="11" rx="1" fill="currentColor" />
      <rect x="14" y="3" width="3" height="14" rx="1" fill="currentColor" opacity="0.85" />
    </svg>
  )
}
