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

export function IconAnalyze(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M3.5 3.5h8v6.2" />
      <path d="M3.5 3.5v13h6.2" />
      <circle cx="12.2" cy="12.2" r="3.6" />
      <path d="m14.8 14.8 2.8 2.8M11 12.2h2.4M12.2 11v2.4" />
    </svg>
  )
}

export function IconModel(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="m10 2.6 7 3.4-7 3.4-7-3.4z" />
      <path d="m3 9.9 7 3.4 7-3.4" />
      <path d="m3 13.4 7 3.4 7-3.4" />
    </svg>
  )
}

export function IconUpload(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M10 12.8V3.2" />
      <path d="m6.4 6.7 3.6-3.5 3.6 3.5" />
      <path d="M3.5 12.5v3.2h13v-3.2" />
    </svg>
  )
}

export function IconFile(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M5 2.8h6l4 4v10.4H5z" />
      <path d="M11 2.8v4h4M7.4 10h5.2M7.4 12.7h5.2" />
    </svg>
  )
}

export function IconClose(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="m4.5 4.5 11 11M15.5 4.5l-11 11" />
    </svg>
  )
}

export function IconCheck(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="m3.5 10.4 4.2 4.2 8.8-9" />
    </svg>
  )
}

export function IconSpark(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M10 2.5c.7 4.4 2.9 6.6 7.3 7.3-4.4.7-6.6 2.9-7.3 7.3-.7-4.4-2.9-6.6-7.3-7.3 4.4-.7 6.6-2.9 7.3-7.3z" />
    </svg>
  )
}

export function IconArrowRight(props: IconProps) {
  return (
    <svg {...base(props)}>
      <path d="M3.5 10h13M11.2 4.7 16.5 10l-5.3 5.3" />
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

/** 侧栏品牌图标：一只编织的鸟窝，喜鹊偶尔会停在上面。 */
export function NestMark() {
  return (
    <svg width="19" height="19" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path
        d="M3.1 8.7h13.8c0 4.2-2.9 7.1-6.9 7.1S3.1 12.9 3.1 8.7Z"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
      <path
        d="M3.6 8.5c1.1-1.9 3.3-3 6.4-3s5.3 1.1 6.4 3"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinecap="round"
      />
      <path
        d="M5.3 11.3l9.4-1.6M5.8 13.5l8.4-1.1"
        stroke="currentColor"
        strokeWidth="1"
        strokeLinecap="round"
        opacity="0.55"
      />
      <path
        d="M6.9 5.9c.7-1.3 1.6-2 2.9-2.4M13 5.8c.6-.7 1.2-1.1 2-1.4"
        stroke="currentColor"
        strokeWidth="1.1"
        strokeLinecap="round"
      />
    </svg>
  )
}
