import type { ReactNode } from 'react'
import { EmptyState } from './EmptyState'
import { Skeleton } from './Skeleton'

export interface Column<T> {
  key: string
  title: ReactNode
  align?: 'left' | 'right' | 'center'
  width?: number | string
  render?: (row: T, index: number) => ReactNode
  className?: string
}

interface DataTableProps<T> {
  columns: Column<T>[]
  rows: T[]
  rowKey: (row: T, index: number) => string | number
  loading?: boolean
  skeletonRows?: number
  emptyText?: string
  emptyHint?: string
  maxHeight?: number
  onRowClick?: (row: T) => void
  isRowActive?: (row: T) => boolean
  caption?: string
}

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  loading = false,
  skeletonRows = 5,
  emptyText = '该区间暂无数据',
  emptyHint,
  maxHeight,
  onRowClick,
  isRowActive,
  caption,
}: DataTableProps<T>) {
  const interactive = Boolean(onRowClick)

  return (
    <div className="table-wrap" style={maxHeight ? { maxHeight } : undefined}>
      <table className="data-table" aria-busy={loading}>
        {caption ? <caption className="sr-only">{caption}</caption> : null}
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                scope="col"
                style={{ width: column.width, textAlign: column.align ?? 'left' }}
                className={column.align === 'right' ? 'num' : undefined}
              >
                {column.title}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {loading ? (
            Array.from({ length: skeletonRows }, (_, rowIndex) => (
              <tr key={`skeleton-${rowIndex}`}>
                {columns.map((column, columnIndex) => (
                  <td key={column.key} className={column.align === 'right' ? 'num' : undefined}>
                    <Skeleton
                      height={11}
                      width={`${48 + ((rowIndex * 7 + columnIndex * 13) % 40)}%`}
                      style={{ marginLeft: column.align === 'right' ? 'auto' : undefined }}
                    />
                  </td>
                ))}
              </tr>
            ))
          ) : rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="table-empty-cell">
                <EmptyState text={emptyText} hint={emptyHint} />
              </td>
            </tr>
          ) : (
            rows.map((row, rowIndex) => {
              const active = isRowActive?.(row) ?? false
              return (
                <tr
                  key={rowKey(row, rowIndex)}
                  className={interactive ? `clickable${active ? ' active' : ''}` : undefined}
                  onClick={onRowClick ? () => onRowClick(row) : undefined}
                  onKeyDown={
                    onRowClick
                      ? (event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault()
                            onRowClick(row)
                          }
                        }
                      : undefined
                  }
                  tabIndex={interactive ? 0 : undefined}
                  aria-selected={interactive ? active : undefined}
                >
                  {columns.map((column) => (
                    <td
                      key={column.key}
                      className={[
                        column.align === 'right' ? 'num' : '',
                        column.className ?? '',
                      ]
                        .filter(Boolean)
                        .join(' ')}
                      style={{ textAlign: column.align ?? 'left' }}
                    >
                      {column.render ? column.render(row, rowIndex) : String(row[column.key as keyof T] ?? '—')}
                    </td>
                  ))}
                </tr>
              )
            })
          )}
        </tbody>
      </table>
    </div>
  )
}
