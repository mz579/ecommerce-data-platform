import { useRef, useState } from 'react'
import { IconUpload } from './Icons'

interface UploadDropzoneProps {
  onFile: (file: File) => void
  uploading: boolean
  progress: number
  error: string | null
}

const ACCEPT = '.csv,.tsv,.txt,.xlsx,.xlsm,.zip'

export function UploadDropzone({ onFile, uploading, progress, error }: UploadDropzoneProps) {
  const inputRef = useRef<HTMLInputElement>(null)
  const [dragging, setDragging] = useState(false)

  const choose = () => {
    if (!uploading) inputRef.current?.click()
  }

  return (
    <div
      className={`dropzone${dragging ? ' is-dragging' : ''}${uploading ? ' is-uploading' : ''}`}
      role="button"
      tabIndex={uploading ? -1 : 0}
      aria-disabled={uploading}
      onClick={choose}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault()
          choose()
        }
      }}
      onDragOver={(event) => {
        event.preventDefault()
        if (!uploading) setDragging(true)
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={(event) => {
        event.preventDefault()
        setDragging(false)
        if (uploading) return
        const file = event.dataTransfer.files?.[0]
        if (file) onFile(file)
      }}
    >
      <input
        ref={inputRef}
        type="file"
        accept={ACCEPT}
        hidden
        onChange={(event) => {
          const file = event.target.files?.[0]
          event.target.value = ''
          if (file) onFile(file)
        }}
      />
      <span className="dropzone-icon" aria-hidden="true">
        <IconUpload size={24} />
      </span>
      <p className="dropzone-title">
        {uploading ? '正在上传数据包' : '拖入数据包，或点击选择文件'}
      </p>
      <p className="dropzone-hint">CSV / TSV / Excel / ZIP · 单文件 ≤ 200MB · ≤ 500 万行</p>

      {uploading ? (
        <div className="dropzone-progress">
          <span style={{ width: `${Math.round(progress * 100)}%` }} />
        </div>
      ) : null}
      {error ? <p className="dropzone-error">{error}</p> : null}
    </div>
  )
}
