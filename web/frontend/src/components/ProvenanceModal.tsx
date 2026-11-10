import { useEffect } from 'react'
import { IconClose } from './Icons'

interface ProvenanceModalProps {
  open: boolean
  onClose: () => void
}

/**
 * 数据来源与口径说明。内容为项目级事实，不依赖当前数据库连接，
 * 这样在分析沙箱/数据库抖动时也能查看。
 */
export function ProvenanceModal({ open, onClose }: ProvenanceModalProps) {
  useEffect(() => {
    if (!open) return undefined
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose])

  if (!open) return null

  return (
    <div className="modal-backdrop" role="presentation" onClick={onClose}>
      <section
        className="modal-panel provenance-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="provenance-title"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="modal-head">
          <div>
            <p className="eyebrow">DATA PROVENANCE</p>
            <h2 className="modal-title" id="provenance-title">
              数据来源与口径
            </h2>
          </div>
          <button type="button" className="icon-button" onClick={onClose} aria-label="关闭">
            <IconClose size={18} />
          </button>
        </header>

        <div className="modal-body">
          <section className="provenance-section">
            <h3>看板数据集</h3>
            <dl className="provenance-list">
              <div>
                <dt>数据集</dt>
                <dd>Retailrocket E-Commerce Dataset（Kaggle / Hugging Face 镜像）</dd>
              </div>
              <div>
                <dt>许可证</dt>
                <dd>CC BY-NC-SA 4.0 · 仅用于学习与非商业演示</dd>
              </div>
              <div>
                <dt>规模</dt>
                <dd>events 2,756,101 行 · item_properties 20,275,902 行 · category_tree 1,669 行</dd>
              </div>
              <div>
                <dt>时间范围</dt>
                <dd>2015-05-03 ~ 2015-09-18（UTC；首末日为部分日）</dd>
              </div>
              <div>
                <dt>校验</dt>
                <dd>4 个原始 CSV 均已记录 SHA-256；ODS 原样保留，DWD 去重后 2,755,641 + 460 + 0</dd>
              </div>
              <div>
                <dt>链路</dt>
                <dd>CSV → ODS → DWD → DWS → ADS → FastAPI 只读接口 → 本页面</dd>
              </div>
            </dl>
          </section>

          <section className="provenance-section">
            <h3>商品编号不是编造的</h3>
            <p>
              页面上出现的 <code>itemid</code> / <code>categoryid</code> 均来自 Retailrocket
              原始数据，是数据集中的真实标识，不是前端生成的代号。原始数据<strong>不提供商品名称</strong>，
              因此看板只能展示编号；上传文件如果包含商品名称列，分析结果会显示名称。
            </p>
          </section>

          <section className="provenance-section">
            <h3>批流对账是什么</h3>
            <p>
              批流对账页比较「离线 ADS 日聚合」与「Kafka → Flink 写入的 rt_* 日聚合」，验证实时链路是否与批处理同口径。
              <strong>差异 = 实时 − 离线</strong>，0 表示一致；SKIP 表示部分日或该指标不可比。它不是两套业务数据在打架。
              该页目前是内部验证视图，尚无新的实时数据源接入，导航入口暂时收起。
            </p>
          </section>

          <section className="provenance-section">
            <h3>上传分析沙箱</h3>
            <p>
              上传文件只写入本机 <code>ANALYSIS_DATA_DIR</code>，每个任务一个 DuckDB 库；
              不写 MySQL，不影响四层数仓和 139 日基线。任务保留 7 天，可随时删除。
            </p>
          </section>
        </div>
      </section>
    </div>
  )
}
