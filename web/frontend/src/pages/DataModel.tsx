import { Link } from 'react-router-dom'
import { IconArrowRight } from '../components/Icons'

const LAYERS = [
  {
    tag: 'ODS',
    title: '原始事件层',
    text: '4 个 Retailrocket CSV 原样落库，保留原始字段、行号与载入批次；不在这里做业务解释。',
    scale: '2,756,101 条事件 · 20,275,902 条商品属性',
  },
  {
    tag: 'DWD',
    title: '明细事实层',
    text: '统一字段类型、时间与事件语义，去重并剔除无用户或商品的行；这是后续所有指标的单一事实来源。',
    scale: '2,755,641 条保留 · 460 条无用户/商品 · 0 条其他异常',
  },
  {
    tag: 'DWS',
    title: '汇总服务层',
    text: '按日、用户、商品与漏斗阶段预聚合，固定 PV / UV / DAU、转化率与 D1 留存等口径。',
    scale: '139 天日粒度 · 13 项对账指标',
  },
  {
    tag: 'ADS',
    title: '应用数据层',
    text: '面向看板的最终宽表与指标快照，FastAPI 只读查询，不在请求线程里重算。',
    scale: '总览 / 商品 / 留存 / 质量页面',
  },
]

const UPLOAD_STEPS = [
  { tag: '01', title: '上传数据包', text: 'CSV / TSV / Excel / ZIP' },
  { tag: '02', title: '识别文件', text: '编码、表头、分隔符、列类型' },
  { tag: '03', title: '字段映射', text: '列名 + 样本值打分，用户确认' },
  { tag: '04', title: '规范化事件', text: 'view / add_to_cart / transaction / other' },
  { tag: '05', title: 'DuckDB 两层模型', text: 'fact_events + agg_*' },
  { tag: '06', title: '结果快照', text: 'result.json，只在任务目录' },
]

export default function DataModelPage() {
  return (
    <div className="page-stack model-page">
      <header className="analysis-report-head model-head">
        <div className="analysis-report-head__main">
          <p className="eyebrow">ARCHITECTURE / 数据模型</p>
          <h1 className="analysis-report-title">不是两套数据，而是两条计算路径</h1>
          <p className="analysis-report-meta">
            四层离线数仓负责可追溯的批处理；Kafka + Flink 负责实时链路；上传分析在独立
            DuckDB 沙箱中完成，不触碰既有基线。
          </p>
        </div>
        <div className="analysis-report-head__actions">
          <Link className="btn btn-primary" to="/analyze">
            上传分析
            <IconArrowRight size={15} />
          </Link>
          <Link className="btn btn-ghost" to="/realtime" title="内部验证视图，实时数据源尚未接入">
            内部批流对账
          </Link>
        </div>
      </header>

      <section className="card analysis-panel" aria-labelledby="principle-title">
        <div className="analysis-panel__head">
          <div>
            <p className="eyebrow">01 / PRINCIPLE</p>
            <h2 className="analysis-panel__title" id="principle-title">
              为什么要「比较」
            </h2>
          </div>
          <span className="analysis-panel__step">同一口径 · 两条实现</span>
        </div>

        <div className="model-prose">
          <p>
            离线批处理和实时流处理不是两份业务数据，而是<strong>同一套指标定义的两种实现</strong>。
            离线链路按天重算，结果稳定、可回填；实时链路按事件增量更新，要求低延迟。两条链路
            如果口径不一致，看板就会在切换数据源时给出互相矛盾的结论。
          </p>
          <p>
            所以批流对账做的不是「比较两份报表谁对」，而是拿实时链路的输出去对齐离线 ADS 的日聚合，
            验证实时作业有没有漏事件、重复计数、时区错位或窗口边界错误。0 差异说明链路可信；
            有差异则定位到具体日期与具体指标。这一页目前是内部验证视图，尚无新的实时数据源接入，
            导航入口暂时收起。
          </p>
        </div>

        <div className="model-equation" aria-label="差异定义">
          <span className="model-equation__label">差异</span>
          <span className="model-equation__op">=</span>
          <span className="model-equation__term">实时聚合</span>
          <span className="model-equation__op">−</span>
          <span className="model-equation__term">离线 ADS</span>
        </div>
        <p className="analysis-side__note">
          差异为 0 表示一致；SKIP 表示部分日或该指标在当前窗口不可比，不是「漏算」。
        </p>
      </section>

      <section className="card analysis-panel" aria-labelledby="paths-title">
        <div className="analysis-panel__head">
          <div>
            <p className="eyebrow">02 / BATCH + STREAM</p>
            <h2 className="analysis-panel__title" id="paths-title">
              两条链路，13 项指标
            </h2>
          </div>
          <span className="analysis-panel__step">137 个已对账日期</span>
        </div>

        <div className="model-flow">
          <div className="model-flow__lane">
            <span className="model-lane-label">离线</span>
            <div className="model-flow__nodes">
              {['原始 CSV', 'ODS', 'DWD', 'DWS', 'ADS'].map((node, index) => (
                <span className="model-flow__node" key={node}>
                  <span className="model-flow__index">{String(index + 1).padStart(2, '0')}</span>
                  {node}
                </span>
              ))}
            </div>
            <span className="model-lane-note">按天重算 · 可回填</span>
          </div>

          <div className="model-flow__lane model-flow__lane--stream">
            <span className="model-lane-label">实时</span>
            <div className="model-flow__nodes">
              {['Kafka', 'Flink 窗口聚合', 'rt_* 日聚合'].map((node, index) => (
                <span className="model-flow__node" key={node}>
                  <span className="model-flow__index">{String(index + 1).padStart(2, '0')}</span>
                  {node}
                </span>
              ))}
            </div>
            <span className="model-lane-note">增量更新 · 低延迟</span>
          </div>

          <div className="model-flow__compare">
            <div>
              <strong>1,781</strong>
              <span>MATCH</span>
            </div>
            <div>
              <strong>0</strong>
              <span>DIFF</span>
            </div>
            <div>
              <strong>26</strong>
              <span>SKIP</span>
            </div>
            <p>13 项指标 × 137 个已对账日期；差异恒为 0 时，实时链路与离线口径一致。</p>
          </div>
        </div>
      </section>

      <section className="card analysis-panel" aria-labelledby="layers-title">
        <div className="analysis-panel__head">
          <div>
            <p className="eyebrow">03 / WAREHOUSE</p>
            <h2 className="analysis-panel__title" id="layers-title">
              四层离线数仓
            </h2>
          </div>
          <span className="analysis-panel__step">139 天基准</span>
        </div>

        <div className="model-layers">
          {LAYERS.map((layer) => (
            <article className="model-layer" key={layer.tag}>
              <span className="model-layer__tag">{layer.tag}</span>
              <h3>{layer.title}</h3>
              <p>{layer.text}</p>
              <small>{layer.scale}</small>
            </article>
          ))}
        </div>

        <div className="model-identity">
          <div className="model-identity__item">
            <strong>2,756,101</strong>
            <span>ODS 原始事件</span>
          </div>
          <span className="model-identity__op">=</span>
          <div className="model-identity__item">
            <strong>2,755,641</strong>
            <span>DWD 保留</span>
          </div>
          <span className="model-identity__op">+</span>
          <div className="model-identity__item">
            <strong>460</strong>
            <span>无用户 / 商品</span>
          </div>
          <span className="model-identity__op">+</span>
          <div className="model-identity__item">
            <strong>0</strong>
            <span>其他异常</span>
          </div>
        </div>
        <p className="analysis-side__note">
          每条原始事件都能在恒等式中找到去向；差异不是丢失，而是可解释的分类。
        </p>
      </section>

      <section className="card analysis-panel" aria-labelledby="sandbox-title">
        <div className="analysis-panel__head">
          <div>
            <p className="eyebrow">04 / UPLOAD SANDBOX</p>
            <h2 className="analysis-panel__title" id="sandbox-title">
              上传分析为什么是两层
            </h2>
          </div>
          <span className="analysis-panel__step">每任务一个 DuckDB</span>
        </div>

        <div className="model-steps">
          {UPLOAD_STEPS.map((step) => (
            <div className="model-step" key={step.tag}>
              <span className="model-step__tag">{step.tag}</span>
              <strong>{step.title}</strong>
              <span>{step.text}</span>
            </div>
          ))}
        </div>

        <div className="model-two-col">
          <div>
            <h3>只保留一条事件事实表</h3>
            <p>
              上传数据没有稳定的 ODS 批次、也没有可复用的 DWS/ADS 主题域。强行套四层只会把
              「识别—映射—规范化」的复杂度藏进空表里。这里直接把用户确认过的列规范化为
              <code> fact_events</code>，再生成日、漏斗、留存、商品、类目与质量六类
              <code> agg_*</code> 表。
            </p>
          </div>
          <div>
            <h3>与既有数仓完全隔离</h3>
            <p>
              每个任务独占一个目录与 DuckDB 文件，只写 <code>ANALYSIS_DATA_DIR</code>；
              不新增 MySQL 写账号，不修改四层数仓、139 天基准或实时对账结果。任务 7 天后自动清理，
              也可以随时手动删除。
            </p>
          </div>
        </div>

        <div className="model-callout-row">
          <div>
            <p className="eyebrow">MINIMUM CONTRACT</p>
            <p>四列必填：用户、商品、事件类型、事件时间。</p>
          </div>
          <div>
            <p className="eyebrow">OPTIONAL</p>
            <p>订单号、类目、金额、商品名称；缺失时对应指标自动降级。</p>
          </div>
          <div>
            <p className="eyebrow">OUTPUT</p>
            <p>结果快照只包含本次上传数据的真实聚合值。</p>
          </div>
        </div>
      </section>

      <section className="card analysis-panel" aria-labelledby="identity-title">
        <div className="analysis-panel__head">
          <div>
            <p className="eyebrow">05 / IDENTIFIERS</p>
            <h2 className="analysis-panel__title" id="identity-title">
              商品编号从哪来
            </h2>
          </div>
          <span className="analysis-panel__step">原始值</span>
        </div>
        <div className="model-prose">
          <p>
            看板中的 <code>itemid</code> / <code>categoryid</code> 是 Retailrocket
            原始数据集里的真实标识，不是前端生成的代号。原始数据不提供商品名称，因此这里只能展示编号；
            上传文件如果带商品名称列，结果页会同时显示名称与原始 ID。
          </p>
          <p>
            上传分析的 ID 同样直接来自你上传的行，工具不会重编号，也不会用序号伪装成商品标识。
          </p>
        </div>
        <div className="analysis-actions">
          <Link className="link-button" to="/items">
            去商品页看原始 ID
            <IconArrowRight size={13} />
          </Link>
          <Link className="link-button" to="/quality">
            查看数据质量
            <IconArrowRight size={13} />
          </Link>
        </div>
      </section>
    </div>
  )
}
