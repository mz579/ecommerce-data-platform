# ODS 层设计文档（Phase 2：ODS 入库与数据批次）

> 本阶段完成 4 个原始 CSV 文件到 MySQL 贴源层（ODS）的**分块、幂等、可断点恢复**
> 入库，并沉淀批次审计（`etl_batch_log`）、确定性代理键规则、失败恢复流程与
> 实际导入对账结果。本文所有行数、耗时、SHA-256 均来自本机真实执行。
>
> 相关文档：`docs/dataset.md`（数据来源与校验）、`docs/data_dictionary.md`（字段画像）、
> `docs/data_contract.md`（异常处置契约）。

## 1. 层级与职责

ODS（Operational Data Store，贴源层）是数仓分层的**第一层**，职责只有两条：

1. **原样保真**：把源 CSV 的每一列作为字符串整体入库，不做清洗、不去重、
   不转换类型。即使字段值与下游契约相违背（如 `events.visitorid=0`、属性值随意），
   ODS 也照收——那个"判官"角色由 Phase 3（DWD）担任。空单元格以空串 `''` 保存，
   与文件物理内容逐字节可对照。
2. **可审计、可重放**：每张 ODS 表都带有血缘元数据列，保证任意一行都能
   grep 回原始文件并说明它来自哪个批次。

ODS 的表与源文件一一对应：

| 源文件 | ODS 表 | 原始列 |
| --- | --- | --- |
| `events.csv` | `ods_events_raw` | timestamp, visitorid, event, itemid, transactionid（5 列） |
| `item_properties_part1.csv` / `_part2.csv` | `ods_item_properties_raw` | timestamp, itemid, property, value（4 列） |
| `category_tree.csv` | `ods_category_tree_raw` | categoryid, parentid（2 列） |

> `item_properties` 的 part1 / part2 是两个独立文件、表头相同、可拼接，因此
> **各自作为一个批次**整文件入库（同一张表），`source_file` 区分来源。

## 2. 表结构

### 2.1 血缘元数据列（三张 ODS 表共用）

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `row_key` | BINARY(32) | 确定性代理键，**幂等键**（见 §4），主键 |
| 原始列… | VARCHAR / TEXT | 原始字段，字符串保真 |
| `source_file` | VARCHAR(255) | 来源文件名（basename），如 `events.csv` |
| `source_row_number` | BIGINT UNSIGNED | CSV 物理行号（**含表头偏移**：表头=1，首条数据=2），`sed -n 'Np'` 可回查 |
| `ingest_batch_id` | BIGINT UNSIGNED | `etl_batch_log.batch_id` 的逻辑外键（不建物理 FK，保证灌数性能） |
| `loaded_at` | DATETIME(3) | 本批次写入时刻（UTC） |

唯一键 `(source_file, source_row_number)` + 主键 `row_key` 共同保证：**同一文件的
同一行，无论重复导入多少次，在表里只有一份。**

### 2.2 各表 DDL 位置

| 对象 | DDL 文件 |
| --- | --- |
| `etl_batch_log` | `sql/ddl/ods_etl_batch_log.sql` |
| `ods_events_raw` | `sql/ddl/ods_events_raw.sql` |
| `ods_item_properties_raw` | `sql/ddl/ods_item_properties_raw.sql` |
| `ods_category_tree_raw` | `sql/ddl/ods_category_tree_raw.sql` |

- 全部 `ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`。
- 全部 `CREATE TABLE IF NOT EXISTS`，可重复执行（迁移类改动需新建脚本，不手工改表）。
- `item_properties.value` 用 `TEXT`（≤64 KB）防止任意长属性值被截断；其余原始列
  给足 VARCHAR 余量后按保真入库，超出会触发 MySQL 严格模式报错而非静默截断。
- 执行方式：`uv run python scripts/apply_ods_ddl.py`（幂等，重复执行打印已存在表）。

### 2.3 etl_batch_log（批次审计表）

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `batch_id` | BIGINT UNSIGNED AUTO_INCREMENT | 批次主键 |
| `source_file` | VARCHAR(255) | 来源文件名 |
| `file_sha256` | CHAR(64) | 源文件整体 SHA-256（16 进制），重复批次检测依据 |
| `table_name` | VARCHAR(64) | 目标 ODS 表 |
| `status` | VARCHAR(16) | `PENDING` / `LOADED` / `FAILED` / `CLEANED` |
| `row_count` | BIGINT UNSIGNED | 本批次**实际提交**的行数（失败时等于失败前已提交的部分行数） |
| `started_at` / `finished_at` | DATETIME(3) | 批次起止（UTC），差值为纯导入耗时 |
| `error_summary` | TEXT | 失败原因（异常类型 + 消息，截断到 4000 字符，不含任何凭据） |

状态机：

```text
PENDING ──成功──► LOADED
   │
   └────失败────► FAILED ──cleanup──► CLEANED
```

## 3. 批次模型

每次「一个文件 → 一张 ODS 表」的运行构成一个**批次**，生命周期：

1. **预检**：计算文件 SHA-256；读取并校验表头（列顺序必须与契约一致）。
2. **幂等判断**（见 §4.2）：查询 `etl_batch_log`：
   - 同 `(source_file, file_sha256)` 已有 `LOADED` → **跳过**（不新开批次，不再写一行）；
   - 存在 `PENDING` / `FAILED` 未清理 → **拒绝**并提示先 `cleanup`（防脏续跑）；
   - 都不是 → 登记 `PENDING` 批次，进入加载。
3. **加载**：`csv.reader` 逐行流式读取，攒够 `chunk_size`（默认 10,000）行后
   `executemany` 多行 INSERT，**每块一个事务**、逐块 commit，内存占用恒定
   （≤ 单块大小）。
4. **收尾**：全部成功 → `status=LOADED, row_count=提交行数, finished_at`；
   任何一步抛错 → 回滚当前块 → `status=FAILED, error_summary, row_count=已提交行数`。

进度日志每 5 块打一行（批号、文件、已提交行数），落在控制台与
`logs/ods_ingestion.log`。

### 3.1 失败恢复 / 断点续跑

失败批次（`PENDING` / `FAILED`）的处理流程：

```text
① 查看失败原因    uv run python -m ecommerce_dw.ingestion.cli status
② 清理失败批次    uv run python -m ecommerce_dw.ingestion.cli cleanup --batch <batch_id>
                  （删除该批次已提交的部分行，日志置 CLEANED，保留审计轨迹）
③ 修复后重跑      uv run python -m ecommerce_dw.ingestion.cli load <file>
                  （自动开新批次；表头校验 + 唯一键双保险，永不重复写已存在的行）
```

`cleanup --file <source_file>` 可一次清掉某文件所有批次。清理只作用于
`ingest_batch_id = batch_id` 的行，绝不触碰其他批次数据。

## 4. 幂等键规则（重点）

### 4.1 确定性代理键 row_key

源数据没有天然唯一 ID（`transactionid` 是订单级、`events` 无主键、属性表
`(itemid, property, timestamp)` 非全序唯一），因此自建**确定性代理键**：

```text
row_key = SHA-256( UTF-8( source_file + U+001F(0x1F) + source_row_number ) )
```

- 存储为 `BINARY(32)`（32 字节），只依赖「文件名 + 物理行号」两个字节级事实，
  **跨批次、跨运行完全不变**；不依赖 MySQL 自增主键，因此不受导入顺序影响。
- 分隔符 U+001F 不会出现在行中，杜绝 `events.csv` + 行号 21 与
  `events.csv` + 行号 2 + `1` 之类的歧义拼接。
- `source_row_number` 用 1 起的物理行号（表头=1），`sed -n 'Np' 源文件` 可直接回查。
- 计算实现：`src/ecommerce_dw/ingestion/keys.py::make_row_key`，
  单元测试覆盖确定性与编码细节。

### 4.2 三层幂等保障

1. **业务唯一键**：每张表 `UNIQUE (source_file, source_row_number)`——即使
   INSERT 重复也会在库层报错，绝不产生物理重复行。
2. **前置跳单**：同 SHA-256 已 `LOADED` 的文件直接跳过（并打印「已是批次 #N」），
   连 INSERT 都不发。
3. **force 重载**：`load --force` 先删除该文件在目标表的所有行、把旧批次置
   `CLEANED`，再全新加载——语义是"重灌"，不是"叠加"，行数必然不变。

三条同时生效，实测（§6）证明重复导入后 `COUNT(*)` 与源文件行数严格相等。

## 5. 代码结构

```text
src/ecommerce_dw/ingestion/
├── __init__.py        # 对外 API（load_csv / cleanup_* / make_row_key / …）
├── cli.py             # CLI：load / inspect / status / cleanup / reconcile
├── loader.py          # 编排：预检、批次登记、分块加载、失败标记、清理
├── chunk.py           # CsvChunkReader：内存有上限的分块读取 + 行号跟踪
├── keys.py            # row_key 生成、文件 SHA-256
├── batch_log.py       # etl_batch_log 仓储
├── specs.py           # 文件/表/原始列 规格注册表
└── db.py              # MySQL 连接工厂（凭据全部来自环境变量）
```

- 配置：连接信息全部来自 `.env` / 环境变量（复用 `src/ecommerce_dw/config.py`），
  代码与日志中不出现密码。
- 日志：结构化 `logging`，控制台 + `logs/ods_ingestion.log`（已被 gitignore）。
- 测试：`tests/unit/test_ingestion_*.py`（无数据库）；`tests/integration/test_ods_mysql.py`
  （连真实 MySQL，测完自清；连不上时 skip 而非 fail）。

## 6. 实测结果（本机真实执行）

（§6 由实际导入后填写：四个文件的 SHA-256、导入耗时、行数对账、重复导入验证，
以及 20M 行的性能策略与结论。）