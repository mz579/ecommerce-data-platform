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
   - 已有 `LOADED` 但 `file_sha256` 已变化（同名换内容）→ **拒绝**，提示先 `cleanup --file`
     或 `load --force`（§4.1 row_key 边界）；
   - 同 `(source_file, file_sha256)` 存在 `PENDING` → 判定为「上次运行进程被中断」，
     **就地续传该批次**（§3.1/§3.2，同一 `batch_id`，不清掉已提交行）；
   - 存在 `FAILED` 或「SHA-256 不同」的 `PENDING` → **拒绝**并提示先 `cleanup`（防脏续跑）；
   - 都不是 → 登记 `PENDING` 批次，进入加载。
3. **加载**：`csv.reader` 逐行流式读取，攒够 `chunk_size`（默认 10,000）行后
   先按活体表结构校验单元格宽度（§4.2 第 4 条，超长直接使批次 `FAILED`），再
   `executemany` 多行 INSERT，**每块一个事务**、逐块 commit，内存占用恒定
   （≤ 单块大小）。
4. **收尾**：全部成功 → `status=LOADED, row_count=提交行数, finished_at`；
   任何一步抛错 → 回滚当前块 → `status=FAILED, error_summary, row_count=已提交行数`。

进度日志每 5 块打一行（批号、文件、已提交行数），落在控制台与
`logs/ods_ingestion.log`。

### 3.1 失败恢复 / 断点续跑

批次未完成时先分两种情况：

- **进程还活着、只是 MySQL 断连** → §3.2 的**进程内自动续传**（无需人工干预）。
- **进程已被终止 / 退出**（遗留 `PENDING` 批次）或**批次曾出错**（遗留 `FAILED` 批次）→
  下一次运行按下表自动处理，也可人工介入：

```text
同一 PENDING + SHA-256 不变  →  就地续传（resume）：SELECT COUNT(*) 统计该批次
                                 已提交行数，从下一行继续写入，batch_id 不变；
                                 进程被杀死绝不丢失已提交的行（实测见 §6.4 故障 4）
FAILED 批次                  →  必须先清理：cleanup --batch <id>（删行 + 置 CLEANED）
PENDING 但 SHA-256 已变化     →  说明源文件被换过，同样必须先清理再重灌
```

人工流程（`uv run python -m ecommerce_dw.ingestion.cli`）：

```text
① 查看批次状态  status                     （看哪个文件卡在 PENDING / FAILED）
② 清理失败批次  cleanup --batch <batch_id>
                （删除该批次已提交的部分行，日志置 CLEANED，保留审计轨迹）
③ 重新加载      load <file>
                （续传语义自动生效：PENDING 同哈希继续、其余开新批次）
```

`cleanup --file <source_file>` 可一次清掉某文件所有批次。清理只作用于
`ingest_batch_id = batch_id` 的行，绝不触碰其他批次数据。

### 3.2 连接丢失自动续传（进程存活）

加载过程中 MySQL 因重启/失联而断连时，加载器**不结束批次**：

1. 回滚当前未提交块（`INSERT IGNORE` 之下重放也是 no-op，见 §4.2）；
2. 轮询重连（每 5 秒探测一次，单次等待上限 `resume_wait_seconds`，可重试
   `max_resumes` 次，全程不退出进程）；
3. `SELECT COUNT(*) where ingest_batch_id = batch_id` 拿到已提交行号，继续从下一行
   写入——**同一进程、同一 `batch_id`、已提交行一个不丢**（实测见 §6.4 故障 1）。

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

**row_key 的适用边界：它假定「同名文件的内容不变」。** row_key 由「文件名 + 物理行号」
两个事实决定。若同一文件名被换成**新内容**（重新下载、历史修正），新文件每一行的
row_key 与唯一键 `(source_file, source_row_number)` 都会和旧数据**逐行相撞**——若直接
入库会被全部 IGNORE，表里还是旧数据，然而批次日志会记一条新 `LOADED`（sha 变了），
`COUNT(*)` 对账依然通过，形成**完全静默的数据错误**。因此加载器对「同一 `source_file`
已有 `LOADED` 批次、但 `file_sha256` 已变化」的情况**直接拒绝**并给明确报错（§3.1），
内容更新必须走**显式替换流程**：`cleanup --file <name>`（或 `load --force`）先清掉旧批次
与旧行，再灌新内容。

**为什么不用自增主键当幂等键：** 自增键「插入了才知道的 ID」跨运行不稳定，无法把
「同一物理行」从一次运行映射到下一次，也就无法支撑失败批次的部分重放、行级对账与
跨进程续传（§3.1/§3.2）。确定性键让 `row_key` 只由文件与物理行号推导，重跑、续传、
替换全程不变——代价正是上面的「边界」，所以用「文件内容变化必须显式替换」来兜底。

### 4.2 幂等与防截断保障

1. **业务唯一键**：每张表 `UNIQUE (source_file, source_row_number)`——即使
   INSERT 重复也会在库层报错，绝不产生物理重复行。
2. **前置跳单 + 内容变化拦截**：同 SHA-256 已 `LOADED` 的文件直接跳过（并打印
   「已是批次 #N」），连 INSERT 都不发；**同名但 SHA-256 已变**（内容被替换）则按
   §4.1 直接**拒绝**并提示走显式替换流程，杜绝"旧键承载新内容"的静默错误。
3. **force 重载**：`load --force` 先删除该文件在目标表的所有行、把旧批次置
   `CLEANED`，再全新加载——语义是"重灌"，不是"叠加"，行数必然不变。
4. **入库前长度校验（防 INSERT IGNORE 静默截断）**：`INSERT IGNORE` 会无视
   `STRICT_TRANS_TABLES`，超长字段会被悄悄截断进列宽。因此加载器在每块 commit 前
   按**活体表结构**（`information_schema.COLUMNS`）校验每个单元格 ≤ 目标列宽，
   超长直接抛 `ColumnTooLongError` 并让批次 `FAILED`（`error_summary` 记录行号与列名），
   而不是截断后成功。

四条同时生效，实测（§6）证明重复导入后 `COUNT(*)` 与源文件行数严格相等。

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

### 6.1 源文件指纹（SHA-256，导入前计算）

| 源文件 | 字节数 | SHA-256 |
| --- | --- | --- |
| `category_tree.csv` | 14,454 | `94e865eb0a3d48cbbfe3b79079018dd92509315c88f5fd8d00d0b4b5af434f5b` |
| `events.csv` | 94,237,913 | `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7` |
| `item_properties_part1.csv` | 484,315,749 | `30aad5aeca58b2dc27dcc73e1708565f5818e45adb3eb57401f91e87355b0b81` |
| `item_properties_part2.csv` | 408,929,907 | `d5e7d1a91dc40f522aeb596b267e6c87d8aed689a7192d12369cfb165eb987e5` |

### 6.2 运行环境特征（为什么设计成"可断点恢复"）

开发机上的 WSL2 虚拟机每几分钟整体重启一次（docker daemon 启动时间与 `uptime`
持续归零），MySQL 容器随之反复失联。15 分钟连通性采样显示 **可用率约 11%**
（27 次 OK / 240 次采样，窗口集中在每次 VM 启动后的 30~60 秒，随后再次重启）。
命名卷让数据跨重启持久化，但这意味着任何"一次性连上、一口气导完"的脚本都不可能
稳定跑完 —— 分块 + INSERT IGNORE + 批内自动续传（§3.2）是让 2200 万行在这种
环境下仍可推进的前提。

### 6.3 性能策略（20M 行的速度与内存取舍）

| 手段 | 说明 |
| --- | --- |
| 分块流式读取（`chunk_size`） | 默认 10,000 行/块；`events` 与两个 `item_properties` 用 **50,000 行/块**。每块一个事务一次 commit，削减 InnoDB fsync 次数（本机单次 commit 约 150–400ms，是每个可用窗口内的主要开销） |
| 单语句批量 INSERT | `cursor.max_stmt_length = 8MB`，`executemany` 把一块 5 万行拼成一条多值 INSERT，避免逐行网络往返 |
| `INSERT IGNORE` | 连接在块提交瞬间断裂时，重放该块是 no-op；配合 `(source_file, source_row_number)` 唯一键，行数永不虚增 |
| 内存上界 | 任意时刻只保留 ≤ 一个块（5 万行 × 4~5 列的参数元组 ≈ 数十 MB），CSV 逐行流式丢弃；从不把 480MB 的源文件整体读进内存 |
| 实测吞吐 | 可用窗口内约 **4–5 万行/秒**（含解析 + 哈希 + 插入 + commit），单次 10~15 秒窗口可提交 20–60 万行 |

行数、耗时与对账结果见下（每次执行后填写真实数字）。

### 6.4 断点恢复实测（本机真实故障实录）

> 本节记录**真实发生**的故障与恢复：2026-11-06 晚间加载 `events.csv`（2,756,101 行）期间，
> WSL2 虚拟机多次被自动关机/重启（Docker 守护进程与 MySQL 容器随之反复失联；连通性
> 采样显示 MySQL 可用率仅约 11%）。以下批次状态、恢复动作与耗时均取自 `etl_batch_log`
> 与 `logs/ods_ingestion.log` 原始记录。

**故障 1 — 批次中途断连（自动续传，无需人工干预）**

加载 `events.csv` 分两轮：第一轮（批次 #16，10k 分块）中途被我主动停掉重灌（见故障 2），
第二轮（批次 #17，50k 分块）最终成功。加载器在插入过程中遇到连接丢失时按 §3.2 自动
回滚当前块 → 重连 → 续传：

**第 1 轮 — 批次 #16**（后被清理重灌，见故障 2）：

| 时刻 | 事件 | 恢复动作 |
| --- | --- | --- |
| 22:09:04 | `Server shutdown in progress`（1053），已提交 ~50 万行 | 回滚 → 重连 → 从行 `510002` 续传；22:13:22 恢复到 55 万行（恢复约 4 min） |
| 22:13:35 | `Lost connection`（2013），已提交 ~70 万行 | 回滚 → 重连 → 从行 `700002` 续传（此后该轮停摆，故障 2 清理重灌） |

**第 2 轮 — 批次 #17**（最终成功，2,756,101 行）：

| 时刻 | 事件 | 恢复动作 |
| --- | --- | --- |
| 22:35:30 | `Lost connection`（2013），启动后尚未提交任何行 | 重连 → 从行 `2` 续传 |
| 22:44:56 | `MySQL server has gone away`（2006），已提交 ~15 万行 | 重连 → 从行 `150002` 续传；22:55:02 恢复到 25 万行（恢复约 10 min） |
| 22:55:17 | `Lost connection`（2013），已提交 ~60 万行 | 重连 → 从行 `600002` 续传；23:03:38 恢复到 75 万行（恢复约 8 min） |
| 23:03:51 | `Lost connection`（2013），已提交 ~80 万行 | 重连 → 从行 `800002` 续传；23:04:45 恢复到 100 万行（恢复 < 1 min） |

单次断连→续传**约 1–10 分钟**（取决于 VM 本次停多久；等待期间进程不退出、不加锁、不占
额外内存）。同行批次内 `batch_id` 保持不变，最终每批只出现唯一的 `LOADED` 批次，行数
= 源文件行数。

**故障 2 — 进程被终止留下 PENDING 批次（手工清理重灌）**

为提高吞吐（§6.3，分块 10k→50k）主动停掉第 1 轮进程，`events.csv` 遗留一个 `PENDING`
批次（#16，已提交约 70 万行）。第 2 轮启动时按"失败批次清理后重跑"流程处理：

```text
22:35:17  INFO loader: batch #16 cleaned: removed 700000 rows from ods_events_raw
           → _cleanup_incomplete / loader.cleanup_batch：
             DELETE ... WHERE ingest_batch_id = 16 → status=CLEANED → 开新批次 #17 重灌
```

清理 + 重灌全程约 **10 分钟**（含等待下一个 MySQL 可用窗口）。验证了三条验收要求：
失败批次可清理、清理后重跑结果与全新导入一致、每行只归属唯一批次。

**故障 3 — 半截批次可被发现（对账依据）**

`etl_batch_log` 与表 `COUNT(*)` 对比是发现半截批次的手段：`PENDING` 批次的
`row_count` 在完成前恒为 0（完成后才回填），但表中 `source_file` 分组计数已含其已提交行；
`LOADED` 批次的 `row_count` 必须等于表中对应 `source_file` 的行数。故障期间两处数字的偏差
均被解释为"在途 PENDING 批次"，无数据丢失，也无不属于任何批次的行。

**故障 4 — 进程被系统杀死后跨进程续传（本次最重的一次）**

加载 `item_properties_part1.csv`（10,999,999 行）期间，后台驱动进程**被系统以内存原因
直接杀死**（会话空闲 + 系统内存吃紧，非 MySQL 故障）。此时批次 #18 已提交 5,600,000 行，
但日志仍为 `PENDING`（尚未 `mark_loaded`）。杀掉进程前这 5.6M 行已落库且已 commit。

重启驱动进程后，加载器在**启动后第一分钟内**自动识别并就地续传，未删任何已提交行：

```text
23:50:53  INFO loader: resume batch #18 for item_properties_part1.csv
          (5600000 rows already committed by the interrupted run)
          → SELECT COUNT(*) WHERE ingest_batch_id = 18 = 5,600,000
          → 从 source_row_number = 5600002 继续写入，batch_id 仍为 18
```

恢复耗时：**进程重启 → 检测到残留批次 → 开始续传 ≈ 数秒**（不含后续加载本身）。
对比故障 2 的"清理 + 重灌"（约 10 分钟、且要重写 70 万行），跨进程续传把影响降到
**零重写、零丢弃**：5.6M 已提交行全部保留，批次对账见 §6.5。

### 6.5 最终导入结果与重复导入对账

**2026-11-06 晚至 11-07 凌晨全量导入完成**（`etl_batch_log` 实测，时间戳为本地时区，
库内以 UTC 存储）。

| 源文件 | 目标表 | 批次 | 最终行数 | batch 跨度（start→finish） | 进程内耗时（loader 计时） | 对账 `COUNT(*)` |
| --- | --- | --- | --- | --- | --- | --- |
| `category_tree.csv` | `ods_category_tree_raw` | #15 | **1,669** | 22:08:51→22:08:51 | 0.1s | 1,669 OK |
| `events.csv` | `ods_events_raw` | #17 | **2,756,101** | 22:35:17→23:13:40 | 2302.9s（约 38 min） | OK |
| `item_properties_part1.csv` | `ods_item_properties_raw` | #18 | **10,999,999** | 23:13:47→00:32:45 | 续传段 2512.5s（约 42 min，含重连等待） | OK |
| `item_properties_part2.csv` | `ods_item_properties_raw` | #28 | **9,275,903** | 00:32:56→01:48:52 | 4555.6s（约 76 min，含重连等待） | OK |
| **合计** | — | — | **23,033,672** | — | — | 全 OK |

说明：

- **耗时是墙钟时间，包含本机每几分钟一次的 VM 重启等待**。`events` 批次跨度含 4 次断连自动
  续传（§6.4 故障 1）；`part1` 跨度含进程被杀后的空窗（23:13 登记 → 23:44 达 5.6M →
  23:50 跨进程续传 → 00:32 完成，§6.4 故障 4）；可用窗口内净写入吞吐约 4–5 万行/秒（§6.3）。
- **重复导入对账（幂等实测）**：全量完成后重跑 `scripts/load_all_ods.py`，四个文件全部
  `skipped: already loaded as batch #…`，脚本退出码 0、`ALL GOOD`；随后独立的
  `SELECT COUNT(*) WHERE source_file=…` 复核仍为
  `1,669 / 2,756,101 / 10,999,999 / 9,275,903`，**行数零增长**。
- **批次一致性**：每个 `LOADED` 批次的 `etl_batch_log.row_count` 与表中
  `ingest_batch_id = batch_id` 的 `COUNT(*)` 逐一相等；清理日志 #16（events 早期 PENDING 批次）
  删尽后 0 行残留、状态 `CLEANED`。全库每个文件的行都唯一归属一个批次，无孤儿行。