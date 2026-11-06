# Retailrocket E-Commerce Dataset：数据来源与校验记录

## 1. 数据集概况

- **数据集名称**：Retailrocket recommender system dataset（Retailrocket 电商用户行为数据集）
- **原始地址（Kaggle）**：<https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset>
- **内容**：真实电商网站采集的原始行为数据，共三个文件：网页浏览 / 加购 / 交易事件（`events.csv`）、商品属性（`item_properties.csv`，Kaggle 上即按 part1 / part2 拆分存储）、商品类目树（`category_tree.csv`）。
- **Kaggle 元数据**：最后更新 `2022-11-08`；数据集总大小 `987,498,023` 字节；许可 **CC BY-NC-SA 4.0**（非商业 / 署名 / 相同方式共享）。

## 2. 镜像候选评估

> 本机无 Kaggle 凭据，最终选择公开镜像。评估了 4 个候选，采用第 1 个。

| # | 候选 | URL | 是否需登录 | 文件清单 / 大小 | 更新时间 | 许可 | 结论 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | **Hugging Face Datasets（已采用）** | `https://huggingface.co/datasets/DanielKiani/RetailRocket-Recommender-Data`<br>直链前缀 `.../resolve/main/data/RetailRocket-Recommender-Data/data/` | 否（公开、非 gated） | `events.csv` 94,237,913B；`item_properties_part1.csv` 484,315,749B；`item_properties_part2.csv` 408,929,907B；`category_tree.csv` 14,454B（合计 987,498,023B，与 Kaggle totalBytes 完全一致） | 2025-09-19 | HF 卡片未标注许可元数据，内容即 Kaggle 原数据，按原许可 CC BY-NC-SA 4.0 使用 | 采用：文件 / 大小与官方一致，无需凭据，CDN 走本地代理可达 |
| 2 | Kaggle 官方匿名下载接口 | `https://www.kaggle.com/api/v1/datasets/download/retailrocket/ecommerce-dataset`（302 到 GCS 带签名 URL 的 `archive.zip`） | 测试时免登录可用；网页版受 reCAPTCHA 拦截 | `archive.zip`（含四个 csv，约 1GB） | 2022-11-08 | CC BY-NC-SA 4.0 | 最权威但与①同源；匿名访问非官方承诺契约，未采用 |
| 3 | RecBole（RUCAIBox）官方托管 | `https://recbole.s3-accelerate.amazonaws.com/ProcessedDatasets/Retailrocket/merged\|not-merged/retailrocket-{view\|addtocart\|transaction}.zip` | — | 按事件类型拆分、处理的「atomic 变体」，非原始 CSV | — | — | 是处理后的变体而非原始文件；实测返回 403，弃用 |
| 4 | RUCAIBox/RecSysDatasets | `https://github.com/RUCAIBox/RecSysDatasets` | — | Retailrocket 对应条目经 Google Drive / 百度网盘分发，且为处理后变体 | — | — | 大陆直连 Google Drive 不可靠，弃用 |

## 3. 下载与校验信息

- **下载日期**：2026-11-06
- **下载方式**：curl（经本地代理 `127.0.0.1:7897`），原样落盘，未解压、未执行任何文件内容。
- **存储位置**：`data/raw/retailrocket/`（已被 `.gitignore` 忽略，不入库、不提交）。

| 文件 | 字节 | SHA-256 | 行数（不含表头） | 表头 |
| --- | --- | --- | --- | --- |
| `events.csv` | 94,237,913 | `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7` | 2,756,101 | `timestamp,visitorid,event,itemid,transactionid` |
| `item_properties_part1.csv` | 484,315,749 | `30aad5aeca58b2dc27dcc73e1708565f5818e45adb3eb57401f91e87355b0b81` | 10,999,999 | `timestamp,itemid,property,value` |
| `item_properties_part2.csv` | 408,929,907 | `d5e7d1a91dc40f522aeb596b267e6c87d8aed689a7192d12369cfb165eb987e5` | 9,275,903 | `timestamp,itemid,property,value` |
| `category_tree.csv` | 14,454 | `94e865eb0a3d48cbbfe3b79079018dd92509315c88f5fd8d00d0b4b5af434f5b` | 1,669 | `categoryid,parentid` |
| **合计** | **987,498,023** | — | events 2,756,101；item_properties 20,275,902；category_tree 1,669 | — |

**校验方法**：本地逐文件计算 SHA-256，与镜像侧 git-lfs OID（等于文件 SHA-256）逐一比对，全部一致；四文件字节合计 `987,498,023` 与 Kaggle 元数据 `totalBytes` 完全相等。

## 4. 数据内容校验

- **事件类型集合**：`{view, addtocart, transaction}`，仅这三个取值，与 Kaggle 官方口径一致。
- **events 时间范围**（timestamp 为 Unix 毫秒，换算 UTC 日期）：最小 `1430622004384` → **2015-05-03**；最大 `1442545187788` → **2015-09-18**。覆盖约 4 个半月，与官方描述一致。
- **item_properties 合并时间范围**（UTC）：2015-05-10 → 2015-09-13，与 events 时间窗口重叠，符合「属性记录先于/伴随行为事件」的原始说明。
- **category_tree**：1,669 个 `categoryid`，`parentid` 取值 362 个，树结构可联通。

## 5. 与原版差异

- **非采样版**：`events.csv` 行数 2,756,101，与完整版已知量级一致；`item_properties` 合计 20,275,902 行为完整量级。三张表表头均与官方定义完全一致。
- **拆分说明**：`item_properties` 在本镜像及 Kaggle 官方源上均为 `part1` / `part2` 两个文件（非本项目拆分）；两 parts 表头相同，整体可拼接。
- **字节级一致性**：合计字节与 Kaggle `totalBytes` 相同，SHA-256 与镜像元数据一致，未发现采样、截断或改写迹象。

## 6. 许可与使用注意

- Kaggle 元数据声明 **CC BY-NC-SA 4.0**：非商业目的、使用需署名、衍生作品须以相同许可发布。
- 本镜像未在 HF 数据集卡片补充许可元数据，但其内容即 Kaggle 原数据，故按原许可约束使用。
- 本项目为学习 / 个人用途，符合该许可的非商业条件；如后续商用需另行确认授权。

## 7. 复现下载命令（供参考）

```bash
base=https://huggingface.co/datasets/DanielKiani/RetailRocket-Recommender-Data/resolve/main/data/RetailRocket-Recommender-Data/data
curl -L -C - -x http://127.0.0.1:7897 -o events.csv               "$base/events.csv"
curl -L -C - -x http://127.0.0.1:7897 -o item_properties_part1.csv "$base/item_properties_part1.csv"
curl -L -C - -x http://127.0.0.1:7897 -o item_properties_part2.csv "$base/item_properties_part2.csv"
curl -L -C - -x http://127.0.0.1:7897 -o category_tree.csv         "$base/category_tree.csv"
```

下载后以第 3 节表格中的字节数与 SHA-256 核对完整性。