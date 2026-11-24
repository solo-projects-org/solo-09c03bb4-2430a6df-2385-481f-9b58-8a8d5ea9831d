# 码头过磅明细按日数据集服务

把挂载目录里一组**同结构** Parquet 过磅明细，按**过磅时间的 UTC 日期**发布成可按日取文件/取数的数据集；
并提供**净重批次结算 API**：用同一箱的毛重票与皮重票（两次过磅可跨 UTC 日）核算净重。
FastAPI + PyArrow 实现，Docker Compose 启动；原始文件只读挂载、绝不改写。

## 设计要点

### 1. 三级处理路径（尽量不读数据行）

对每个输入文件：

1. **页脚快路径 `reuse_footer`**：只读取 Parquet footer 的列统计（min/max/null_count，不读任何数据行）。
   若能证明时间列**非空且全部落在同一个 UTC 日**，直接在清单里登记原文件路径，零数据读取。
2. **读明细确认 `reuse_scan`**：页脚证明不了（如 `write_statistics=False`、字符串统计无法判定）时，
   只读时间列：确认非空且同日 → **仍复用原档**，不重写。
3. **跨日拆写 `split`**：确认一份文件混了多个 UTC 日时，才读全表并按日过滤，
   写到 `staged/v<版本号>/` 下的新 Parquet（临时文件 + `os.replace`，无半成品）。
   零行文件记为 `empty`，不产生任何按日条目。

### 2. 空时间与发布失败

- 时间列为 null、空白字符串或无法解析 → 立即中止发布（`PublishError`），API 返回 422。
- 写盘失败等任何异常同理：清理本次暂存目录，**`current.json` 不会被替换**。
- 按日取文件/取数的 API 永远只认 `current.json`，因此失败后读到的仍是上一版清单。

### 3. 原子换版与不重不漏

- 新版本的所有拆写文件全部写完、行数按来源文件核对无误后，才把新清单写入临时文件并 `os.replace`
  原子替换 `current.json`（归档到 `versions/manifest-vXXXXXX.json`）。
- 发布时按 `(date, source_file)` 复核行数，保证每条输入记录只在其实际过磅日出现一次，不重不漏。
- 发布进程间用 `flock` 互斥；输入路径限定在 `input_dir` 内，拒绝绝对路径与 `..`。

### 4. UTC 取日规则

- 带时区 timestamp：先转 UTC 再取日（例：北京 11-24 07:30 → UTC 11-23 23:30，归 11-23）。
- 无时区 timestamp/date：按存储字面值取日（落地统一 UTC 的约定）。
- 字符串：按 `WEIGH_TIME_STRING_FORMAT` 解析，未配置则尝试常见 ISO 格式；解析失败按空时间处理。

## 净重批次结算（毛票 + 皮票 → 净重）

运费结算要用**同一箱**的毛重票与皮重票核算净重（净重 = 毛重 − 皮重），两次过磅可能跨 UTC 日。

### 票数据约定

净重 API 读取的票文件需要以下列（列名/取值均可由环境变量配置，见下表）：

| 含义 | 默认列名 | 说明 |
| --- | --- | --- |
| 票号 | `ticket_id` | 与 UTC 日一起唯一定位一张票 |
| 箱号 | `container_no` | 毛/皮两票必须相同 |
| 票种 | `ticket_type` | 毛票取值 `gross`、皮票取值 `tare` |
| 重量 | `weight_kg` | 毛票存毛重、皮票存皮重 |

样例数据里的 `nett_*.parquet` 即此结构；旧结构（只有 `gross_weight_kg`）的按日发布/读取不受影响，
只是不能用于净重结算（缺列时 API 返回 422 并说明缺哪些列）。

### 规则与一致性

1. 请求包含批号、**核对过的清单版号** `manifest_version` 和一组票对；每张票用 `(UTC 日, 票号)` 定位。
2. 每张票在当日必须**唯一命中**（0 条或多条都整批退回）；票对两票箱号相同、毛重不低于皮重、票种正确。
3. 同日同票号在**本批内**与**所有已入库批次**中只能使用一次；冲突会指明占用它的批号。
4. 读票期间**允许发布换版**：净重落库用独立的 `net_batches/.net-batches.lock`，与发布锁互不阻塞。
   落库前重新读取 `current.json`，若清单已换版（版号不符）或复检出任一冲突，**整批退回**并逐条说明原因，
   `batches.json` 不被触碰。
5. 成功后原子写入 `data_out/net_batches/batches.json`，返回每对的**原票定位**
   （UTC 日、票号、来源 original/split、来源文件、文件内 `row_index`）、清单版号与净重。
6. 幂等：
   - **重传已成功批次的原请求**（批号 + 版号 + 票对完全一致）：即使清单已换版，也原样返回首次结果，
     HTTP 200 且 `replayed=true`；
   - **同批号改内容**：返回 409 冲突；
   - 首次成功入库返回 201。

### 可直接试的净重 API 请求

先发布毛/皮票清单（样例文件分别落在 23 日与 24 日，天然跨日）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":[
    "nett_gross_2026-11-23.parquet",
    "nett_tare_2026-11-24.parquet"
  ]}'
```

提交跨日净重批次（毛票 23 日、皮票 24 日）：

```bash
curl -s -X POST http://localhost:8000/net-batches \
  -H 'Content-Type: application/json' \
  -d '{
    "batch_id": "NB-001",
    "manifest_version": 1,
    "pairs": [
      {"gross": {"date": "2026-11-23", "ticket_id": "NG-1001"},
       "tare":  {"date": "2026-11-24", "ticket_id": "NT-2001"}},
      {"gross": {"date": "2026-11-23", "ticket_id": "NG-1002"},
       "tare":  {"date": "2026-11-24", "ticket_id": "NT-2002"}}
    ]
  }' | python -m json.tool
```

原样重传上面的请求：返回 200、`replayed: true`、净重结果与首次完全一致；
发布换版后再重传仍是同样行为。把批号改成 `NB-001` 但换掉票对内容，则返回 409。

问题票整批退回演示（箱号不一致 / 毛重低于皮重，原因逐条返回）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":["nett_conflicts.parquet"]}'

curl -s -X POST http://localhost:8000/net-batches \
  -H 'Content-Type: application/json' \
  -d '{
    "batch_id": "NB-BAD",
    "manifest_version": 2,
    "pairs": [
      {"gross": {"date": "2026-11-23", "ticket_id": "NG-9001"},
       "tare":  {"date": "2026-11-24", "ticket_id": "NT-9001"}},
      {"gross": {"date": "2026-11-23", "ticket_id": "NG-9002"},
       "tare":  {"date": "2026-11-24", "ticket_id": "NT-9002"}}
    ]
  }'
```

按批号查询已入库批次：

```bash
curl -s http://localhost:8000/net-batches/NB-001 | python -m json.tool
```

## 目录结构

```
app/                  应用代码
  config.py           环境变量配置
  timeutil.py         时间列 → UTC 日期
  manifest.py         清单模型与原子换版存储
  publisher.py        发布核心（页脚判定/扫描/拆写/锁/回滚）
  locking.py          flock 进程间互斥锁（发布锁 / 净重批次锁）
  netbatch.py         净重批次：票定位、票对校验、幂等与原子落库
  service.py          按日读取服务
  api.py              FastAPI 路由
  cli.py              命令行入口
docker/               Dockerfile 与 docker-compose.yml
scripts/make_sample_data.py  样例数据生成（含毛/皮票与问题票）
tests/                pytest 测试（40 项）
data_in/              原始 Parquet（compose 只读挂载）
data_out/             current.json、versions/、staged/、net_batches/
```

## Docker Compose 启动

```bash
# 1) 准备样例数据（可选，也可直接挂真实目录）
python scripts/make_sample_data.py ./data_in

# 2) 启动
docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml logs -f
```

环境变量（compose 里已给默认值）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WEIGH_INPUT_DIR` | `/data/input` | 原始 Parquet 目录（容器内只读） |
| `WEIGH_OUTPUT_DIR` | `/data/output` | 清单与拆写产物目录 |
| `WEIGH_TIME_COLUMN` | `weigh_time` | 过磅时间列名 |
| `WEIGH_TIME_STRING_FORMAT` | 空（自动 ISO） | 字符串时间列的 strptime 格式 |
| `WEIGH_LOCK_TIMEOUT` | `10` | 发布/净重批次锁等待秒数 |
| `WEIGH_TICKET_ID_COLUMN` | `ticket_id` | 净重结算票号列 |
| `WEIGH_CONTAINER_COLUMN` | `container_no` | 净重结算箱号列 |
| `WEIGH_TICKET_TYPE_COLUMN` | `ticket_type` | 票种列 |
| `WEIGH_GROSS_TYPE_VALUE` / `WEIGH_TARE_TYPE_VALUE` | `gross` / `tare` | 毛票/皮票取值 |
| `WEIGH_WEIGHT_COLUMN` | `weight_kg` | 重量列（毛重/皮重） |

## 可直接试的 API 请求

健康检查：

```bash
curl -s http://localhost:8000/health
```

发布（文件路径相对 `WEIGH_INPUT_DIR`，必须同结构）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":[
    "batch_a_sameday.parquet",
    "batch_b_mixed.parquet",
    "batch_d_nostats.parquet"
  ]}'
```

查看当前清单：

```bash
curl -s http://localhost:8000/manifest | python -m json.tool
```

按 UTC 日取文件清单（含下载地址、来源 original/split、行数）：

```bash
curl -s http://localhost:8000/days/2026-11-23/files | python -m json.tool
curl -s http://localhost:8000/days/2026-11-24/files | python -m json.tool
```

按 UTC 日直接读记录（多文件自动拼接，`limit` 控制返回行数）：

```bash
curl -s 'http://localhost:8000/days/2026-11-24/rows?limit=100' | python -m json.tool
```

下载当日某个 Parquet（URL 取自 files 接口的 `download_url`）：

```bash
curl -s -o day23-split.parquet \
  "http://localhost:8000/days/2026-11-23/files/split/<base64路径>"
```

失败场景（含空时间的文件，返回 422 且 `retained_version` 指向上一版）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":["batch_a_sameday.parquet","batch_c_null.parquet"]}'
# 再按日取，仍是旧版：
curl -s http://localhost:8000/days/2026-11-23/files
```

交互式文档：启动后访问 `http://localhost:8000/docs`。

## 命令行用法（容器外本地调试）

```bash
python -m app.cli publish batch_a_sameday.parquet batch_b_mixed.parquet
python -m app.cli manifest
python -m app.cli read 2026-11-23
python -m app.cli net-batch NB-001 1 \
  '2026-11-23:NG-1001=2026-11-24:NT-2001' \
  '2026-11-23:NG-1002=2026-11-24:NT-2002'
python -m app.cli net-batch-get NB-001
```

## 测试

```bash
python -m pytest tests/ -q
```

覆盖：页脚快路径零数据读取、无统计降级扫描复用、跨日拆写不重不漏、空时间中止、
模拟写盘失败保留旧版、成功换版、路径穿越/异结构拒绝、字符串与带时区时间、多 row group、
零行文件，以及 API 全流程与失败后回归旧版；净重批次覆盖跨日配票、票不唯一/未命中、
票种/箱号/毛重皮重校验、本批与已入库批次票号复用、清单换版（预检与落库前）、
原请求幂等重放、同批号改内容冲突、缺列 422 与 HTTP 状态码。
