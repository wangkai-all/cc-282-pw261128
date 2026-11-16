# 数据质量处置平台

仓库数据发布后发现坏记录时的质量处置系统：**隔离并重新发布，而不是删掉坏记录
假装历史没发生**。从空目录实现，仅依赖 Python 3.11 标准库 + 原生前端，SQLite 持久化。

## 启动

```bash
./run_server.sh                 # 默认 http://localhost:8000
# 或：QUALITY_DB=data/quality.db PORT=8000 python3 -m app.server
```

打开浏览器访问 `http://localhost:8000/`，点右上角 **「一键载入演示场景」**
即可获得完整故事线：v2 含坏记录 r1004、三个处于不同消费位置的消费者、
一个待返回的迟到质检任务。

## 测试

```bash
./run_tests.sh
# python3 -I -m unittest discover -s tests -v   （21 个用例）
# node tests/dom_smoke.js                        （前端渲染冒烟）
```

## 核心设计（对应需求逐条落地）

### 1. 不改原始接入记录，坏历史可审计
- `raw_records` 是只追加台账，引擎代码中不存在任何 UPDATE/DELETE 路径；
  每条记录带 `source_hash`，测试逐字段断言处置前后不变。
- 坏数据登记在独立的 `injected_defects`（坏值/正确值/确认状态），
  确认后仅改变「真值」计算，原始行永远保留坏值。
- `versions` 只追加：坏版本永久留在账本，修复永远以**更高版本**表达
  （补偿版本 / 修复快照）。

### 2. 影响面追踪
隔离单按「分区 × 指标 × 坏版本」归并，可追到：
受影响记录、传播到的所有版本、**已读旧版**与**尚未读到**的消费者两组名单、总差额。

### 3. 两种处置策略的真实对比（页面可执行）
| 维度 | 补偿记录（compensation） | 完整新快照（repair_snapshot） |
|---|---|---|
| 修复范围 | 仅受影响分区×指标（重写少量记录） | 全表所有分区×指标（重算全部记录） |
| 等待 | 短，无需全量重算，立即发布 | 长，全量重算 + 等所有消费者换基线 |
| 下游动作 | 拉新版本，按幂等键应用差额 | replace 整份本地状态（快照键原子完成） |
| 重复风险 | 幂等消费者安全；非幂等消费者重试会重复冲减 | replace 本身不重复；漏接快照/误当增量累加会偏离或翻倍 |
| 落后者 | 直接跳到修正后累计，不先坏后好地抖动 | 必须先对齐快照，否则后续增量无基线可挂 |

### 4. 迟到质检进围栏
质检任务带目标版本号；返回时若已有更新版本发布，结果记为 `fenced`：
**只留痕，不产生隔离单、不确认缺陷、不撤销任何新版本**。

### 5. 失败恢复不重复冲减
- 补偿单元携带确定性幂等键 `v{坏版本}:{record_id}`，应用前先落
  `idempotency_keys`；`mid` 崩溃后重试自动跳过已应用单元（页面显示
  skipped_units，对账仍一致）。
- `before_commit` 崩溃整版回滚，无部分效果。
- 快照用 `snapshot:{version}` 单键 + 整版替换保证原子。
- 注册为 **非幂等** 的消费者不写键：重试会真实地重复处理，差异由对账
  如实报出（平台不掩盖下游接线缺陷）。「把快照当增量累加」的错误接线
  也有专门按钮演示，对账会显示翻倍差异。

### 6. 处置期间增量不停
版本号全局单调递增，正常增量与修复版本各自独立发布，消费者顺序消费，
测试覆盖「坏值 + 处置期新增量 + 补偿 = 真值」。

## 页面操作流（10 个面板）

1. 版本账本（只追加，按类型着色） 2. 原始接入记录（可手工接入/发增量/发快照）
3. 质检任务（创建/通过/失败，迟到自动围栏） 4. 坏数据注入与人工确认
5. 隔离单与影响面 6. 补偿 vs 快照对比并一键执行
7. 消费者注册（幂等/非幂等） 8. 下游接收模拟（投递/两种崩溃重试/错误接线/重置）
9. 对账（真值 vs 每个消费者，逐项差异） 10. 审计事件流（全操作留痕，5 秒自动刷新）

## 目录结构

```
app/
  schema.py   SQLite 表结构（12 张表）
  engine.py   领域引擎：接入/版本/质检/隔离/处置/投递/对账
  server.py   标准库 ThreadingHTTPServer，JSON API + 静态页
web/
  index.html  styles.css  app.js   原生前端，无构建、无外部依赖
tests/
  test_engine.py  18 个引擎级用例（不可变、围栏、幂等、对账……）
  test_api.py      3 个 HTTP 端到端故事线
  dom_smoke.js     前端渲染冒烟
  fixture_state.json
run_server.sh / run_tests.sh
```

## 主要 API

`POST /api/ingest` `/api/publish-incremental` `/api/snapshot`
`/api/defect/inject` `/api/defect/confirm`
`/api/qc/create` `/api/qc/complete`
`/api/issue/impact` `/api/disposition/plan` `/api/disposition/execute`
`/api/consumer/register` `/api/consumer/deliver`
`/api/consumer/misuse-snapshot` `/api/consumer/reset`
`/api/demo/seed` `/api/reset`；`GET /api/state` `/api/reconcile`
