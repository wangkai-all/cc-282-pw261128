"""SQLite 表结构定义。

设计原则：
- raw_records 是原始接入记录，只追加、永不 UPDATE/DELETE（由 engine 层约束，
  代码中不存在对该表的修改/删除路径），隔离与修复都通过新版本表达。
- 版本(versions)只追加，坏版本保留在历史中，不物理删除。
- 消费者侧状态与平台账本严格分表，模拟不同下游接收行为。
"""

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- 原始接入记录：不可变台账
CREATE TABLE IF NOT EXISTS raw_records (
    record_id     TEXT PRIMARY KEY,
    stream        TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    metric        TEXT NOT NULL,
    amount        REAL NOT NULL,
    event_time    TEXT NOT NULL,
    received_at   TEXT NOT NULL,
    source_hash   TEXT NOT NULL
);

-- 坏数据嫌疑/确认登记（独立于 raw_records，不改动原始记录）
CREATE TABLE IF NOT EXISTS injected_defects (
    defect_id      TEXT PRIMARY KEY,
    record_id      TEXT NOT NULL UNIQUE,
    bad_amount     REAL NOT NULL,
    correct_amount REAL NOT NULL,
    note           TEXT,
    confirmed      INTEGER NOT NULL DEFAULT 0,   -- 1=质检确认，确认后才进入真值
    confirmed_at   TEXT,
    injected_at    TEXT NOT NULL
);

-- 发布版本（只追加）
CREATE TABLE IF NOT EXISTS versions (
    version      INTEGER PRIMARY KEY AUTOINCREMENT,
    version_type TEXT NOT NULL,                 -- incremental/snapshot/compensation/repair_snapshot
    note         TEXT,
    payload_json TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

-- 版本-原始记录包含关系（影响追踪索引；快照版本也登记当时全量记录）
CREATE TABLE IF NOT EXISTS version_records (
    version   INTEGER NOT NULL,
    record_id TEXT NOT NULL,
    PRIMARY KEY (version, record_id)
);

-- 质检任务
CREATE TABLE IF NOT EXISTS qc_tasks (
    task_id      TEXT PRIMARY KEY,
    version      INTEGER NOT NULL,
    status       TEXT NOT NULL,                 -- pending/passed/failed/fenced
    result_json  TEXT,
    created_at   TEXT NOT NULL,
    completed_at TEXT
);

-- 质量问题（隔离单）
CREATE TABLE IF NOT EXISTS issues (
    issue_id      TEXT PRIMARY KEY,
    state         TEXT NOT NULL,                -- isolated/fenced/repairing/repaired
    partition_key TEXT NOT NULL,
    metric        TEXT NOT NULL,
    bad_version   INTEGER NOT NULL,
    task_id       TEXT,
    record_ids_json TEXT NOT NULL,
    bad_total     REAL NOT NULL,
    expected_total REAL NOT NULL,
    disposition_id TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

-- 处置单
CREATE TABLE IF NOT EXISTS dispositions (
    disposition_id TEXT PRIMARY KEY,
    strategy       TEXT NOT NULL,               -- compensation/snapshot
    issue_ids_json TEXT NOT NULL,
    repair_version INTEGER,
    rationale_json TEXT NOT NULL,
    state          TEXT NOT NULL,               -- created/released
    created_at     TEXT NOT NULL,
    released_at    TEXT
);

-- 消费者
CREATE TABLE IF NOT EXISTS consumers (
    consumer_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    mode        TEXT NOT NULL,                  -- idempotent/non_idempotent
    last_offset INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
);

-- 消费者物化状态：分区×指标累计值
CREATE TABLE IF NOT EXISTS consumer_state (
    consumer_id   TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    metric        TEXT NOT NULL,
    total         REAL NOT NULL,
    base_version  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_id, partition_key, metric)
);

-- 消费者版本投递轨迹
CREATE TABLE IF NOT EXISTS consumer_versions (
    consumer_id  TEXT NOT NULL,
    version      INTEGER NOT NULL,
    status       TEXT NOT NULL,                 -- delivered/crashed/idempotent_skipped
    attempts     INTEGER NOT NULL DEFAULT 0,
    delivered_at TEXT,
    PRIMARY KEY (consumer_id, version)
);

-- 幂等键（崩溃恢复不重复冲减的核心）
CREATE TABLE IF NOT EXISTS idempotency_keys (
    consumer_id TEXT NOT NULL,
    idem_key    TEXT NOT NULL,
    version     INTEGER NOT NULL,
    applied_at  TEXT NOT NULL,
    PRIMARY KEY (consumer_id, idem_key)
);

-- 审计事件流
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    kind        TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
"""
