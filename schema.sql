-- schema.sql —— 任务调度系统核心表结构

-- 任务组：L2 参数覆盖存放在 override_params
CREATE TABLE IF NOT EXISTS task_groups (
    id              bigserial PRIMARY KEY,
    name            text,
    override_params jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(override_params) = 'object')
);

-- 任务：status 状态机 pending -> claimed -> running -> done/failed
-- claim_epoch：单调认领代数——每次认领 +1、回收不清零。
CREATE TABLE IF NOT EXISTS tasks (
    id           bigserial PRIMARY KEY,
    group_id     bigint REFERENCES task_groups(id),
    status       text NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','claimed','running','done','failed')),
    base_params  jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(base_params) = 'object'),
    claimed_by   text,
    claimed_at   timestamptz,
    claim_epoch  bigint NOT NULL DEFAULT 0,
    current_step int NOT NULL DEFAULT 1 CHECK (current_step >= 1),
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);

-- 步骤：L3 参数覆盖存放在 override_params，同一任务内 step_index 唯一
CREATE TABLE IF NOT EXISTS steps (
    id              bigserial PRIMARY KEY,
    task_id         bigint REFERENCES tasks(id),
    step_index      int NOT NULL CHECK (step_index >= 1),
    override_params jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(override_params) = 'object'),
    UNIQUE (task_id, step_index)
);

-- 步骤日志：(task_id, step_index) 为主键，保证 first-report-wins 幂等
CREATE TABLE IF NOT EXISTS step_logs (
    task_id       bigint NOT NULL REFERENCES tasks(id),
    step_index    int NOT NULL CHECK (step_index >= 1),
    success       boolean NOT NULL,
    reported_at   timestamptz NOT NULL DEFAULT now(),
    worker_id     text,
    error_message text,
    channel       text CHECK (channel IN ('worker','manual')),
    claim_epoch   bigint,
    duration_ms   int,
    PRIMARY KEY (task_id, step_index)
);

-- 认领热路径部分索引：只索引 pending 行，认领按 id 升序取首行
CREATE INDEX IF NOT EXISTS idx_tasks_pending ON tasks(id) WHERE status='pending';

-- schema 版本登记表
CREATE TABLE IF NOT EXISTS schema_meta (
    version    int PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO schema_meta (version) VALUES (2) ON CONFLICT DO NOTHING;
