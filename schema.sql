-- schema.sql —— 任务调度系统核心表结构（schema 版本 2）
-- 非破坏性可重复执行总原则：CREATE TABLE/INDEX IF NOT EXISTS + ADD COLUMN
-- IF NOT EXISTS + 幂等 DO 块，对"全新库"与"旧库"重复执行结果一致。
-- 旧库升级路径由文件末尾的迁移段覆盖：补审计列、step_logs→steps 复合外键
-- （先违例预检，大声失败）、idx_tasks_lease 函数式索引重建、schema_meta
-- 建表与版本登记。seed 启动时会断言 schema_meta 版本，防止漏迁移静默运行。
-- 破坏性重建请手工 DROP 五表（按 step_logs/steps/tasks/task_groups/
-- schema_meta 顺序）后再执行本文件，再跑 seed 播种演示数据。

-- 任务组：L2 参数覆盖存放在 override_params
CREATE TABLE IF NOT EXISTS task_groups (
    id              bigserial PRIMARY KEY,
    name            text,
    override_params jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(override_params) = 'object')
);

-- 任务：status 状态机 pending -> claimed -> running -> done/failed
-- current_step CHECK >= 1：step 编号与 steps 表口径统一，禁止 0/负数的幽灵步号；
-- finished_at：done/failed 翻转时由 transition() 打点，预留终态打点，
-- 供排查/审计查询，当前看板未展示。
-- claim_epoch：单调认领代数——每次认领 +1、回收不清零。这是围栏对
-- "回收后重认领同一任务"仍有效的根因：仅靠 claimed_by 的围栏会在
-- 任务被回收（claimed_by=NULL）后被原僵尸 worker 重认领时再次匹配，
-- 带上代际编号后旧 worker 手里的过期 epoch 永远对不上新代。
--
-- 【留档】claimed_by/claimed_at 同空 CHECK 被否决：
--   曾提议 CHECK ((claimed_by IS NULL) = (claimed_at IS NULL)) 保证两者
--   同生同灭。否决理由：reaper 的 reclaim_expired 明确回收 claimed_at IS
--   NULL 的脏行（防御性兜底语义），该 CHECK 会把"待回收的脏数据"变成
--   "非法数据"，并与 tests/test_recovery.py 用例⑧（claimed_at IS NULL
--   脏行回收）直接冲突。保持脏行可存在、由 reaper 收敛。
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
-- error_message：失败上报携带的异常摘要（可空，worker 截断到 500 字符）
-- 审计三列（schema 版本 2 新增，全部可空，存量行 NULL）：
--   channel：上报通道，'worker'=worker 主循环，'manual'=API 手动通道；
--   claim_epoch：worker 上报时持有的代际令牌（手动通道无持有关系，NULL）；
--   duration_ms：该步从开始执行到上报的耗时（毫秒 int，手动通道 NULL）。
-- 复合外键 (task_id, step_index)→steps：日志只能挂在真实存在的步骤上，
--   结构上杜绝幽灵步日志（旧库由迁移 DO 块预检违例后补加）。
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
    PRIMARY KEY (task_id, step_index),
    -- 显式命名：与迁移 DO 块的判存口径（conname）对齐，新库不会被重复添加
    CONSTRAINT step_logs_steps_fkey
        FOREIGN KEY (task_id, step_index) REFERENCES steps(task_id, step_index)
);

-- 认领热路径部分索引：只索引 status='pending' 的行，认领按 id 升序取首行。
-- 相比全量 idx_tasks_status：索引更小（pending 通常只占少数行），
-- 且状态流转不再触发索引维护。
CREATE INDEX IF NOT EXISTS idx_tasks_pending ON tasks(id) WHERE status='pending';
-- reaper 扫描部分索引（schema 版本 2 改为函数式索引）：
-- COALESCE(claimed_at, '-infinity') 让 claimed_at IS NULL 的脏行也进入索引
-- （映射到最小值，租约过期判定必然命中），reclaim_expired 的
-- claimed_at 过滤条件对脏行不再漏扫。旧库同名普通索引由迁移 DO 块比对
-- pg_indexes 定义后 DROP 重建。
CREATE INDEX IF NOT EXISTS idx_tasks_lease
    ON tasks ((COALESCE(claimed_at, '-infinity'::timestamptz)))
    WHERE status IN ('claimed','running');
-- 注：不给 claimed_by 建索引 —— 当前没有任何查询按它过滤（fencing 都是
-- 先按 id 或 status 定位到个位数行后再校验 claimed_by）。

-- ============================================================
-- 旧库升级路径（schema 版本 1 -> 2）：以下全部幂等，可重复执行。
-- ============================================================

-- 补列：为旧结构库补齐审计列与既有历史列（ADD COLUMN IF NOT EXISTS 幂等，
-- 全部可空/带默认，无存量数据回填风险）。
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS finished_at timestamptz;
ALTER TABLE step_logs ADD COLUMN IF NOT EXISTS error_message text;
-- claim_epoch 为旧库补列（认领+1、回收不清零的单调围栏令牌）
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0;
-- schema 版本 2 审计三列
ALTER TABLE step_logs ADD COLUMN IF NOT EXISTS channel text CHECK (channel IN ('worker','manual'));
ALTER TABLE step_logs ADD COLUMN IF NOT EXISTS claim_epoch bigint;
ALTER TABLE step_logs ADD COLUMN IF NOT EXISTS duration_ms int;

-- 幂等 DO 块：① 复合外键（先违例预检，大声失败）；② idx_tasks_lease
-- 函数式索引（比对定义不匹配才重建）；③ schema_meta 建表与版本登记。
DO $$
DECLARE
    v_bad_count   int;
    v_bad_sample  text;
    v_index_def   text;
BEGIN
    -- ① step_logs → steps(task_id, step_index) 复合外键。
    --    加约束前必须先跑违例预检：找出 step_logs 中不存在对应 steps 行
    --    的记录。有违例则 RAISE EXCEPTION 打印行明细并中止（大声失败，
    --    绝不静默跳过或替用户删数据）；无违例才 ADD CONSTRAINT。
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'step_logs_steps_fkey'
          AND conrelid = 'step_logs'::regclass
    ) THEN
        SELECT count(*) INTO v_bad_count
          FROM step_logs sl
         WHERE NOT EXISTS (
             SELECT 1 FROM steps s
             WHERE s.task_id = sl.task_id AND s.step_index = sl.step_index
         );
        IF v_bad_count > 0 THEN
            SELECT string_agg(
                       format('task_id=%s step_index=%s', sl.task_id, sl.step_index),
                       ', ' ORDER BY sl.task_id, sl.step_index)
              INTO v_bad_sample
              FROM (
                  SELECT task_id, step_index FROM step_logs sl2
                  WHERE NOT EXISTS (
                      SELECT 1 FROM steps s2
                      WHERE s2.task_id = sl2.task_id
                        AND s2.step_index = sl2.step_index
                  )
                  LIMIT 20
              ) sl;
            RAISE EXCEPTION
                'schema 迁移中止：step_logs 存在 % 条无对应 steps 行的违例记录（外键预检失败，示例：%）。请人工核对后重跑 schema.sql',
                v_bad_count, v_bad_sample;
        END IF;
        ALTER TABLE step_logs
            ADD CONSTRAINT step_logs_steps_fkey
            FOREIGN KEY (task_id, step_index) REFERENCES steps(task_id, step_index);
    END IF;

    -- ② idx_tasks_lease 函数式索引：比对 pg_indexes 中的现存定义，
    --    不含 COALESCE（即旧版普通索引或结构漂移）才 DROP + 重建；
    --    已是函数式定义则原样保留。
    SELECT indexdef INTO v_index_def
      FROM pg_indexes
     WHERE tablename = 'tasks' AND indexname = 'idx_tasks_lease';
    IF v_index_def IS NOT NULL AND position('COALESCE' IN upper(v_index_def)) = 0 THEN
        DROP INDEX idx_tasks_lease;
    END IF;
    CREATE INDEX IF NOT EXISTS idx_tasks_lease
        ON tasks ((COALESCE(claimed_at, '-infinity'::timestamptz)))
        WHERE status IN ('claimed','running');

    -- ③ schema_meta 建表与版本登记（与文件末尾顶层语句互为双保险，均幂等）
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables WHERE tablename = 'schema_meta'
    ) THEN
        EXECUTE 'CREATE TABLE schema_meta (
            version    int PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )';
    END IF;
    INSERT INTO schema_meta (version) VALUES (2) ON CONFLICT DO NOTHING;
END $$;

-- schema 版本登记表：每次 schema 变更递增版本号；seed 启动断言当前版本，
-- 防止漏迁移的旧库静默运行。
CREATE TABLE IF NOT EXISTS schema_meta (
    version    int PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
-- 当前 schema 版本 = 2（本计划定义）；ON CONFLICT DO NOTHING 保证幂等登记。
INSERT INTO schema_meta (version) VALUES (2) ON CONFLICT DO NOTHING;
