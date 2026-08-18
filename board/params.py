# board/params.py —— 三层参数合并引擎（纯函数，零 I/O）
#
# 三层语义：
#   L1 base        任务创建时的默认参数，是一切的起点
#   L2 group       组级覆盖，整体合入；"" 是普通字面值，正常覆盖
#   L3 step[i]     步骤级覆盖，按顺序逐个应用，且是"粘性"的：
#                  一旦某 key 被非空值覆盖，后续 step 都继承该值
#
# 关键规则（答辩重点）：
#   L3 中值为 "" 的 key 一律【跳过】——它表达"此步不修改该参数"，
#   保留的是【当前生效值】（可能来自 base / L2 / 更早 step 的粘性值），
#   绝不回跳到 base；若该 key 从未被定义过，则保持不存在。
#   注意 "" 的含义在 L2 与 L3 是不对称的：L2 的 "" 是字面值，L3 的 "" 是哨兵。


import copy


def resolve(base: dict, group_override: dict, step_overrides: list) -> list:
    """返回每个 step 的生效参数快照列表（按 step 顺序）。

    参数:
        base           L1 基础参数字典
        group_override L2 组级覆盖字典（可为空）
        step_overrides L3 各 step 的覆盖字典列表（按 step_index 顺序）
    """
    # 步骤0：边界防御：脏数据在入口炸响，不在合并中沉默。
    # 类型错误带类型上下文，方便定位是哪一层传入了脏数据。
    if not isinstance(base, dict):
        raise TypeError(f"base must be dict, got {type(base).__name__}")
    if not isinstance(group_override, dict):
        raise TypeError(f"group_override must be dict, got {type(group_override).__name__}")
    if not isinstance(step_overrides, list):
        raise TypeError(f"step_overrides must be list, got {type(step_overrides).__name__}")
    for i, ov in enumerate(step_overrides):
        if not isinstance(ov, dict):
            raise TypeError(f"step_overrides[{i}] must be dict, got {type(ov).__name__}")

    # 步骤1：起点 = base 的深拷贝（评审修复 4）：既避免污染调用方传入的 base，
    # 也让嵌套值（dict/list）不与入参共享引用，改快照不会改到入参。
    current = copy.deepcopy(base)

    # 步骤2：整体合入 L2。不做任何 "" 特判——L2 的 "" 就是字面值，
    # 直接覆盖 base 中的同名 key；L2 也可以引入 base 没有的新 key。
    for key, value in group_override.items():
        # deepcopy：消除 L2 值与快照之间的别名窗口，改入参不影响已生效状态
        current[key] = copy.deepcopy(value)

    # 步骤3：逐个应用 L3，每个 step 产出一份生效快照
    snapshots = []
    for override in step_overrides:
        for key, value in override.items():
            if value == "":
                # "" 是哨兵：跳过，保留当前生效值。
                # 若 key 从未定义过，这里什么都不做 → key 保持不存在。
                continue
            # 非 "" 值覆盖进 current —— 粘性由此产生：
            # 写入 current 后，后续所有 step 的快照都会继承它。
            # deepcopy：消除 L3 值与快照之间的别名窗口（与 L2 同口径）。
            current[key] = copy.deepcopy(value)
        # 每个 step 记录一份独立的深拷贝，保证快照互不影响，
        # 且嵌套值不与 current/其他快照共享引用（之后互不串改）。
        snapshots.append(copy.deepcopy(current))

    return snapshots
