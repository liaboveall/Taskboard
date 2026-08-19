# tests/test_params.py —— 参数合并引擎的边界测试（纯逻辑，无需数据库）
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board.params import resolve


# ---------- 粘性传递 ----------

def test_sticky_carries_forward():
    # Step2 override 的值对 Step3..N 持续生效
    snaps = resolve({"a": 1}, {}, [{"b": 10}, {}, {}])
    assert snaps[0]["b"] == 10
    assert snaps[1]["b"] == 10
    assert snaps[2]["b"] == 10


def test_later_step_overrides_same_key():
    # 后续 Step 再次覆盖同一 key，新值生效且继续粘性
    snaps = resolve({"a": 1}, {}, [{"a": 10}, {"a": 20}, {}])
    assert [s["a"] for s in snaps] == [10, 20, 20]


# ---------- 关键链：base a=1 → L2 a=20 → Step2 a=200 → Step3 a="" ----------

def test_critical_chain_no_fallback_to_base():
    snaps = resolve({"a": 1}, {"a": 20}, [{}, {"a": 200}, {"a": ""}])
    assert snaps[0]["a"] == 20     # L2 覆盖 base
    assert snaps[1]["a"] == 200    # Step2 粘性写入
    assert snaps[2]["a"] == 200    # Step3 的 "" 跳过，保留粘性值 200，绝不回跳 1 或 20


# ---------- L2 "" 是字面值 ----------

def test_l2_empty_string_is_literal():
    snaps = resolve({"a": 1}, {"a": ""}, [{}, {}])
    assert snaps[0]["a"] == ""
    assert snaps[1]["a"] == ""


def test_l2_literal_then_l3_empty_keeps_literal():
    # L2 a="" 生效后，L3 a="" 仍保留 ""（不回跳 base 的 1）
    snaps = resolve({"a": 1}, {"a": ""}, [{"a": ""}, {}])
    assert snaps[0]["a"] == ""
    assert snaps[1]["a"] == ""


# ---------- L3 "" 作用于从未定义过的 key ----------

def test_l3_empty_on_never_defined_key_stays_absent():
    snaps = resolve({"a": 1}, {}, [{"ghost": ""}, {}])
    assert "ghost" not in snaps[0]
    assert "ghost" not in snaps[1]


# ---------- 新 key 引入 ----------

def test_new_keys_from_l2_and_l3():
    snaps = resolve({"a": 1}, {"g": "from-l2"}, [{"s": "from-l3"}, {}])
    assert snaps[0]["g"] == "from-l2"
    assert snaps[0]["s"] == "from-l3"
    assert snaps[1]["s"] == "from-l3"  # L3 新 key 也粘性


# ---------- 空 dict ----------

@pytest.mark.parametrize("base,group,steps", [
    ({}, {}, [{}]),
    ({"a": 1}, {}, [{}]),
    ({}, {"a": 1}, [{}]),
    ({"a": 1}, {"b": 2}, [{}]),
    ({}, {}, []),
])
def test_empty_dicts(base, group, steps):
    snaps = resolve(base, group, steps)
    assert len(snaps) == len(steps)
    if steps:
        merged = dict(base)
        merged.update(group)
        assert snaps[0] == merged
    # 输入不被污染
    assert isinstance(base, dict)


# ---------- Step1 即带 override ----------

def test_first_step_has_override():
    snaps = resolve({"a": 1}, {}, [{"a": 9, "b": 8}])
    assert snaps[0] == {"a": 9, "b": 8}


# ---------- 多 key 混合演变的完整轨迹（对照表逐 Step 断言） ----------

def test_full_trajectory_table():
    base = {"a": 1, "b": 2, "c": 3}
    group = {"a": 10, "d": 4}                 # L2: 覆盖 a，引入 d
    steps = [
        {"b": 20, "e": ""},                   # Step1: b 粘性=20；e 从未定义→不存在
        {"a": "", "c": ""},                   # Step2: a/c 保留当前生效值
        {"a": 100},                           # Step3: a 再次覆盖
        {},                                   # Step4: 纯继承
    ]
    snaps = resolve(base, group, steps)
    expect = [
        {"a": 10, "b": 20, "c": 3, "d": 4},
        {"a": 10, "b": 20, "c": 3, "d": 4},
        {"a": 100, "b": 20, "c": 3, "d": 4},
        {"a": 100, "b": 20, "c": 3, "d": 4},
    ]
    assert snaps == expect
    assert all("e" not in s for s in snaps)


# ---------- 快照独立性：修改返回值不影响后续/再次调用 ----------

def test_snapshots_are_independent_copies():
    snaps = resolve({"a": 1}, {}, [{"a": 2}, {}])
    snaps[0]["a"] = 999
    assert snaps[1]["a"] == 2
    # 原始输入未被污染
    base = {"a": 1}
    resolve(base, {"a": 5}, [{"b": 6}])
    assert base == {"a": 1}


# ---------- 嵌套结构深拷贝 ----------

def test_nested_deep_copy_isolation():
    # 快照里的嵌套 dict/list 是深拷贝：改一个快照不影响 base 与兄弟快照
    base = {"cfg": {"retries": 1, "tags": ["a"]}}
    snaps = resolve(base, {}, [{}, {}])
    snaps[0]["cfg"]["retries"] = 99
    snaps[0]["cfg"]["tags"].append("b")
    assert base == {"cfg": {"retries": 1, "tags": ["a"]}}    # base 未被污染
    assert snaps[1]["cfg"] == {"retries": 1, "tags": ["a"]}  # 兄弟快照未受影响


def test_override_nested_not_aliased_to_input():
    # override 里的嵌套值也不与调用方传入的 dict 共享引用
    ov = {"cfg": {"x": 1}}
    snaps = resolve({"cfg": {"y": 0}}, {}, [ov, {}])
    snaps[0]["cfg"]["x"] = 99
    assert ov == {"cfg": {"x": 1}}       # 入参未被污染
    assert snaps[1]["cfg"]["x"] == 1     # 后续快照的粘性值不受影响


# ---------- 嵌套 dict override：整体替换而非深合并（行为锁定） ----------

def test_nested_dict_override_replaces_whole_dict_not_deep_merge():
    """刻意取舍的行为锁定，防止未来误改成深合并：
    嵌套 dict 的 override 是【整体替换】而非逐 key 深合并——
    L2 只给 cfg.x 时，base 里的 cfg.y 会消失，而不是被保留。"""
    snaps = resolve({"cfg": {"x": 1, "y": 2}}, {"cfg": {"x": 9}}, [{}, {}])
    assert snaps[0]["cfg"] == {"x": 9}   # y 消失：整体替换
    assert snaps[1]["cfg"] == {"x": 9}   # 整体替换后的值照常粘性


# ---------- 假值不是哨兵：仅精确等于 "" 才触发跳过 ----------

@pytest.mark.parametrize("val", [0, False, None, "0", " "])
def test_falsy_values_are_literal_not_sentinel(val):
    # 防回归：若实现被误改成 not value / value == "" 的宽匹配，这里会红
    snaps = resolve({"a": 1}, {}, [{"a": val}, {}])
    assert snaps[0]["a"] == val
    assert snaps[1]["a"] == val          # 且照常粘性


# ---------- L2 引入的新 key 被 L3 "" 命中：保留 L2 值 ----------

def test_l3_empty_after_l2_new_key_keeps_l2_value():
    snaps = resolve({"a": 1}, {"g": "l2"}, [{"g": ""}, {}])
    assert snaps[0]["g"] == "l2"        # 不回跳 base（base 里根本没有 g）
    assert snaps[1]["g"] == "l2"


# ---------- group / step_overrides 入参不被污染 ----------

def test_group_and_step_inputs_not_mutated():
    group = {"g": {"nested": 1}}
    steps = [{"s": {"nested": 2}}]
    resolve({"a": 1}, group, steps)
    assert group == {"g": {"nested": 1}}
    assert steps == [{"s": {"nested": 2}}]


# ---------- 类型防御：脏数据在入口炸响（覆盖全部四个防御分支） ----------

def test_base_type_error():
    # 分支 1：base 非 dict → TypeError 且带类型上下文
    with pytest.raises(TypeError, match=r"base must be dict, got str"):
        resolve("not-a-dict", {}, [{}])


def test_group_override_type_error():
    # 分支 2：group_override 非 dict → TypeError（即使 base 合法）
    with pytest.raises(TypeError, match=r"group_override must be dict, got list"):
        resolve({"a": 1}, ["not-a-dict"], [{}])


def test_step_overrides_type_error():
    # 分支 3：step_overrides 非 list → TypeError
    with pytest.raises(TypeError, match=r"step_overrides must be list, got dict"):
        resolve({"a": 1}, {}, {"a": 2})


def test_step_overrides_element_type_error():
    # 分支 4：step_overrides 内元素非 dict → TypeError 且带下标定位
    with pytest.raises(TypeError, match=r"step_overrides\[1\] must be dict, got str"):
        resolve({"a": 1}, {}, [{}, "not-a-dict"])


def test_type_check_happens_before_merge():
    # 防御在合并之前：脏数据不应产生任何副作用（快照列表从未被构造返回），
    # 且合法入参不被脏分支误伤
    with pytest.raises(TypeError):
        resolve(None, {}, [{}])
    snaps = resolve({"a": 1}, {}, [{}])
    assert snaps == [{"a": 1}]
