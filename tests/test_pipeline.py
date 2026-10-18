#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tests/test_pipeline.py — 回归测试。

需要 pandas 与 python-dateutil。跑法:

    python tests/test_pipeline.py     # 直接跑,失败返回退出码 1
    pytest tests/                     # 或用 pytest

覆盖四块:
  1. Charlson CCI —— 权重、三组层级互斥、年龄分
  2. 清洗各阶段 —— 缺失 / 异常 / 编码 / 逻辑 / 去重 / 脱敏 / 衍生 / 结构清洗 / 缩放
  3. 小计数抑制 —— 主抑制、二次抑制、0 不抑制
  4. 端到端 + **PHI 回归** —— 画像 JSON 及所有产物不得出现任何 PHI 原值

其中 PHI 回归对应审计发现:原版 profile_data() 会在脱敏前把每列前 3 个原值
写进 00_profile_raw.json,导致姓名/身份证/电话直接落盘。
"""

import json
import sys
import tempfile
import traceback
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from charlson_cci import charlson_age_points, compute_cci  # noqa: E402
import hospital_cleaner as hc  # noqa: E402
import small_count_suppress as scs  # noqa: E402


# ══════════════════════════════════════════════
# 1. Charlson CCI
# ══════════════════════════════════════════════
def test_age_points():
    assert charlson_age_points(49) == 0
    assert charlson_age_points(50) == 1
    assert charlson_age_points(59) == 1
    assert charlson_age_points(60) == 2
    assert charlson_age_points(70) == 3
    assert charlson_age_points(80) == 4
    assert charlson_age_points(95) == 4
    assert charlson_age_points(None) == 0
    assert charlson_age_points("bad") == 0


def test_cci_single_component_weight():
    df = pd.DataFrame({"patient_id": ["A"], "icd10_code": ["I21.0"]})
    out = compute_cci(df)
    assert out.loc[0, "cci_score"] == 1
    assert out.loc[0, "myocardial_infarction"] == 1


def test_cci_hierarchy_not_double_counted():
    """三组层级互斥:轻症必须让位于重症,否则同一疾病被重复计分。"""
    # 肝癌:C34(any_malignancy=2)+ C78(metastatic=6) → 6,不是 8
    out = compute_cci(pd.DataFrame({"patient_id": ["A", "A"],
                                    "icd10_code": ["C34.1", "C78.0"]}))
    assert out.loc[0, "cci_score"] == 6
    assert out.loc[0, "metastatic_tumor"] == 1
    assert out.loc[0, "any_malignancy"] == 0

    # 肝病:K73(轻=1)+ K72(中重度=3) → 3,不是 4
    out = compute_cci(pd.DataFrame({"patient_id": ["B", "B"],
                                    "icd10_code": ["K73.9", "K72.9"]}))
    assert out.loc[0, "cci_score"] == 3

    # 糖尿病:E11.9(无并发症=1)+ E11.5(有并发症=2) → 2,不是 3
    out = compute_cci(pd.DataFrame({"patient_id": ["C", "C"],
                                    "icd10_code": ["E11.9", "E11.5"]}))
    assert out.loc[0, "cci_score"] == 2


def test_cci_total_includes_age():
    out = compute_cci(pd.DataFrame({"patient_id": ["A", "A"],
                                    "icd10_code": ["I50.9", "E11.9"],
                                    "age_years": [72, 72]}), age_col="age_years")
    assert out.loc[0, "cci_score"] == 2
    assert out.loc[0, "age_points"] == 3
    assert out.loc[0, "cci_total"] == 5


def test_cci_without_age_omits_total():
    out = compute_cci(pd.DataFrame({"patient_id": ["A"], "icd10_code": ["I21.0"]}))
    assert "cci_total" not in out.columns
    assert "age_points" not in out.columns


# ══════════════════════════════════════════════
# 2. PHI 识别与画像
# ══════════════════════════════════════════════
def test_is_phi_column_catches_variants_without_false_positives():
    assert hc.is_phi_column("姓名")
    assert hc.is_phi_column("联系电话")
    assert hc.is_phi_column("patient_name")
    assert hc.is_phi_column("身份证号")
    # 合法列不得误伤
    assert not hc.is_phi_column("drug_name")
    assert not hc.is_phi_column("diagnosis_name")
    assert not hc.is_phi_column("patient_id")


def test_profile_omits_phi_by_default():
    df = pd.DataFrame({"patient_id": ["P1"], "姓名": ["张三"], "电话": ["13800138000"]})
    rep = hc.profile_data(df)
    assert rep["sample_values_omitted"] is True
    assert "sample_values" not in rep
    assert "张三" not in json.dumps(rep, ensure_ascii=False)
    assert "13800138000" not in json.dumps(rep, ensure_ascii=False)


def test_profile_samples_skip_phi_columns():
    df = pd.DataFrame({"temperature_c": [36.5], "姓名": ["张三"], "drug_name": ["dupilumab"]})
    rep = hc.profile_data(df, include_samples=True)
    assert "temperature_c" in rep["sample_values"]
    assert "drug_name" in rep["sample_values"]
    assert "姓名" not in rep["sample_values"]
    assert "姓名" in rep["sample_values_skipped_phi_columns"]
    assert "张三" not in json.dumps(rep, ensure_ascii=False)


# ══════════════════════════════════════════════
# 3. 清洗各阶段
# ══════════════════════════════════════════════
def test_handle_missing():
    df = pd.DataFrame({"patient_id": ["P1", None, "P3"], "value": [1.0, None, 3.0]})
    log_entries = []
    out = hc.handle_missing(df, ["patient_id"], log_entries)
    assert "patient_id_critical_missing" in out.columns
    assert int(out["patient_id_critical_missing"].sum()) == 1
    assert int(out["value"].isnull().sum()) == 0
    assert int(out["value_was_missing"].sum()) == 1


def test_detect_outliers_default_keeps_value():
    log_entries = []
    out = hc.detect_outliers(pd.DataFrame({"temperature_c": [36.5, 3.4]}), log_entries)
    assert out.loc[1, "temperature_c"] == 3.4, "默认应保留原值(3.4 多半是 34 的录入错误,删了查不回来)"
    assert out.loc[1, "temperature_c_outlier_flag"] == "HARD_BOUND"
    assert any(e["action"] == "hard_bound_flagged" for e in log_entries)


def test_detect_outliers_can_null_when_asked():
    log_entries = []
    out = hc.detect_outliers(pd.DataFrame({"temperature_c": [36.5, 3.4]}), log_entries,
                             null_hard_bound=True)
    assert pd.isna(out.loc[1, "temperature_c"])
    assert any(e["action"] == "hard_bound_nulled" for e in log_entries)


def test_standardize_encodings():
    df = pd.DataFrame({
        "gender": ["男", "female", "9", "?"],
        "icd10_code": ["I50.9", "BADCODE", "E11", "C34.1"],
        "admit_date": ["2026/01/02", "20260103", "2026-01-04", "2026-01-05"],
    })
    out = hc.standardize_encodings(df, [])
    assert list(out["gender"]) == ["M", "F", "U", "U"]
    assert list(out["icd10_valid"]) == [True, False, True, True]
    assert list(out["admit_date"]) == ["2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]


def test_check_logic_flags_all_three_rules():
    df = pd.DataFrame({"patient_id": ["P1"], "admit_date": ["2026-01-05"],
                       "discharge_date": ["2026-01-01"], "gender": ["M"],
                       "icd10_code": ["N70.0"], "age_years": [-1]})
    out = hc.check_logic(df, [])
    issues = out.loc[0, "logic_issues"]
    assert "discharge_before_admission" in issues
    assert "male_with_female_dx" in issues
    assert "negative_age" in issues


def test_deduplicate():
    df = pd.DataFrame({"patient_id": ["P1", "P1", "P2"],
                       "admit_date": ["2026-01-01"] * 3,
                       "value": [1, 1, 2]})
    out = hc.deduplicate(df, ["patient_id", "admit_date"], [])
    assert len(out) == 2


def test_deidentify_drops_exact_phi_but_only_warns_on_suspected():
    df = pd.DataFrame({"patient_id": ["P1"], "姓名": ["张三"], "联系电话": ["138"],
                       "age_years": [30]})
    log_entries = []
    out = hc.deidentify(df, False, log_entries)
    assert "姓名" not in out.columns
    assert "联系电话" in out.columns, "疑似列只告警不自动删(误删 drug_name 之类更危险)"
    assert "age_group" in out.columns
    assert any(e.get("action") == "suspected_phi_review_required" for e in log_entries)
    assert "patient_id" in out.columns, "patient_id 是联结键,不应被当成 PHI 删掉"


def test_engineer_features_preserves_row_order():
    df = pd.DataFrame({"patient_id": ["P2", "P1", "P2"],
                       "admit_date": ["2026-02-01", "2026-01-01", "2026-01-01"],
                       "discharge_date": ["2026-02-05", "2026-01-10", "2026-01-05"]},
                      index=[10, 11, 12])
    out = hc.engineer_features(df, [])
    assert list(out.index) == [10, 11, 12], "行序必须还原,否则与输入表错位"
    assert list(out["patient_id"]) == ["P2", "P1", "P2"]
    assert out.loc[10, "readmit_30d"] == 1


# ══════════════════════════════════════════════
# 5. 类别 9 — 数据结构清洗
# ══════════════════════════════════════════════
def test_clean_structure_converts_numeric_text():
    """检验值被 Excel 当文本存是医院 CSV 的常态;不转则分位数/异常检测静默跳过。"""
    df = pd.DataFrame({"lab_value": ["12.5", "13", "14.2", "11"],
                       "dept": ["内科", "内科", "外科", "外科"]})
    out = hc.clean_structure(df, [])
    assert pd.api.types.is_numeric_dtype(out["lab_value"])
    assert out.loc[0, "lab_value"] == 12.5
    assert not pd.api.types.is_numeric_dtype(out["dept"])


def test_clean_structure_skips_identifiers_and_leading_zeros():
    df = pd.DataFrame({"patient_id": ["1001", "1002"],
                       "icd10_code": ["I50", "E11"],
                       "ward_no": ["007", "012"],
                       "age": ["70", "60"]})
    out = hc.clean_structure(df, [])
    for col in ("patient_id", "icd10_code", "ward_no"):
        assert not pd.api.types.is_numeric_dtype(out[col]), \
            f"{col} 不该被转成数值(id/code 名或带前导零)"
    assert pd.api.types.is_numeric_dtype(out["age"])


def test_clean_structure_long_wide_roundtrip():
    df = pd.DataFrame({"patient_id": ["P1", "P2"], "wbc": [5.0, 6.0], "crp": [1.0, 2.0]})
    lng = hc.to_long(df, id_vars=["patient_id"], value_vars=["wbc", "crp"])
    assert len(lng) == 4
    assert set(lng["variable"]) == {"wbc", "crp"}
    wide = hc.to_wide(lng, index="patient_id", columns="variable", values="value")
    assert {"patient_id", "wbc", "crp"} <= set(wide.columns)


# ══════════════════════════════════════════════
# 6. 类别 13 — 标准化缩放
# ══════════════════════════════════════════════
def test_standardize_scales_zscore():
    df = pd.DataFrame({"age": [10, 20, 30, 40]})
    out, params = hc.standardize_scales(df, method="zscore")
    assert "age_scaled" in out.columns
    assert "age" in out.columns, "原列必须保留 —— 覆盖是不可逆事故"
    assert abs(out["age_scaled"].mean()) < 1e-9
    assert abs(out["age_scaled"].std(ddof=1) - 1.0) < 1e-9
    assert params["age"]["std"] > 0


def test_standardize_scales_minmax_and_robust():
    df = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0]})
    out, _ = hc.standardize_scales(df, method="minmax", columns=["x"])
    assert out["x_scaled"].min() == 0.0
    assert out["x_scaled"].max() == 1.0
    out2, p2 = hc.standardize_scales(df, method="robust", columns=["x"])
    assert p2["x"]["method"] == "robust"
    assert pd.api.types.is_numeric_dtype(out2["x_scaled"])


def test_standardize_scales_rejects_unknown_method():
    try:
        hc.standardize_scales(pd.DataFrame({"x": [1.0, 2.0]}), method="nope")
    except ValueError:
        return
    raise AssertionError("未知缩放方法应该报错")


def test_standardize_scales_skips_identifier_columns():
    df = pd.DataFrame({"patient_id": [1, 2, 3, 4], "x": [1.0, 2.0, 3.0, 4.0]})
    _, params = hc.standardize_scales(df, method="zscore")
    assert "x" in params
    assert "patient_id" not in params, "ID 列不该被缩放"


def test_apply_scaling_reproduces_training_transform():
    train = pd.DataFrame({"x": [10.0, 20.0, 30.0]})
    _, params = hc.standardize_scales(train, method="zscore", columns=["x"])
    got = hc.apply_scaling(pd.DataFrame({"x": [40.0]}), params)["x_scaled"].iloc[0]
    expect = (40.0 - train["x"].mean()) / train["x"].std()
    assert abs(got - expect) < 1e-9, "测试集必须用训练集参数,否则是数据泄漏"


# ══════════════════════════════════════════════
# 7. 类别 3 — 小计数抑制
# ══════════════════════════════════════════════
def test_suppress_primary_only():
    df = pd.DataFrame({"dept": ["A", "B", "C"], "n": [3, 10, 12]})
    out, info = scs.suppress_small_counts(df, count_col="n", group_cols=["dept"],
                                          threshold=5, secondary=False)
    assert info["primary"] == 1 and info["secondary"] == 0
    assert bool(out.loc[0, "suppressed"]) is True
    assert bool(out.loc[1, "suppressed"]) is False
    assert pd.isna(out.loc[0, "n"])
    assert out.loc[0, "suppression_reason"] == "primary"


def test_suppress_secondary_kicks_in():
    """n=3 被抑制后,合计可反推出它 → 必须再抑制一个最小单元(B=10)。"""
    df = pd.DataFrame({"dept": ["A", "B", "C"], "n": [3, 10, 12]})
    out, info = scs.suppress_small_counts(df, count_col="n", group_cols=["dept"],
                                          threshold=5, secondary=True)
    assert info["primary"] == 1
    assert info["secondary"] == 1
    assert int(out["suppressed"].sum()) == 2
    assert out.loc[1, "suppression_reason"] == "secondary"


def test_suppress_zero_is_not_suppressed():
    df = pd.DataFrame({"dept": ["A", "B"], "n": [0, 7]})
    out, info = scs.suppress_small_counts(df, count_col="n", group_cols=["dept"],
                                          threshold=5, secondary=False)
    assert info["suppressed_rows"] == 0, "零计数不泄露个体,不该被抑制"


def test_suppress_two_dimensional():
    df = pd.DataFrame({"dept": ["A", "A", "B", "B"],
                       "age": ["<5", "5+", "<5", "5+"],
                       "n": [3, 20, 8, 9]})
    out, info = scs.suppress_small_counts(df, count_col="n",
                                          group_cols=["dept", "age"], threshold=5)
    assert info["primary"] == 1
    assert info["suppressed_rows"] >= 2, "列联表里单抑制行/列会连锁触发二次抑制"


def test_suppress_marker_and_raw_aggregation():
    # A 出现 2 次、B 出现 6 次 → 只有 A 需抑制(写完夹具先把计数算清楚再断言)
    raw = pd.DataFrame({"dept": ["A", "A"] + ["B"] * 6})
    out, info = scs.suppress_small_counts(raw, group_cols=["dept"], threshold=5,
                                          secondary=False, marker="<5")
    assert info["primary"] == 1
    assert out.loc[out["suppressed"], "count"].iloc[0] == "<5"
    assert not out.loc[~out["suppressed"], "suppressed"].any()


def test_suppress_rejects_bad_threshold():
    try:
        scs.suppress_small_counts(pd.DataFrame({"n": [1]}), count_col="n", threshold=1)
    except ValueError:
        return
    raise AssertionError("threshold < 2 应该报错")


# ══════════════════════════════════════════════
# 8. 端到端 + PHI 回归
# ══════════════════════════════════════════════
def test_end_to_end_no_phi_leaks_into_outputs():
    src = pd.DataFrame({
        "patient_id": ["P1", "P1", "P2"],
        "姓名": ["张三", "张三", "李四"],
        "电话": ["13800138000", "13800138000", "13900139000"],
        "gender": ["男", "男", "女"],
        "admit_date": ["2026-01-01", "2026-01-01", "2026-02-01"],
        "discharge_date": ["2026-01-10", "2026-01-10", "2026-02-05"],
        "icd10_code": ["I50.9", "I50.9", "N70.0"],
        "temperature_c": [36.5, 36.5, 3.4],
        "age_years": [72, 72, 45],
    })
    with tempfile.TemporaryDirectory() as tmp:
        csv_in = Path(tmp) / "raw.csv"
        src.to_csv(csv_in, index=False, encoding="utf-8-sig")
        out_dir = Path(tmp) / "out"
        hc.run_pipeline(str(csv_in), str(out_dir))

        names = sorted(p.name for p in out_dir.iterdir())
        for prefix in ("00_profile_raw", "01_cleaning_log", "03_cleaned",
                       "04_quality_report"):
            assert any(n.startswith(prefix) for n in names), f"缺少产物 {prefix}* / 实际:{names}"

        blob = ""
        for p in out_dir.iterdir():
            if p.suffix in (".json", ".csv", ".md"):
                blob += p.read_text(encoding="utf-8", errors="ignore")
            elif p.suffix == ".parquet":
                blob += pd.read_parquet(p).to_csv(index=False)
        for phi in ("张三", "李四", "13800138000", "13900139000"):
            assert phi not in blob, f"PHI 泄漏到产物:{phi}"


# ══════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════
def main():
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    passed, failed = 0, []
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  PASS  {name}")
        except Exception:
            failed.append(name)
            print(f"  FAIL  {name}")
            print("        " + traceback.format_exc().replace("\n", "\n        ").rstrip())
    print("\n" + "=" * 56)
    if failed:
        print(f"FAILED {len(failed)}/{len(tests)}: {', '.join(failed)}")
        return 1
    print(f"ALL PASS ({passed} tests)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
