#!/usr/bin/env python3
"""
hospital_cleaner.py
-------------------
Hospital data cleaning pipeline.

覆盖 SKILL.md 15 类中的以下 12 类(其余 3 类只有规则描述、无脚本实现):

    0  数据画像            profile_data()
    1  缺失值处理          handle_missing()
    2  异常值与错误值      detect_outliers()
    3  小样本抑制          scripts/small_count_suppress.py
    4  重复数据            deduplicate()
    5  逻辑一致性          check_logic()
    6  编码标准化          standardize_encodings()
    7  时间数据            build_temporal_vars()
    8  隐私脱敏            deidentify()
    9  数据结构清洗        clean_structure() / to_long() / to_wide()
    12 衍生变量            engineer_features() / charlson_cci.py
    13 标准化缩放          standardize_scales() / apply_scaling()
    15 质控与可追溯        generate_quality_report()

尚未实现(SKILL.md 有描述、脚本无对应函数):
    10 文本与 NLP 清洗 / 11 数据来源核对与血缘 / 14 数据集成

Usage:
    python hospital_cleaner.py --input raw_data.csv --output output/
    python hospital_cleaner.py --input raw_data.parquet --output output/ --config config.json
"""

import argparse
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from dateutil import parser as dateutil_parser

# ─────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("hospital_cleaner")

# ─────────────────────────────────────────────
# Configuration defaults
# ─────────────────────────────────────────────
VITAL_BOUNDS = {
    "temperature_c":   (34.0, 43.0),
    "heart_rate":      (20,   300),
    "systolic_bp":     (40,   300),
    "diastolic_bp":    (20,   200),
    "spo2_pct":        (50,   100),
    "respiratory_rate":(4,    80),
    "blood_glucose":   (0.5,  55.0),
    "age_years":       (0,    120),
    "weight_kg":       (0.3,  500),
    "height_cm":       (30,   250),
}

GENDER_MAP = {
    '男': 'M', '女': 'F', '未知': 'U', 'unknown': 'U',
    'male': 'M', 'female': 'F', 'm': 'M', 'f': 'F',
    '1': 'M', '2': 'F', '9': 'U',
}

DATE_FORMATS = [
    '%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%m/%d/%Y',
    '%Y%m%d', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M',
]

# ─────────────────────────────────────────────
# PHI 字段识别(画像取样与脱敏共用)
# ─────────────────────────────────────────────
# 精确字段名:命中即在 deidentify() 里删除或伪名化
PHI_FIELDS = [
    "patient_name", "name", "姓名",
    "id_number", "national_id", "身份证",
    "phone", "mobile", "telephone", "电话",
    "address", "住址",
    "email", "邮箱",
]

# 子串模式:用于**告警**和**画像取样跳过**。
# 刻意不含裸 "name",否则会命中 drug_name / diagnosis_name 这类合法列。
PHI_PATTERNS = [
    "姓名", "身份证", "电话", "手机", "住址", "邮箱",
    "监护人", "家属", "联系人", "联系方式", "病历号", "住院号", "门诊号",
    "patient_name", "id_number", "national_id", "mrn", "phone", "mobile",
    "telephone", "address", "email", "contact",
]


def is_phi_column(col) -> bool:
    """列名是否疑似 PHI。宁多勿漏,误报由人工复核。"""
    name = str(col).lower()
    return any(p in name for p in PHI_PATTERNS)


# ─────────────────────────────────────────────
# 文本列判定(跨 pandas 2/3)
# ─────────────────────────────────────────────
def _is_text_series(s: pd.Series) -> bool:
    """是否为文本列。

    ⚠️ pandas 3.0 起字符串列的 dtype 是 ``str`` 而不再是 ``object``,
    因此 ``s.dtype == "object"`` 在新版上**恒为假、整段逻辑静默跳过**。
    原版用这个判断做日期标准化,导致日期原样流入输出且不报错。
    """
    if isinstance(s.dtype, pd.CategoricalDtype):
        return False
    return bool(pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s))


def _text_columns(df: pd.DataFrame) -> list:
    """文本列名列表。不用 select_dtypes("object"):pandas 3 会对它发弃用告警。"""
    return [c for c in df.columns if _is_text_series(df[c])]


# ─────────────────────────────────────────────
# Phase 0 — Data Profiling
# ─────────────────────────────────────────────
def profile_data(df: pd.DataFrame, include_samples: bool = False) -> dict:
    """Generate pre/post cleaning profile report.

    Args:
        include_samples: 是否把每列前 3 个**原值**写进画像 JSON。

    ⚠️ 默认 False。画像是在脱敏**之前**生成的,原值可能包含姓名、身份证、电话;
    一旦落盘,01/02/03 号文件再干净也白搭 —— 直接违反"清洗日志不得含 PHI"的核心原则。
    仅在已确认数据不含 PHI 时才开;即便开启,疑似 PHI 列也会被跳过。
    """
    report = {
        "timestamp": datetime.now().isoformat(),
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "missing_pct": {c: round(float(df[c].isnull().mean() * 100), 2) for c in df.columns},
        "duplicate_rows": int(df.duplicated().sum()),
        "dtypes": {c: str(df[c].dtype) for c in df.columns},
        "nunique": {c: int(df[c].nunique(dropna=True)) for c in df.columns},
        "sample_values_omitted": not include_samples,
    }

    if include_samples:
        skipped = [c for c in df.columns if is_phi_column(c)]
        report["sample_values"] = {
            c: [str(v)[:40] for v in df[c].dropna().head(3).tolist()]
            for c in df.columns if c not in skipped
        }
        if skipped:
            report["sample_values_skipped_phi_columns"] = skipped
        log.warning(
            "画像已开启原值输出(--profile-samples / profile_samples=true)。"
            "请确认数据不含 PHI;疑似 PHI 列已跳过:%s", skipped or "无"
        )
    else:
        log.info("画像未输出任何原值(默认)。如需诊断取样请显式开启 --profile-samples。")

    log.info(f"Profile: {report['rows']} rows, {report['columns']} cols, "
             f"{report['duplicate_rows']} duplicates")
    return report


# ─────────────────────────────────────────────
# Category 1 — Missing Data
# ─────────────────────────────────────────────
# 列名像标识符/编码 —— 这类列不做数值强转(会丢前导零、破坏编码语义)
_ID_LIKE_PATTERNS = [
    "id", "code", "编号", "编码", "卡号", "病历号", "住院号", "门诊号",
    "phone", "tel", "mobile", "zip", "postcode", "icd", "序号",
]


def _looks_like_identifier(col) -> bool:
    name = str(col).lower()
    return any(k in name for k in _ID_LIKE_PATTERNS)


# ═══════════════════════════════════════════════
# 类别 9 — 数据结构清洗
# ═══════════════════════════════════════════════
def clean_structure(df: pd.DataFrame, log_entries: list, coerce_numeric: bool = True,
                    numeric_parse_rate: float = 0.95,
                    categorical_max_unique: int = 0) -> pd.DataFrame:
    """类型规范:把"看着是数字的文本列"转为数值型,可选降为 category。

    Args:
        coerce_numeric: 医院导出的 CSV 里检验值常被 Excel 当作文本存,
            不转的话分位数 / 均值 / IQR 异常检测会**静默跳过**这些列 ——
            不报错,但结果是错的。
        numeric_parse_rate: 判定阈值 —— 非空值中可解析为数字的最低比例。
        categorical_max_unique: >0 时,把唯一值数不超过它的文本列转为 category。

    安全规则(避免把编码/标识符转坏):
        * 列名像标识符/编码(id、code、编号、卡号、ICD、序号…)→ 跳过
        * 疑似 PHI 列 → 跳过
        * 任意非空值带前导零(如 "007")→ 跳过,避免丢零
    """
    df = df.copy()
    converted, skipped_zero, made_cat = [], [], []

    if coerce_numeric:
        for col in _text_columns(df):
            if _looks_like_identifier(col) or is_phi_column(col):
                continue
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            cleaned = (non_null.astype(str).str.strip()
                       .str.replace(",", "", regex=False))
            if cleaned.str.match(r'^[+-]?0\d').any():
                skipped_zero.append(col)
                continue
            parse_rate = pd.to_numeric(cleaned, errors="coerce").notna().mean()
            if parse_rate >= numeric_parse_rate:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.strip().str.replace(",", "", regex=False),
                    errors="coerce")
                converted.append(col)

    if categorical_max_unique > 0:
        for col in _text_columns(df):
            if is_phi_column(col):
                continue
            n_unique = df[col].nunique(dropna=True)
            if 0 < n_unique <= categorical_max_unique:
                df[col] = df[col].astype("category")
                made_cat.append(col)

    if converted:
        log.info(f"结构清洗:文本→数值 {len(converted)} 列 {converted}")
        log_entries.append({"category": "structure", "action": "text_to_numeric",
                            "fields": converted})
    if skipped_zero:
        log.warning(f"含前导零,已跳过数值转换(保护编码):{skipped_zero}")
        log_entries.append({"category": "structure", "action": "skipped_leading_zero",
                            "fields": skipped_zero})
    if made_cat:
        log_entries.append({"category": "structure", "action": "to_category",
                            "fields": made_cat})
    return df


def to_long(df, id_vars, value_vars=None, var_name="variable", value_name="value",
            log_entries=None) -> pd.DataFrame:
    """宽表 → 长表(melt)。"""
    out = df.melt(id_vars=id_vars, value_vars=value_vars,
                  var_name=var_name, value_name=value_name)
    if log_entries is not None:
        log_entries.append({"category": "structure", "action": "wide_to_long",
                            "rows_from": int(len(df)), "rows_to": int(len(out))})
    return out


def to_wide(df, index, columns, values, log_entries=None, aggfunc="first") -> pd.DataFrame:
    """长表 → 宽表(pivot);重复 (index, columns) 组合按 aggfunc 聚合。"""
    out = df.pivot_table(index=index, columns=columns, values=values,
                         aggfunc=aggfunc).reset_index()
    if log_entries is not None:
        log_entries.append({"category": "structure", "action": "long_to_wide",
                            "rows_from": int(len(df)), "rows_to": int(len(out))})
    return out


def handle_missing(df: pd.DataFrame, critical_fields: list, log_entries: list) -> pd.DataFrame:
    """Handle missing values by field priority."""
    df = df.copy()

    # Block on critical fields
    for field in critical_fields:
        if field in df.columns:
            n_missing = df[field].isnull().sum()
            if n_missing > 0:
                log.warning(f"CRITICAL FIELD '{field}' has {n_missing} nulls — flagging rows")
                df[f"{field}_critical_missing"] = df[field].isnull().astype(int)
                log_entries.append({"category": "missing", "field": field,
                                    "action": "flagged_critical", "count": int(n_missing)})

    # Numeric fields: add missing indicator, then impute with group median
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    for col in numeric_cols:
        if col in critical_fields:
            continue
        n_miss = df[col].isnull().sum()
        if n_miss == 0:
            continue
        miss_pct = n_miss / len(df)
        df[f"{col}_was_missing"] = df[col].isnull().astype(int)
        if miss_pct <= 0.5:
            df[col] = df[col].fillna(df[col].median())
            log_entries.append({"category": "missing", "field": col,
                                 "action": "median_imputation", "count": int(n_miss)})
        else:
            log.warning(f"Field '{col}' has {miss_pct:.1%} missing — dropping column")
            df.drop(columns=[col, f"{col}_was_missing"], inplace=True)
            log_entries.append({"category": "missing", "field": col,
                                 "action": "dropped_high_missing", "count": int(n_miss)})

    # Categorical fields: fill with 'Unknown'
    # ⚠️ 不要写 select_dtypes(include=["object", "category"]):pandas 3 下字符串列
    #    的 dtype 是 "str",依赖旧兼容行为会发 Pandas4Warning 并在未来失效。
    cat_cols = _text_columns(df) + [c for c in df.columns
                                    if isinstance(df[c].dtype, pd.CategoricalDtype)]
    for col in cat_cols:
        if col in critical_fields:
            continue
        n_miss = df[col].isnull().sum()
        if n_miss > 0:
            df[col] = df[col].fillna("Unknown")
            log_entries.append({"category": "missing", "field": col,
                                 "action": "fill_unknown", "count": int(n_miss)})
    return df


# ─────────────────────────────────────────────
# Category 2 — Outlier Detection
# ─────────────────────────────────────────────
def detect_outliers(df: pd.DataFrame, log_entries: list,
                    null_hard_bound: bool = False) -> pd.DataFrame:
    """Detect and flag outliers using hard bounds and IQR.

    Args:
        null_hard_bound: 硬边界外的值是否直接置空。
            默认 False —— 保留原值并只打标记,符合 SKILL.md 2.3
            "异常值子类分类(不直接删除,先分类标记)"。
            置空会丢失信息(3.4℃ 多半是 34℃ 的录入错误,删了就查不回来了)。

    ⚠️ SKILL.md 2.4 要求"单位统一必须在异常检测之前"。本函数不做单位换算,
       调用方须保证传入的列已是标准单位,否则硬边界判定会误杀。
    """
    df = df.copy()

    # Hard boundary rules
    for field, (low, high) in VITAL_BOUNDS.items():
        if field not in df.columns:
            continue
        mask = (df[field] < low) | (df[field] > high)
        n = int(mask.sum())
        if n > 0:
            df[f"{field}_outlier_flag"] = np.where(mask, "HARD_BOUND", "OK")
            if null_hard_bound:
                df.loc[mask, field] = np.nan
                action = "hard_bound_nulled"
            else:
                action = "hard_bound_flagged"
            log_entries.append({"category": "outlier", "field": field,
                                 "action": action, "count": n})

    # IQR statistical outliers for remaining numeric fields
    for col in df.select_dtypes(include=[np.number]).columns:
        if col.endswith("_was_missing") or col.endswith("_flag"):
            continue
        Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
        IQR = Q3 - Q1
        if IQR == 0:
            continue
        low_iqr = Q1 - 3.0 * IQR
        high_iqr = Q3 + 3.0 * IQR
        mask = (df[col] < low_iqr) | (df[col] > high_iqr)
        n = int(mask.sum())
        if n > 0:
            flag_col = f"{col}_outlier_flag"
            if flag_col not in df.columns:
                df[flag_col] = "OK"
            df.loc[mask & (df[flag_col] == "OK"), flag_col] = "STAT_OUTLIER"
            log_entries.append({"category": "outlier", "field": col,
                                 "action": "stat_outlier_flagged", "count": n})
    return df


# ─────────────────────────────────────────────
# Category 4 — Deduplication
# ─────────────────────────────────────────────
def deduplicate(df: pd.DataFrame, dedup_keys: list, log_entries: list) -> pd.DataFrame:
    """Remove exact duplicates; flag fuzzy patient duplicates."""
    df = df.copy()
    n_before = len(df)

    # Exact row duplicates
    df.drop_duplicates(inplace=True)
    n_exact = n_before - len(df)
    log_entries.append({"category": "dedup", "action": "exact_rows_removed", "count": n_exact})

    # Event-level duplicates (same patient + key + datetime)
    if all(k in df.columns for k in dedup_keys):
        n_before2 = len(df)
        df = df.sort_values("created_time", ascending=False) if "created_time" in df.columns else df
        df = df.drop_duplicates(subset=dedup_keys, keep="first")
        n_event = n_before2 - len(df)
        log_entries.append({"category": "dedup", "action": "event_duplicates_removed",
                             "count": n_event})
    return df


# ─────────────────────────────────────────────
# Category 5 — Logical Consistency
# ─────────────────────────────────────────────
def check_logic(df: pd.DataFrame, log_entries: list) -> pd.DataFrame:
    """Flag records failing logical consistency rules."""
    df = df.copy()
    df["logic_issues"] = ""

    # Temporal rules
    temporal_checks = [
        ("admit_date", "discharge_date", "discharge_before_admission"),
        ("birth_date", "admit_date", "admission_before_birth"),
        ("surgery_start", "surgery_end", "surgery_end_before_start"),
    ]
    for col_a, col_b, label in temporal_checks:
        if col_a in df.columns and col_b in df.columns:
            mask = pd.to_datetime(df[col_a], errors='coerce') > pd.to_datetime(df[col_b], errors='coerce')
            n = int(mask.sum())
            if n > 0:
                df.loc[mask, "logic_issues"] += f"|{label}"
                log_entries.append({"category": "logic", "rule": label, "count": n})

    # Clinical rules
    if "gender" in df.columns and "icd10_code" in df.columns:
        mask = (df["gender"] == "M") & df["icd10_code"].fillna("").str.startswith(("N7", "O"))
        n = int(mask.sum())
        if n > 0:
            df.loc[mask, "logic_issues"] += "|male_with_female_dx"
            log_entries.append({"category": "logic", "rule": "male_female_dx", "count": n})

    if "age_years" in df.columns:
        mask = df["age_years"] < 0
        n = int(mask.sum())
        if n > 0:
            df.loc[mask, "logic_issues"] += "|negative_age"
            log_entries.append({"category": "logic", "rule": "negative_age", "count": n})

    return df


# ─────────────────────────────────────────────
# Category 6 — Encoding Standardization
# ─────────────────────────────────────────────
def standardize_encodings(df: pd.DataFrame, log_entries: list) -> pd.DataFrame:
    """Standardize gender, ICD codes, and units."""
    df = df.copy()

    # Gender
    if "gender" in df.columns:
        df["gender_original"] = df["gender"]
        df["gender"] = (df["gender"].astype(str).str.strip().str.lower()
                        .map(GENDER_MAP).fillna("U"))
        n = int((df["gender"] == "U").sum())
        log_entries.append({"category": "encoding", "field": "gender",
                             "action": "standardized", "unmapped": n})

    # ICD-10 format validation
    if "icd10_code" in df.columns:
        icd_pattern = re.compile(r'^[A-Z]\d{2}(\.\d{1,4})?$')
        mask = ~df["icd10_code"].fillna("").apply(
            lambda x: bool(icd_pattern.match(x.upper())) if x else False
        )
        n = int(mask.sum())
        if n > 0:
            df["icd10_valid"] = ~mask
            log_entries.append({"category": "encoding", "field": "icd10_code",
                                 "action": "invalid_flagged", "count": n})

    # Date fields
    # ⚠️ 原版写的是 `if df[col].dtype == "object"`,pandas 3.0 起字符串 dtype 是 "str",
    #    该条件恒为假 → 日期标准化整段静默空转,原始格式直接流到输出。
    date_fields = [c for c in df.columns if any(k in c.lower() for k in ["date", "time", "dt"])]
    for col in date_fields:
        if _is_text_series(df[col]):
            df[col] = df[col].apply(_safe_parse_date)
            log_entries.append({"category": "encoding", "field": col,
                                 "action": "date_standardized"})

    # String cleanup
    for col in _text_columns(df):
        df[col] = df[col].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip()
                                 if pd.notnull(x) else x)
    return df


def _safe_parse_date(s) -> str | None:
    if pd.isnull(s) or str(s).strip() in ("", "nan", "None"):
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(str(s).strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    try:
        return dateutil_parser.parse(str(s)).strftime('%Y-%m-%d')
    except Exception:
        return None


# ─────────────────────────────────────────────
# Category 7 — Temporal Variables
# ─────────────────────────────────────────────
def build_temporal_vars(df: pd.DataFrame, log_entries: list) -> pd.DataFrame:
    """Compute derived time variables."""
    df = df.copy()

    if "admit_date" in df.columns and "discharge_date" in df.columns:
        df["los_days"] = (
            pd.to_datetime(df["discharge_date"], errors="coerce") -
            pd.to_datetime(df["admit_date"], errors="coerce")
        ).dt.days
        log_entries.append({"category": "temporal", "action": "computed_los_days"})

    if "symptom_onset" in df.columns and "diagnosis_date" in df.columns:
        df["days_to_diagnosis"] = (
            pd.to_datetime(df["diagnosis_date"], errors="coerce") -
            pd.to_datetime(df["symptom_onset"], errors="coerce")
        ).dt.days
        log_entries.append({"category": "temporal", "action": "computed_days_to_diagnosis"})
    return df


# ─────────────────────────────────────────────
# Category 8 — De-identification
# ─────────────────────────────────────────────
def deidentify(df: pd.DataFrame, keep_pseudonym: bool, log_entries: list) -> pd.DataFrame:
    """Remove or pseudonymize PHI fields.

    精确名命中 → 删除或伪名化;疑似列(子串命中但不在 PHI_FIELDS)→ **只告警不删**。
    自动删疑似列会误伤 drug_name / diagnosis_name 之类的合法列,漏删比误删好救。
    """
    df = df.copy()
    removed = []

    suspected = [c for c in df.columns if is_phi_column(c) and c not in PHI_FIELDS]
    if suspected:
        log.warning(f"疑似 PHI 列(未自动删除,请人工确认后再决定):{suspected}")
        log_entries.append({"category": "deidentify",
                            "action": "suspected_phi_review_required",
                            "fields": suspected})
    for field in PHI_FIELDS:
        if field in df.columns:
            if keep_pseudonym and field in ("patient_name", "name"):
                import hashlib
                df[field] = df[field].apply(
                    lambda x: "SUBJ_" + hashlib.sha256(str(x).encode()).hexdigest()[:8]
                    if pd.notnull(x) else x
                )
                log_entries.append({"category": "deidentify", "field": field,
                                     "action": "pseudonymized"})
            else:
                df.drop(columns=[field], inplace=True)
                removed.append(field)
    if removed:
        log_entries.append({"category": "deidentify", "fields_removed": removed})

    # Age banding for quasi-identifier protection
    if "age_years" in df.columns:
        bins   = [0, 1, 5, 15, 45, 65, np.inf]
        labels = ['<1', '1-4', '5-14', '15-44', '45-64', '65+']
        df["age_group"] = pd.cut(df["age_years"], bins=bins, labels=labels, right=False)
        log_entries.append({"category": "deidentify", "action": "age_banded"})
    return df


# ─────────────────────────────────────────────
# Category 12 — Feature Engineering
# ─────────────────────────────────────────────
def engineer_features(df: pd.DataFrame, log_entries: list) -> pd.DataFrame:
    """Build commonly needed derived variables."""
    df = df.copy()

    # BMI
    if "weight_kg" in df.columns and "height_cm" in df.columns:
        df["bmi"] = (df["weight_kg"] / (df["height_cm"] / 100) ** 2).round(1)
        log_entries.append({"category": "features", "action": "computed_bmi"})

    # Readmission flag (requires sorted data by patient + date)
    if "patient_id" in df.columns and "admit_date" in df.columns and "discharge_date" in df.columns:
        original_order = df.index
        df_sorted = df.sort_values(["patient_id", "admit_date"])
        df_sorted["prev_discharge"] = df_sorted.groupby("patient_id")["discharge_date"].shift(1)
        df_sorted["days_since_discharge"] = (
            pd.to_datetime(df_sorted["admit_date"], errors="coerce") -
            pd.to_datetime(df_sorted["prev_discharge"], errors="coerce")
        ).dt.days
        df_sorted["readmit_30d"] = (df_sorted["days_since_discharge"] <= 30).astype(int)
        # ⚠️ 必须还原行序:否则输出的 03_cleaned 与输入行一一对应关系被破坏,
        #    下游按行合并/比对会静默错位。
        df = df_sorted.drop(columns=["prev_discharge"]).reindex(original_order)
        log_entries.append({"category": "features", "action": "computed_readmit_30d"})
    return df


# ─────────────────────────────────────────────
# Category 15 — Quality Report
# ─────────────────────────────────────────────
SCALE_METHODS = ("zscore", "minmax", "robust")


# ═══════════════════════════════════════════════
# 类别 13 — 数据标准化 / 缩放
# ═══════════════════════════════════════════════
def standardize_scales(df: pd.DataFrame, method: str = "zscore", columns=None,
                       log_entries=None, suffix: str = "_scaled"):
    """标准化/缩放。

    ⚠️ **仅用于建模前**。描述性统计与论文里的原始数值表都不要缩放,否则量纲失去意义。

    默认生成新列 ``<col>_scaled``,**不覆盖原列** —— 缩放不可逆地丢掉尺度,
    覆盖是这类脚本最常见的事故来源。

    Returns:
        (df_out, params):params 记录每列的变换参数,可用 apply_scaling() 对
        新数据(测试集/后续队列)施加**完全相同**的变换。
    """
    if method not in SCALE_METHODS:
        raise ValueError(f"未知缩放方法 {method!r};可选 {SCALE_METHODS}")
    log_entries = log_entries if log_entries is not None else []
    df = df.copy()
    params = {}

    if columns is None:
        columns = [c for c in df.columns
                   if pd.api.types.is_numeric_dtype(df[c])
                   and not c.endswith(("_was_missing", "_flag", suffix))
                   and not _looks_like_identifier(c)]

    for col in columns:
        if col not in df.columns:
            log.warning(f"缩放跳过不存在的列:{col}")
            continue
        s = pd.to_numeric(df[col], errors="coerce")

        if method == "zscore":
            mu, sd = s.mean(), s.std()
            if pd.isna(sd) or sd == 0:
                log.warning(f"列 {col} 标准差为 0,跳过 z-score")
                continue
            df[f"{col}{suffix}"] = (s - mu) / sd
            params[col] = {"method": "zscore", "mean": float(mu), "std": float(sd)}
        elif method == "minmax":
            lo, hi = s.min(), s.max()
            if pd.isna(lo) or hi == lo:
                log.warning(f"列 {col} 取值范围为单点,跳过 min-max")
                continue
            df[f"{col}{suffix}"] = (s - lo) / (hi - lo)
            params[col] = {"method": "minmax", "min": float(lo), "max": float(hi)}
        else:  # robust:对极端值不敏感,医院数据里比 z-score 更稳
            med = s.median()
            iqr = s.quantile(0.75) - s.quantile(0.25)
            if pd.isna(iqr) or iqr == 0:
                log.warning(f"列 {col} IQR 为 0,跳过 robust 缩放")
                continue
            df[f"{col}{suffix}"] = (s - med) / iqr
            params[col] = {"method": "robust", "median": float(med), "iqr": float(iqr)}

        log_entries.append({"category": "scaling", "field": col, "action": method})

    if params:
        log.info(f"已缩放 {len(params)} 列(method={method});参数可用 apply_scaling() 复用")
    return df, params


def apply_scaling(df: pd.DataFrame, params: dict, suffix: str = "_scaled") -> pd.DataFrame:
    """用 standardize_scales() 返回的 params 对新数据施加**相同**变换。

    训练集/测试集必须用同一套参数,否则既是数据泄漏也是量纲不一致。
    """
    df = df.copy()
    for col, p in params.items():
        if col not in df.columns:
            log.warning(f"apply_scaling:目标数据缺少列 {col},跳过")
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if p["method"] == "zscore":
            df[f"{col}{suffix}"] = (s - p["mean"]) / p["std"]
        elif p["method"] == "minmax":
            span = (p["max"] - p["min"]) or 1
            df[f"{col}{suffix}"] = (s - p["min"]) / span
        elif p["method"] == "robust":
            df[f"{col}{suffix}"] = (s - p["median"]) / (p["iqr"] or 1)
    return df


def generate_quality_report(df_before: pd.DataFrame, df_after: pd.DataFrame,
                              log_entries: list, output_dir: Path) -> str:
    """Generate a markdown quality report."""
    completeness = {c: round((1 - df_after[c].isnull().mean()) * 100, 1)
                    for c in df_after.columns}
    avg_completeness = round(sum(completeness.values()) / len(completeness), 1)

    report = f"""# Data Quality Report
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
| Metric | Before | After |
|--------|--------|-------|
| Rows | {len(df_before):,} | {len(df_after):,} |
| Columns | {len(df_before.columns)} | {len(df_after.columns)} |
| Avg Completeness | — | {avg_completeness}% |
| Duplicate rows | {df_before.duplicated().sum():,} | {df_after.duplicated().sum():,} |

## Cleaning Actions Log
| Category | Action | Count |
|----------|--------|-------|
"""
    for entry in log_entries:
        cat = entry.get("category", "")
        action = entry.get("action", str(entry))
        count = entry.get("count", "—")
        report += f"| {cat} | {action} | {count} |\n"

    report += "\n## Field Completeness\n| Field | Completeness |\n|-------|-------------|\n"
    for field, pct in sorted(completeness.items()):
        status = "✅" if pct >= 95 else ("🟡" if pct >= 80 else "🔴")
        report += f"| {field} | {status} {pct}% |\n"

    out_path = output_dir / f"04_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    out_path.write_text(report, encoding="utf-8")
    log.info(f"Quality report saved: {out_path}")
    return str(out_path)


# ─────────────────────────────────────────────
# Main Pipeline
# ─────────────────────────────────────────────
def _write_dataset(df: pd.DataFrame, output_path: Path, ts: str, log_entries: list) -> Path:
    """写出清洗后数据集:优先 parquet,缺引擎时退化为 CSV。

    ⚠️ parquet 需要 pyarrow / fastparquet。医院机器上常常没有,
    若直接 to_parquet 会在流水线**最后一步**抛异常 —— 前面所有清洗都白做。
    这里退化为 CSV(utf-8-sig,Excel 直接双击不乱码),并把降级记进日志。
    """
    target = output_path / f"03_cleaned_{ts}.parquet"
    try:
        df.to_parquet(target, index=False)
        return target
    except (ImportError, ValueError, AttributeError) as exc:
        csv_target = output_path / f"03_cleaned_{ts}.csv"
        df.to_csv(csv_target, index=False, encoding="utf-8-sig")
        log.warning(f"parquet 写入不可用({type(exc).__name__});已改写 CSV:{csv_target}")
        log.warning("如需 parquet:pip install pyarrow")
        log_entries.append({"category": "output", "action": "parquet_unavailable_wrote_csv",
                            "detail": str(exc)[:120]})
        return csv_target


def run_pipeline(input_path: str, output_dir: str, config: dict = None) -> None:
    config = config or {}
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load data
    if input_path.endswith(".parquet"):
        df = pd.read_parquet(input_path)
    else:
        df = pd.read_csv(input_path, low_memory=False)
    log.info(f"Loaded {len(df):,} rows from {input_path}")

    # Profile raw data
    # ⚠️ 画像在脱敏之前生成,默认不输出任何原值,避免 PHI 落盘
    raw_profile = profile_data(df, include_samples=bool(config.get("profile_samples", False)))
    with open(output_path / "00_profile_raw.json", "w") as f:
        json.dump(raw_profile, f, indent=2, default=str)

    df_original = df.copy()
    log_entries = []

    # Execute pipeline
    critical_fields = config.get("critical_fields", ["patient_id", "admit_date"])
    dedup_keys      = config.get("dedup_keys",      ["patient_id", "admit_date"])

    # 类别 9 — 先做结构清洗(文本→数值)。否则后续分位数/异常检测会静默跳过文本列
    df = clean_structure(df, log_entries,
                         coerce_numeric=bool(config.get("coerce_numeric_types", True)))

    df = handle_missing(df, critical_fields, log_entries)
    df = deduplicate(df, dedup_keys, log_entries)
    df = standardize_encodings(df, log_entries)
    df = build_temporal_vars(df, log_entries)
    df = detect_outliers(df, log_entries,
                         null_hard_bound=bool(config.get("null_hard_bound", False)))
    df = check_logic(df, log_entries)
    df = engineer_features(df, log_entries)

    if config.get("deidentify", True):
        df = deidentify(df, config.get("keep_pseudonym", False), log_entries)
    else:
        log.warning("deidentify=False:输出文件将**保留原始 PHI 列**。"
                    "仅在已获伦理批准且落盘环境受控时才这样用。")
        log_entries.append({"category": "deidentify", "action": "skipped_by_config"})

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 类别 13 — 标准化缩放:默认关闭,只在建模前显式开启
    scale_method = config.get("scale_method")
    if scale_method:
        df, scale_params = standardize_scales(df, method=scale_method,
                                              columns=config.get("scale_columns"),
                                              log_entries=log_entries)
        with open(output_path / f"05_scaler_params_{ts}.json", "w", encoding="utf-8") as f:
            json.dump(scale_params, f, indent=2, ensure_ascii=False)
        log.info(f"缩放参数已存:{len(scale_params)} 列 → 05_scaler_params_{ts}.json")

    # Save outputs
    _write_dataset(df, output_path, ts, log_entries)

    log_df = pd.DataFrame(log_entries)
    log_df.to_csv(output_path / f"01_cleaning_log_{ts}.csv", index=False)

    flagged = df[df.get("logic_issues", pd.Series(dtype=str)).str.len() > 0] \
        if "logic_issues" in df.columns else pd.DataFrame()
    if len(flagged):
        flagged.to_csv(output_path / f"02_flagged_records_{ts}.csv", index=False)
        log.info(f"{len(flagged):,} flagged records saved")

    generate_quality_report(df_original, df, log_entries, output_path)
    log.info("Pipeline complete.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Hospital Data Cleaning Pipeline")
    ap.add_argument("--input",  required=True, help="Input CSV or Parquet file path")
    ap.add_argument("--output", default="output", help="Output directory")
    ap.add_argument("--config", default=None, help="JSON config file path")
    ap.add_argument("--profile-samples", action="store_true",
                    help="画像 JSON 中输出每列前 3 个原值。默认关闭;数据含 PHI 时切勿开启")
    args = ap.parse_args()

    cfg = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, encoding="utf-8") as f:
            cfg = json.load(f)
    if args.profile_samples:
        cfg["profile_samples"] = True

    run_pipeline(args.input, args.output, cfg)
