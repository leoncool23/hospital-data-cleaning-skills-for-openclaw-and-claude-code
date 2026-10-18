"""
charlson_cci.py
---------------
Compute Charlson Comorbidity Index (CCI) from ICD-10 codes.

算法来源:
    Charlson ME, et al. J Chronic Dis. 1987;40(5):373-83.(原始权重)
    Quan H, et al. Med Care. 2005;43(11):1130-9.(ICD-10 码表,本文件采用)
    年龄加权:Charlson 原始文献即含年龄分(0-4),与合并症分相加。

Usage:
    df = pd.read_csv("diagnoses.csv")          # 一行一条诊断
    cci = compute_cci(df, icd_col="icd10_code", patient_col="patient_id",
                      age_col="age_years")
    # cci 列:patient_id, cci_score, age_points, cci_total, 各分项 0/1 标记

    # 不传 age_col 也大:只算合并症分,不带年龄分
"""

import pandas as pd

# ICD-10 code ranges for each CCI component (Quan 2005)
CCI_COMPONENTS = {
    "myocardial_infarction": {
        "codes": ["I21", "I22", "I252"],
        "weight": 1,
    },
    "congestive_heart_failure": {
        "codes": ["I099", "I110", "I130", "I132", "I255", "I420", "I425", "I426",
                  "I427", "I428", "I429", "I43", "I50", "P290"],
        "weight": 1,
    },
    "peripheral_vascular_disease": {
        "codes": ["I70", "I71", "I731", "I738", "I739", "I771", "I790", "I792",
                  "K551", "K558", "K559", "Z958", "Z959"],
        "weight": 1,
    },
    "cerebrovascular_disease": {
        "codes": ["G45", "G46", "H340", "I60", "I61", "I62", "I63", "I64",
                  "I65", "I66", "I67", "I68", "I69"],
        "weight": 1,
    },
    "dementia": {
        "codes": ["F00", "F01", "F02", "F03", "F051", "G30", "G311"],
        "weight": 1,
    },
    "copd": {
        "codes": ["I278", "I279", "J40", "J41", "J42", "J43", "J44", "J45",
                  "J46", "J47", "J60", "J61", "J62", "J63", "J64", "J65",
                  "J66", "J67", "J684", "J701", "J703"],
        "weight": 1,
    },
    "rheumatic_disease": {
        "codes": ["M05", "M06", "M315", "M32", "M33", "M34", "M351", "M353", "M360"],
        "weight": 1,
    },
    "peptic_ulcer": {
        "codes": ["K25", "K26", "K27", "K28"],
        "weight": 1,
    },
    "mild_liver_disease": {
        "codes": ["B18", "K700", "K701", "K702", "K703", "K709", "K713",
                  "K714", "K715", "K717", "K73", "K74", "K760", "K762",
                  "K763", "K764", "K768", "K769", "Z944"],
        "weight": 1,
    },
    "diabetes_without_complications": {
        "codes": ["E100", "E101", "E106", "E108", "E109", "E110", "E111",
                  "E116", "E118", "E119", "E120", "E121", "E126", "E128",
                  "E129", "E130", "E131", "E136", "E138", "E139", "E140",
                  "E141", "E146", "E148", "E149"],
        "weight": 1,
    },
    "diabetes_with_complications": {
        "codes": ["E102", "E103", "E104", "E105", "E107", "E112", "E113",
                  "E114", "E115", "E117", "E122", "E123", "E124", "E125",
                  "E127", "E132", "E133", "E134", "E135", "E137", "E142",
                  "E143", "E144", "E145", "E147"],
        "weight": 2,
    },
    "hemiplegia_or_paraplegia": {
        "codes": ["G041", "G114", "G801", "G802", "G81", "G82", "G830",
                  "G831", "G832", "G833", "G834", "G839"],
        "weight": 2,
    },
    "renal_disease": {
        "codes": ["I120", "I131", "N032", "N033", "N034", "N035", "N036",
                  "N037", "N052", "N053", "N054", "N055", "N056", "N057",
                  "N18", "N19", "N250", "Z490", "Z491", "Z492", "Z940", "Z992"],
        "weight": 2,
    },
    "any_malignancy": {
        "codes": ["C00", "C01", "C02", "C03", "C04", "C05", "C06", "C07",
                  "C08", "C09", "C10", "C11", "C12", "C13", "C14", "C15",
                  "C16", "C17", "C18", "C19", "C20", "C21", "C22", "C23",
                  "C24", "C25", "C26", "C30", "C31", "C32", "C33", "C34",
                  "C37", "C38", "C39", "C40", "C41", "C43", "C45", "C46",
                  "C47", "C48", "C49", "C50", "C51", "C52", "C53", "C54",
                  "C55", "C56", "C57", "C58", "C60", "C61", "C62", "C63",
                  "C64", "C65", "C66", "C67", "C68", "C69", "C70", "C71",
                  "C72", "C73", "C74", "C75", "C76", "C81", "C82", "C83",
                  "C84", "C85", "C88", "C90", "C91", "C92", "C93", "C94",
                  "C95", "C96", "C97"],
        "weight": 2,
    },
    "moderate_severe_liver_disease": {
        "codes": ["I850", "I859", "I864", "I982", "K704", "K711", "K721",
                  "K729", "K765", "K766", "K767"],
        "weight": 3,
    },
    "metastatic_tumor": {
        "codes": ["C77", "C78", "C79", "C80"],
        "weight": 6,
    },
    "aids_hiv": {
        "codes": ["B20", "B21", "B22", "B24"],
        "weight": 6,
    },
}

# 层级互斥组:(轻, 重)。Charlson 定义里这三组是**互斥**的 ——
# 同一患者同时命中时只计较重那个,不能相加。
# 漏了这一步的典型后果:肝癌患者同时有 C34(恶性肿瘤,2)+ C78(转移瘤,6),
# 会被错算成 8 分而不是 6 分;糖尿病同理。
HIERARCHICAL_PAIRS = [
    ("mild_liver_disease", "moderate_severe_liver_disease"),
    ("diabetes_without_complications", "diabetes_with_complications"),
    ("any_malignancy", "metastatic_tumor"),
]

# 年龄加权分(Charlson 原始文献):50-59=1, 60-69=2, 70-79=3, ≥80=4
AGE_POINTS = [(80, 4), (70, 3), (60, 2), (50, 1)]


def charlson_age_points(age) -> int:
    """按 Charlson 年龄分阶梯返回 0-4 分。缺失/非数值返回 0。"""
    if age is None or pd.isna(age):
        return 0
    try:
        age = float(age)
    except (TypeError, ValueError):
        return 0
    for threshold, points in AGE_POINTS:
        if age >= threshold:
            return points
    return 0


def _icd_matches(icd_code: str, code_list: list) -> bool:
    """Check if ICD code starts with any code in list."""
    if not isinstance(icd_code, str):
        return False
    clean = icd_code.replace(".", "").upper().strip()
    return any(clean.startswith(c.replace(".", "").upper()) for c in code_list)


def compute_cci(df: pd.DataFrame, icd_col: str = "icd10_code",
                patient_col: str = "patient_id",
                age_col: str = None) -> pd.DataFrame:
    """
    Compute Charlson Comorbidity Index per patient.

    Args:
        df: DataFrame with one row per diagnosis per patient
        icd_col: Column name containing ICD-10 codes
        patient_col: Column name for patient identifier
        age_col: 年龄列名。给了就算年龄分并输出 cci_total;不给就只输出合并症分

    Returns:
        DataFrame with patient_id, cci_score(仅合并症), 各分项 0/1 标记;
        若提供了 age_col,额外多 age_points 与 cci_total(= cci_score + age_points)。

    ⚠️ 论文报告时**必须写清用的是哪一种**:只报合并症分(常记 CCI)还是含年龄分
       (常记 CCI-age / Charlson age-comorbidity index)。两者数值不同,不可混用。
    """
    has_age = bool(age_col) and age_col in df.columns
    results = []
    for patient_id, group in df.groupby(patient_col):
        codes = group[icd_col].dropna().tolist()
        flags = {}
        for component, info in CCI_COMPONENTS.items():
            flags[component] = int(any(_icd_matches(c, info["codes"]) for c in codes))

        # 层级互斥:轻症让位于重症,否则同一疾病被重复计分
        for mild, severe in HIERARCHICAL_PAIRS:
            if flags[mild] and flags[severe]:
                flags[mild] = 0

        score = sum(CCI_COMPONENTS[c]["weight"] for c, present in flags.items() if present)
        row = {patient_col: patient_id, "cci_score": score, **flags}

        if has_age:
            age = group[age_col].dropna()
            age_value = age.iloc[0] if len(age) else None
            points = charlson_age_points(age_value)
            row["age_years"] = age_value
            row["age_points"] = points
            row["cci_total"] = score + points
        results.append(row)
    return pd.DataFrame(results)


if __name__ == "__main__":
    # Demo
    demo = pd.DataFrame({
        "patient_id": ["P001", "P001", "P002", "P003", "P003", "P004"],
        "icd10_code": ["I50.0", "E11.9", "C34.1", "K73.9", "K72.9", "E11.0"],
        "age_years":  [72, 72, 45, 81, 81, 58],
    })
    cci = compute_cci(demo, age_col="age_years")
    cols = ["patient_id", "cci_score", "age_points", "cci_total",
            "congestive_heart_failure", "diabetes_without_complications",
            "mild_liver_disease", "moderate_severe_liver_disease", "any_malignancy"]
    print(cci[[c for c in cols if c in cci.columns]].to_string(index=False))
    print()
    print("P003 同时有 K73(轻度肝病)与 K72(中重度肝病)→ 只计中重度 3 分,不叠加为 4")
    print("P001 同时有 I50(心衰)与 E11.9(糖尿病无并发症)→ 1+1=2,加年龄分 3 = 5")
