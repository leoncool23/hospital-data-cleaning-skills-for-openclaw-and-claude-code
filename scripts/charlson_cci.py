"""
charlson_cci.py
---------------
Compute Charlson Comorbidity Index from ICD-10 codes.
Reference: Quan et al. 2005 (ICD-10 coding algorithm).
"""

import pandas as pd
import re

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


def _icd_matches(icd_code: str, code_list: list) -> bool:
    """Check if ICD code starts with any code in list."""
    if not isinstance(icd_code, str):
        return False
    clean = icd_code.replace(".", "").upper().strip()
    return any(clean.startswith(c.replace(".", "").upper()) for c in code_list)


def compute_cci(df: pd.DataFrame, icd_col: str = "icd10_code",
                patient_col: str = "patient_id") -> pd.DataFrame:
    """
    Compute Charlson Comorbidity Index per patient.

    Args:
        df: DataFrame with one row per diagnosis per patient
        icd_col: Column name containing ICD-10 codes
        patient_col: Column name for patient identifier

    Returns:
        DataFrame with patient_id and cci_score
    """
    # Build patient × diagnosis matrix
    results = []
    for patient_id, group in df.groupby(patient_col):
        codes = group[icd_col].dropna().tolist()
        score = 0
        flags = {}
        for component, info in CCI_COMPONENTS.items():
            present = any(_icd_matches(c, info["codes"]) for c in codes)
            flags[component] = int(present)
            if present:
                score += info["weight"]
        results.append({patient_col: patient_id, "cci_score": score, **flags})
    return pd.DataFrame(results)


if __name__ == "__main__":
    # Demo
    demo = pd.DataFrame({
        "patient_id": ["P001", "P001", "P002"],
        "icd10_code": ["I50.0", "E11.9", "C34.1"],
    })
    cci = compute_cci(demo)
    print(cci[["patient_id", "cci_score", "congestive_heart_failure",
               "diabetes_without_complications", "any_malignancy"]])
