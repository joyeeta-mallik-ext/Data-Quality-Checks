"""


Purpose
-------
This Streamlit application provides a Data Quality Insights Portal for:
1. Mandatory DQC monitoring
2. AI-generated DQ rule review
3. QC_RULE_MASTER record preparation
4. Dashboarding and drill-down analytics
5. Pipeline execution monitoring

"""

import hashlib
import importlib
import json
import os
import re
from typing import Dict, Iterable, List, Optional, Tuple
import subprocess
import sys
import pandas as pd
import plotly.express as px
import streamlit as st
import time
from Predifined_Rule_Checks import get_connection
import traceback



# =============================================================================
# Runtime pipeline context helpers
# =============================================================================
PIPELINE_CONTEXT_FILE = "pipeline_context.json"


def load_pipeline_context(base_dir: str) -> dict:
    """"
    Purpose: Loads the latest pipeline execution context (saved by run_full_pipeline.py) from a JSON file.
    Input: base_dir (str): Folder path where pipeline_context.json is expected.
    Output: dict containing pipeline context data if file exists and is valid.
    Returns {} (empty dictionary) if:
        File does not exist.
        JSON reading/parsing fails.
    """
    # Load latest batch output paths written by run_full_pipeline.py.
    context_path = os.path.join(base_dir, PIPELINE_CONTEXT_FILE)

    if not os.path.exists(context_path):
        return {}

    try:
        with open(context_path, "r", encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except Exception:
        return {}


def resolve_runtime_file(base_dir: str, context_key: str, fallback_file_name: str) -> str:
    """Purpose: Resolves the correct runtime file path for the UI to use.

    Input:
    base_dir → Base folder where files are stored.
    context_key → Key name to look up in pipeline_context.json.
    fallback_file_name → Default file name to use if the context file is unavailable.

    Output:
    Returns a string containing the file path to be used by the UI."""

    context = load_pipeline_context(base_dir)
    context_file = context.get(context_key, "")

    if context_file:
        if os.path.isabs(context_file) and os.path.exists(context_file):
            return context_file

        relative_context_file = os.path.join(base_dir, context_file)
        if os.path.exists(relative_context_file):
            return relative_context_file

        if os.path.exists(context_file):
            return context_file

    return os.path.join(base_dir, fallback_file_name)

def is_pipeline_ready(base_dir: str, context_key: str) -> bool:
    context = load_pipeline_context(base_dir)

    if not context:
        return False

    file_path = context.get(context_key)

    if not file_path:
        return False

    if os.path.exists(file_path):
        return True

    if os.path.exists(os.path.join(base_dir, file_path)):
        return True

    return False
def render_editable_selected_rules(selected_df: pd.DataFrame, widget_prefix: str) -> pd.DataFrame:
    """Purpose: Allows users to edit selected DQ rules in the UI, save their changes, and retain them for preview/download.
    
    Input: selected_df (pd.DataFrame) → Selected rules dataframe.
    widget_prefix (str) → Unique prefix ("mandatory" or "ai") used to create unique Streamlit widget/session keys.

    Output: pd.DataFrame → Updated/edited selected rules dataframe stored in session state."""

    selected_df = ensure_target_columns(selected_df)

    edit_key = f"{widget_prefix}_edited_selected_rules"
    signature_key = f"{widget_prefix}_selected_rules_signature"

    current_signature = "|".join(
        (
            selected_df["RULE_DESCRIPTION"].astype(str)
            + "||SOURCE_NAME="
            + selected_df["SOURCE_NAME"].astype(str)
        ).tolist()
    )

    if (
        edit_key not in st.session_state
        or st.session_state.get(signature_key) != current_signature
    ):
        st.session_state[edit_key] = selected_df.copy()
        st.session_state[signature_key] = current_signature

    edited_df = st.data_editor(
        st.session_state[edit_key],
        use_container_width=True,
        num_rows="fixed",
        disabled=["RULE_ACTIVE"],
        key=f"{widget_prefix}_selected_rules_editor",
    )

    if st.button("💾 Save Edited Rules", key=f"{widget_prefix}_save_edited_rules"):
        edited_df = ensure_target_columns(edited_df)
        edited_df["RULE_ACTIVE"] = RULE_ACTIVE_INACTIVE_VALUE
        st.session_state[edit_key] = edited_df.copy()

        if widget_prefix == "ai":
            st.session_state.selected_qc_rule_rows = edited_df.copy()

        st.success("Edited selected rules saved successfully.")

    return st.session_state[edit_key].copy()

# =============================================================================
# Streamlit page configuration
# =============================================================================
st.set_page_config(layout="wide", page_title="Data Quality Insights")


# =============================================================================
# Constants
# =============================================================================
RULE_ACTIVE_INACTIVE_VALUE = "N"
PAGE_SIZE = 10

TARGET_COLUMNS = [
    "RULEGROUP",
    "RULEID",
    "JOBNUMBER",
    "JOBNAME",
    "SOURCE_NAME",
    "ENTITY",
    "DG_KPI",
    "REJECT_COLUMNNAME",
    "RULE_DESCRIPTION",
    "RULE_SQL",
    "THRESHOLD__",
    "CONTACT_DISTRIBUTION_LIST",
    "RULE_ACTIVE",
    "UPDATE_QUERY",
]

ENTITY_JOB_MAPPING = {
    "MDS_HCP_ADDR_DATA_FULL": {
        "ENTITY": "HCP_ADDRESS",
        "JOBNAME": "job_MDS_mdm_lndg_to_stg_hcp_addr",
        "LND_TABLE": "stg.lndg_src_hcp_addr",
    },
    "MDS_HCP_PRFL_DATA_FULL": {
        "ENTITY": "HCP_PROFILE",
        "JOBNAME": "job_MDS_mdm_lndg_to_stg_hcp_profl",
        "LND_TABLE": "stg.lndg_src_hcp_profl",
    },
    "MDS_HCP_LIC_DATA_FULL": {
        "ENTITY": "HCP_LICENSE",
        "JOBNAME": "job_MDS_mdm_lndg_to_stg_hcp_lic",
        "LND_TABLE": "stg.lndg_src_hcp_lic",
    },
    "MDM_HCO_ADDR_DATA_FULL": {
        "ENTITY": "HCO_ADDRESS",
        "JOBNAME": "job_mdm_lndg_to_stg_hco_addr",
        "LND_TABLE": "stg.lndg_src_hco_addr",
    },
    "MDM_HCO_PRFL_DATA_FULL": {
        "ENTITY": "HCO_PROFILE",
        "JOBNAME": "job_mdm_lndg_to_stg_hco_profl",
        "LND_TABLE": "stg.lndg_src_hco_profl",
    },
    "MDM_HCO_LIC_DATA_FULL": {
        "ENTITY": "HCO_LICENSE",
        "JOBNAME": "job_mdm_lndg_to_stg_hco_lic",
        "LND_TABLE": "stg.lndg_src_hco_lic",
    },
}

MODEL_TABLE_PATTERN = r"cph_db_dev\.MODEL_V2\.[A-Za-z0-9_]+"


# =============================================================================
# Generic reusable helpers
# =============================================================================
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of the dataframe with uppercase, trimmed column names."""
    df = df.copy()
    df.columns = df.columns.astype(str).str.upper().str.strip()
    return df


def enforce_rule_active_inactive(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Force RULE_ACTIVE = "N" on a QC_RULE_MASTER-style dataframe."""
    if df is None:
        return None

    df = df.copy()
    df.columns = df.columns.str.upper().astype(str).astype(str).str.strip()
    df["RULE_ACTIVE"] = RULE_ACTIVE_INACTIVE_VALUE
    return df


def ensure_target_columns(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Ensure all TARGET_COLUMNS exist, force RULE_ACTIVE = "N", and order columns."""
    if df is None or df.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = enforce_rule_active_inactive(df)

    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[TARGET_COLUMNS]
    df = df.fillna("")
    for col in df.columns:
        df[col] = df[col].astype(str)
    return df
    




def apply_selected_source_name(selected_df: pd.DataFrame, source_options: List[str], widget_prefix: str) -> pd.DataFrame:
    """Apply a Source CD selected from existing source values as SOURCE_NAME."""
    st.markdown("### Source Name")
    st.caption("Select a source name to apply it to the selected rules and QC Master preview.")
    available_sources = [str(x).strip() for x in source_options if str(x).strip() and str(x).strip() != "Choose options"]
    source_name = st.selectbox(
        "Source Name",
        ["Choose options"] + sorted(set(available_sources)),
        key=f"{widget_prefix}_source_name",
        label_visibility="collapsed",
    )
    selected_df = ensure_target_columns(selected_df)
    if source_name != "Choose options" and not selected_df.empty:
        selected_df = selected_df.copy()
        selected_df["SOURCE_NAME"] = source_name
    return selected_df


# =============================================================================
# QC_RULE_MASTER insert preview helpers
# =============================================================================
def get_landing_table_from_qc_row(row: pd.Series) -> str:
    """Return the landing table for an HCP/HCO profile, address, or license QC row."""
    entity = str(row.get("ENTITY", "") or "").strip().upper()
    job_name = str(row.get("JOBNAME", "") or "").strip().lower()

    entity_landing_map = {
        "HCP_PROFILE": "stg.lndg_src_hcp_profl",
        "HCP_ADDRESS": "stg.lndg_src_hcp_addr",
        "HCP_LICENSE": "stg.lndg_src_hcp_lic",
        "HCO_PROFILE": "stg.lndg_src_hco_profl",
        "HCO_ADDRESS": "stg.lndg_src_hco_addr",
        "HCO_LICENSE": "stg.lndg_src_hco_lic",
    }
    if entity in entity_landing_map:
        return entity_landing_map[entity]

    for mapping in ENTITY_JOB_MAPPING.values():
        if job_name and job_name == str(mapping.get("JOBNAME", "")).strip().lower():
            return str(mapping.get("LND_TABLE", "")).strip()
    return ""


def enforce_landing_table_in_rule_sql(insert_df: pd.DataFrame) -> pd.DataFrame:
    """Replace MODEL_V2 source tables in preview RULE_SQL with mapped STG landing tables."""
    insert_df = insert_df.copy()
    if "RULE_SQL" not in insert_df.columns:
        return insert_df

    for idx, row in insert_df.iterrows():
        landing_table = get_landing_table_from_qc_row(row)
        if landing_table:
            rule_sql = str(row.get("RULE_SQL", "") or "").strip()
            insert_df.at[idx, "RULE_SQL"] = re.sub(
                r"(?:[A-Za-z0-9_$]+\.)?MODEL_V2\.[A-Za-z0-9_$]+",
                landing_table,
                rule_sql,
                flags=re.IGNORECASE,
            )
    return insert_df


def prepare_qc_master_insert_df(selected_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Prepare selected rules in the final QC_RULE_MASTER insert structure.

    This is preview-only. It does not execute any Snowflake INSERT.
    Any missing QC_RULE_MASTER target column is added as NULL, and RULE_ACTIVE is
    forced to "N" before the preview dataframe is returned.

    Parameters
    ----------
    selected_df : Optional[pd.DataFrame]
        Selected QC_RULE_MASTER-style rows from Mandatory DQC or By AI.

    Returns
    -------
    pd.DataFrame
        Dataframe aligned with PHCDW_DEV.QC.QC_RULE_MASTER insert columns.
    """
    qc_rule_master_columns = TARGET_COLUMNS + [
        "CREATED_BY",
        "CREATED_DATE",
        "MODIFIED_BY",
        "MODIFIED_DATE",
    ]

    if selected_df is None or selected_df.empty:
        return pd.DataFrame(columns=qc_rule_master_columns)

    insert_df = selected_df.copy()
    insert_df.columns = insert_df.columns.astype(str).str.upper().str.strip()

    if "RULE_ACTIVE" not in insert_df.columns:
        insert_df["RULE_ACTIVE"] = RULE_ACTIVE_INACTIVE_VALUE
    else:
        insert_df["RULE_ACTIVE"] = RULE_ACTIVE_INACTIVE_VALUE

    for col in qc_rule_master_columns:
        if col not in insert_df.columns:
            insert_df[col] = None

    insert_df = insert_df[qc_rule_master_columns]
    insert_df = enforce_landing_table_in_rule_sql(insert_df)
    return insert_df


def sql_literal(value) -> str:
    """
    Convert Python/Pandas values into SQL preview literals.

    This helper is used only for displaying the sample INSERT statement. It does
    not run SQL and does not change any selected-rule business logic.
    """
    if pd.isna(value):
        return "NULL"

    escaped_value = str(value).replace("'", "''")
    return f"'{escaped_value}'"


def build_insert_sql_preview(insert_df: pd.DataFrame) -> str:
    """
    Build a sample INSERT SQL statement from the first prepared row.

    Parameters
    ----------
    insert_df : pd.DataFrame
        Prepared QC_RULE_MASTER insert dataframe.

    Returns
    -------
    str
        Preview-only INSERT SQL for the first selected row.
    """
    if insert_df is None or insert_df.empty:
        return ""

    first_row = insert_df.iloc[0]
    cols = ", ".join(insert_df.columns)
    values = [sql_literal(val) for val in first_row]
    value_string = ", ".join(values)

    return f"""
INSERT INTO PHCDW_DEV.QC.QC_RULE_MASTER
({cols})
VALUES
({value_string});
""".strip()


def render_qc_master_insert_preview(selected_df: pd.DataFrame, widget_prefix: str) -> None:
    """
    Render the preview-only QC_RULE_MASTER insert section.

    The selected dataframe is already created using existing page logic. This
    function only prepares the final insert-shaped dataframe and displays one
    sample INSERT statement for validation.
    """
    st.markdown("---")
    st.subheader("📥 Insert Selected Queries to QC Master")

    if selected_df is None or selected_df.empty:
        st.info("Select at least one rule to prepare for QC master insert.")
        return

    st.caption(
        "Preview only. No Snowflake insert will happen now. "
        "Selected rules are aligned with PHCDW_DEV.QC.QC_RULE_MASTER structure. "
        "Missing QC_RULE_MASTER audit columns are shown as NULL."
    )

    if st.button("Prepare QC Master Insert", key=f"{widget_prefix}_prepare_insert"):
        insert_df = prepare_qc_master_insert_df(selected_df)

        st.success(f"{len(insert_df)} rule(s) prepared for QC_RULE_MASTER.")

        st.markdown("### Preview of records that will be inserted")
        st.dataframe(insert_df, use_container_width=True)

        st.markdown("### Sample INSERT SQL Preview")
        st.code(build_insert_sql_preview(insert_df), language="sql")
        st.download_button(
            label="📥 Download QC Master Rules",
            data=insert_df.to_csv(index=False),
            file_name="QC_Master_Rules.csv",
            mime="text/csv",
            key=f"{widget_prefix}_download_qc_master_rules",
        )

def to_numeric_series(series: pd.Series) -> pd.Series:
    """Convert a pandas Series to numeric after removing comma separators."""
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0)


def replace_model_table_with_landing_table(rule_sql: str, landing_table: str) -> str:
    """Replace model-table references in RULE_SQL with the mapped landing table."""
    rule_sql = str(rule_sql or "").strip()
    landing_table = str(landing_table or "").strip()

    if not rule_sql or not landing_table:
        return rule_sql

    return re.sub(
        MODEL_TABLE_PATTERN,
        landing_table,
        rule_sql,
        flags=re.IGNORECASE,
    )


def get_entity_job_reference(table_name: str) -> Dict[str, str]:
    """Return ENTITY/JOBNAME/LND_TABLE mapping used by Mandatory DQC."""
    table_name = str(table_name or "").strip().upper()
    simple_table_name = table_name.split(".")[-1]

    if table_name in ENTITY_JOB_MAPPING:
        return ENTITY_JOB_MAPPING[table_name]

    if simple_table_name in ENTITY_JOB_MAPPING:
        return ENTITY_JOB_MAPPING[simple_table_name]

    return {"ENTITY": "", "JOBNAME": "", "LND_TABLE": ""}


def make_stable_widget_key(text: str) -> str:
    """Create a stable Streamlit widget key using MD5 hash."""
    return hashlib.md5(str(text).encode("utf-8")).hexdigest()


# =============================================================================
# Snowflake helper
# =============================================================================
def get_job_details(job_name: str) -> pd.DataFrame:
    """Read JOBNUMBER and max RULEGROUP for a JOBNAME from QC_RULE_MASTER."""
    conn = get_connection(os.getenv("private_key_text"), os.getenv("passphrase"), os.getenv("SNOW_ACCOUNT"), os.getenv("SNOW_USER"),
                          os.getenv("SNOW_DB"), os.getenv("SNOW_WH"), os.getenv("SNOW_SCHEMA"),os.getenv("SNOW_ROLE"), key='cursor')
    

    query = f"""
    SELECT
        JOBNUMBER,
        MAX(RULEGROUP) AS MAX_RULEGROUP
    FROM PHCDW_DEV.QC.QC_RULE_MASTER
    WHERE JOBNAME = '{job_name}'
    GROUP BY JOBNUMBER
    """

    return pd.read_sql(query, conn)


# =============================================================================
# Data loading and normalization
# =============================================================================
@st.cache_data
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize source columns and calculate common DQ metrics."""
    df = standardize_columns(df)

    failed_candidates = [
        "IMPACTED_UNIQUE_SOURCE_ID_COUNT",
        "IMPACTED_COUNT",
        "FAILED_COUNT",
    ]

    source_failed_col = None
    for col in failed_candidates:
        if col in df.columns:
            source_failed_col = col
            break

    if source_failed_col:
        df["FAILED_COUNT"] = to_numeric_series(df[source_failed_col])
    else:
        df["FAILED_COUNT"] = 0

    row_candidates = [
        "TOTAL_TABLE_ROWS",
        "ROW_COUNT",
        "TOTAL_RECORDS",
    ]

    source_row_col = None
    for col in row_candidates:
        if col in df.columns:
            source_row_col = col
            break

    if source_row_col:
        df["ROW_COUNT"] = to_numeric_series(df[source_row_col])
    else:
        df["ROW_COUNT"] = 0

    if "SQL" not in df.columns:
        df["SQL"] = df["SQL_QUERY"] if "SQL_QUERY" in df.columns else ""

    if "RULE_DESCRIPTION" not in df.columns:
        if "INSIGHT" in df.columns:
            df["RULE_DESCRIPTION"] = df["INSIGHT"]
        elif "TASK" in df.columns:
            df["RULE_DESCRIPTION"] = df["TASK"]
        else:
            df["RULE_DESCRIPTION"] = ""

    default_columns = {
        "CATEGORY": "Mandatory DQC",
        "ENTITY_TYPE": "Unknown",
        "SOURCE_CD_SUMMARY": "",
        "SAMPLE_RESULT": "",
        "TASK": "",
        "TABLE": "",
        "COLUMN": "",
    }

    for col, default_value in default_columns.items():
        if col not in df.columns:
            df[col] = default_value

    df["FAILED_COUNT"] = pd.to_numeric(df["FAILED_COUNT"], errors="coerce").fillna(0)
    df["ROW_COUNT"] = pd.to_numeric(df["ROW_COUNT"], errors="coerce").fillna(0)

    df["FAILED_COUNT_FOR_RATE"] = df.apply(
        lambda row: min(row["FAILED_COUNT"], row["ROW_COUNT"])
        if row["ROW_COUNT"] > 0
        else 0,
        axis=1,
    )

    df["FAILED_PERCENTAGE"] = df.apply(
        lambda row: (row["FAILED_COUNT_FOR_RATE"] / row["ROW_COUNT"] * 100)
        if row["ROW_COUNT"] > 0
        else 0,
        axis=1,
    )

    df["PASSED_COUNT"] = df.apply(
        lambda row: max(row["ROW_COUNT"] - row["FAILED_COUNT_FOR_RATE"], 0),
        axis=1,
    )

    df["PASSED_PERCENTAGE"] = df.apply(
        lambda row: 100 - row["FAILED_PERCENTAGE"]
        if row["ROW_COUNT"] > 0
        else 0,
        axis=1,
    )

    df["DATA_QUALITY_SCORE"] = df["PASSED_PERCENTAGE"]
    df["IMPACT_SCORE"] = df["FAILED_PERCENTAGE"]

    return df


def load_data(file: str) -> pd.DataFrame:
    """Load CSV or XLSX input and normalize it."""
    if file.lower().endswith(".xlsx"):
        df = pd.read_excel(file)
    else:
        df = pd.read_csv(file)

    return normalize_columns(df)


# =============================================================================
# Chart data preparation
# =============================================================================
def get_unique_record_col(df: pd.DataFrame) -> Optional[str]:
    """Detect the best available unique record identifier column."""
    candidate_cols = [
        "RECORD_ID",
        "ROW_ID",
        "SOURCE_ID",
        "NPI",
        "ID",
        "MASTER_ID",
        "HCP_ID",
        "HCO_ID",
    ]

    for col in candidate_cols:
        if col in df.columns:
            return col

    return None


def prepare_highest_volume_data(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str]]:
    """Prepare normalized rule-level data for the Highest Failure Rate chart."""
    unique_record_col = get_unique_record_col(df_filtered)

    if df_filtered is None or df_filtered.empty:
        return pd.DataFrame(), "No rule data available", unique_record_col

    if "IMPACTED_COUNT" in df_filtered.columns:
        top_rules = df_filtered.copy()

        top_rules["FAILED_COUNT"] = to_numeric_series(top_rules["IMPACTED_COUNT"])
        top_rules["ROW_COUNT"] = to_numeric_series(top_rules["ROW_COUNT"])

        top_rules["FAILED_COUNT_FOR_RATE"] = top_rules.apply(
            lambda row: min(row["FAILED_COUNT"], row["ROW_COUNT"])
            if row["ROW_COUNT"] > 0
            else 0,
            axis=1,
        )

        top_rules["FAILED_PERCENTAGE"] = top_rules.apply(
            lambda row: (row["FAILED_COUNT_FOR_RATE"] / row["ROW_COUNT"] * 100)
            if row["ROW_COUNT"] > 0
            else 0,
            axis=1,
        )

        top_rules["DATA_QUALITY_SCORE"] = 100 - top_rules["FAILED_PERCENTAGE"]
        top_rules["TASK_DISPLAY"] = top_rules["TASK"]

        return (
            top_rules.sort_values("FAILED_PERCENTAGE", ascending=True),
            "AI normalized failure %",
            None,
        )

    required_cols = ["TASK", "COLUMN", "TABLE", "ROW_COUNT", "FAILED_COUNT"]
    missing_cols = [col for col in required_cols if col not in df_filtered.columns]

    if missing_cols:
        return pd.DataFrame(), f"Missing columns: {', '.join(missing_cols)}", unique_record_col

    group_cols = ["TASK", "COLUMN"]

    table_row_counts = (
        df_filtered[["TASK", "COLUMN", "TABLE", "ROW_COUNT"]]
        .drop_duplicates()
        .groupby(group_cols, as_index=False)["ROW_COUNT"]
        .sum()
    )

    failed_counts = df_filtered.groupby(group_cols, as_index=False)["FAILED_COUNT"].max()

    top_rules = failed_counts.merge(table_row_counts, on=group_cols, how="left")

    top_rules["FAILED_COUNT_FOR_RATE"] = top_rules.apply(
        lambda row: min(row["FAILED_COUNT"], row["ROW_COUNT"])
        if row["ROW_COUNT"] > 0
        else 0,
        axis=1,
    )

    top_rules["FAILED_PERCENTAGE"] = top_rules.apply(
        lambda row: (row["FAILED_COUNT_FOR_RATE"] / row["ROW_COUNT"] * 100)
        if row["ROW_COUNT"] > 0
        else 0,
        axis=1,
    )

    top_rules["DATA_QUALITY_SCORE"] = 100 - top_rules["FAILED_PERCENTAGE"]
    top_rules["TASK_DISPLAY"] = top_rules["TASK"]

    top_rules = top_rules.sort_values("FAILED_PERCENTAGE", ascending=True)

    chart_label = "Failure rate calculated as FAILED_COUNT / total ROW_COUNT of all involved tables"

    return top_rules, chart_label, unique_record_col


# =============================================================================
# QC_RULE_MASTER integration for AI generated rules
# =============================================================================
def get_ai_qc_rule_master_row_df(ai_row: pd.Series) -> pd.DataFrame:
    """
    Build QC_RULE_MASTER row directly for a By AI rule.
    No external qc7.py dependency is required.
    """

    rule_sql = (
        ai_row.get("SQL")
        or ai_row.get("SQL_QUERY")
        or ai_row.get("RULE_SQL")
        or ""
    )

    row = {
        "RULEGROUP": "",
        "RULEID": "",
        "JOBNUMBER": "",
        "JOBNAME": "",
        "SOURCE_NAME": "",
        "ENTITY": str(ai_row.get("TABLE", "")),
        "DG_KPI": "",
        "REJECT_COLUMNNAME": str(ai_row.get("COLUMN", "")),
        "RULE_DESCRIPTION": str(ai_row.get("RULE_DESCRIPTION", "")),
        "RULE_SQL": str(rule_sql),
        "THRESHOLD__": "",
        "CONTACT_DISTRIBUTION_LIST": "",
        "RULE_ACTIVE": RULE_ACTIVE_INACTIVE_VALUE,
        "UPDATE_QUERY": "",
    }

    result_df = pd.DataFrame([row])
    result_df = result_df.fillna("")

    for col in result_df.columns:
        result_df[col] = result_df[col].astype(str)

    return enforce_rule_active_inactive(result_df)

def build_qc_rule_selection_key(ai_row: pd.Series, qc_master_df: pd.DataFrame) -> str:
    """Build a stable logical key for selected AI QC_RULE_MASTER rows."""
    task = str(ai_row.get("TASK", "")).strip()
    table = str(ai_row.get("TABLE", "")).strip()
    column = str(ai_row.get("COLUMN", "")).strip()

    sql_text = ""

    if qc_master_df is not None and not qc_master_df.empty:
        qc_cols = {col.upper().strip(): col for col in qc_master_df.columns}

        for sql_col in ["RULE_SQL", "SQL", "RULE_QUERY", "QUERY"]:
            actual_col = qc_cols.get(sql_col)

            if actual_col:
                sql_text = str(qc_master_df.iloc[0].get(actual_col, "")).strip()
                break

    return f"{task}||{table}||{column}||{sql_text}"


def update_selected_qc_rule(checkbox_key: str, selection_key: str, qc_master_df: pd.DataFrame) -> None:
    """Persist or remove an AI selected QC_RULE_MASTER row in Streamlit session state."""
    if "selected_qc_rule_rows_dict" not in st.session_state:
        
        st.session_state.selected_qc_rule_rows_dict = {}
        
    if "selected_qc_rule_rows" not in st.session_state:
        
        st.session_state.selected_qc_rule_rows = pd.DataFrame()
    if st.session_state.get(checkbox_key, False):
        qc_row_to_save = ensure_target_columns(qc_master_df)
        qc_row_to_save["_SELECTION_KEY"] = selection_key
        qc_row_to_save["SELECTED_RULE"] = RULE_ACTIVE_INACTIVE_VALUE

        st.session_state.selected_qc_rule_rows_dict[selection_key] = qc_row_to_save
    else:
        if selection_key in st.session_state.selected_qc_rule_rows_dict:
            del st.session_state.selected_qc_rule_rows_dict[selection_key]

    if st.session_state.selected_qc_rule_rows_dict:
        st.session_state.selected_qc_rule_rows = pd.concat(
            list(st.session_state.selected_qc_rule_rows_dict.values()),
            ignore_index=True,
        )
        st.session_state.selected_qc_rule_rows = enforce_rule_active_inactive(
            st.session_state.selected_qc_rule_rows
        )
    else:
        st.session_state.selected_qc_rule_rows = pd.DataFrame()


def enrich_qc_rule_master_display_row(qc_master_df: pd.DataFrame, ai_row: pd.Series) -> pd.DataFrame:
    """Enrich AI QC_RULE_MASTER row with schema/table information in ENTITY and SQL."""
    if qc_master_df is None or qc_master_df.empty:
        return qc_master_df

    qc_master_df = standardize_columns(qc_master_df)
    col_map = {col.upper().strip(): col for col in qc_master_df.columns}

    schema_candidates = ["SCHEMA_NAME", "TABLE_SCHEMA", "SOURCE_SCHEMA", "DB_SCHEMA", "SCHEMA"]
    table_candidates = ["TABLE_NAME", "SOURCE_TABLE", "DB_TABLE", "TABLE"]
    sql_candidates = ["RULE_SQL", "SQL", "RULE_QUERY", "QUERY"]
    entity_candidates = ["ENTITY", "ENTITY_NAME"]

    def get_value_from_qc_row(candidate_cols: Iterable[str]) -> Optional[str]:
        for c in candidate_cols:
            actual_col = col_map.get(c)

            if actual_col and pd.notna(qc_master_df.iloc[0].get(actual_col)):
                val = str(qc_master_df.iloc[0].get(actual_col)).strip()

                if val:
                    return val

        return None

    schema_name = get_value_from_qc_row(schema_candidates)
    table_name = get_value_from_qc_row(table_candidates)

    if not table_name:
        ai_table_value = str(ai_row.get("TABLE", "")).strip()

        if ai_table_value:
            parts = ai_table_value.split(".")

            if len(parts) >= 3:
                schema_name = schema_name or parts[-2]
                table_name = parts[-1]
            elif len(parts) == 2:
                schema_name = schema_name or parts[0]
                table_name = parts[1]
            else:
                table_name = ai_table_value

    full_entity_name = f"{schema_name}.{table_name}" if schema_name and table_name else table_name

    for c in entity_candidates:
        actual_col = col_map.get(c)

        if actual_col and full_entity_name:
            qc_master_df.at[qc_master_df.index[0], actual_col] = full_entity_name
            break

    for c in sql_candidates:
        actual_col = col_map.get(c)

        if actual_col:
            rule_sql = str(qc_master_df.iloc[0].get(actual_col, "")).strip()

            if rule_sql and schema_name and table_name:
                qualified_table = f"{schema_name}.{table_name}"

                rule_sql = (
                    rule_sql.replace("{SCHEMA_NAME}", schema_name)
                    .replace("{TABLE_NAME}", table_name)
                    .replace("<SCHEMA_NAME>", schema_name)
                    .replace("<TABLE_NAME>", table_name)
                    .replace("${SCHEMA_NAME}", schema_name)
                    .replace("${TABLE_NAME}", table_name)
                )

                rule_sql = re.sub(
                    rf"\bFROM\s+{re.escape(table_name)}\b",
                    f"FROM {qualified_table}",
                    rule_sql,
                    flags=re.IGNORECASE,
                )

                rule_sql = re.sub(
                    rf"\bJOIN\s+{re.escape(table_name)}\b",
                    f"JOIN {qualified_table}",
                    rule_sql,
                    flags=re.IGNORECASE,
                )

                qc_master_df.at[qc_master_df.index[0], actual_col] = rule_sql

            break

    return enforce_rule_active_inactive(qc_master_df)


def apply_mandatory_jobname_logic_to_ai_row(qc_master_df: pd.DataFrame, ai_row: pd.Series) -> pd.DataFrame:
    """Apply Mandatory DQC JOBNAME/JOBNUMBER/RULEGROUP logic to By AI rows."""

    if qc_master_df is None or qc_master_df.empty:
        return qc_master_df

    qc_master_df = standardize_columns(qc_master_df)
    qc_master_df = enforce_rule_active_inactive(qc_master_df)

    table_name = str(ai_row.get("TABLE", "")).strip().upper()
    ref = get_entity_job_reference(table_name)

    job_name = ref.get("JOBNAME", "")
    entity_name = ref.get("ENTITY", "")
    landing_table = ref.get("LND_TABLE", "")

    if not job_name:
        return qc_master_df

    for col in TARGET_COLUMNS:
        if col not in qc_master_df.columns:
            qc_master_df[col] = ""

    qc_master_df.at[qc_master_df.index[0], "JOBNAME"] = job_name

    if entity_name:
        qc_master_df.at[qc_master_df.index[0], "ENTITY"] = entity_name

    if landing_table and "RULE_SQL" in qc_master_df.columns:
        rule_sql = str(qc_master_df.iloc[0].get("RULE_SQL", "")).strip()
        qc_master_df.at[qc_master_df.index[0], "RULE_SQL"] = replace_model_table_with_landing_table(
            rule_sql,
            landing_table,
        )

    job_info = get_job_details(job_name)

    if not job_info.empty:
        qc_master_df.at[qc_master_df.index[0], "JOBNUMBER"] = str(job_info.iloc[0]["JOBNUMBER"])

        qc_master_df.at[qc_master_df.index[0], "RULEGROUP"] = str(int(job_info.iloc[0]["MAX_RULEGROUP"]) + 1)

    else:
        qc_master_df.at[
            qc_master_df.index[0],
            "JOBNUMBER"
        ] = ""

        qc_master_df.at[
        qc_master_df.index[0],
        "RULEGROUP"
        ] = "1"

    qc_master_df["RULE_ACTIVE"] = RULE_ACTIVE_INACTIVE_VALUE
    qc_master_df = qc_master_df.fillna("")
    for col in qc_master_df.columns:
        qc_master_df[col] = qc_master_df[col].astype(str)
    return qc_master_df


# =============================================================================
# Source CD parsing and impact preparation
# =============================================================================
def extract_source_cd(sample: str) -> Optional[str]:
    """Extract SOURCE_CD from SAMPLE_RESULT when it contains JSON-like text."""
    try:
        if isinstance(sample, str) and sample.strip():
            parsed = json.loads(sample.replace("'", '"'))

            if isinstance(parsed, dict):
                return parsed.get("SOURCE_CD")

            if isinstance(parsed, list) and len(parsed) > 0:
                first_item = parsed[0]

                if isinstance(first_item, dict):
                    return first_item.get("SOURCE_CD")
    except Exception:
        pass

    return None


def parse_source_summary(summary: str) -> List[Tuple[str, int, float]]:
    """Parse SOURCE_CD_SUMMARY values into source/count/percentage tuples."""
    result: List[Tuple[str, int, float]] = []

    if pd.isna(summary):
        return result

    items = str(summary).split(";")

    for item in items:
        item = item.strip()
        match = re.match(r"(.+?)\(([\d,]+),\s*([\d.]+)%\)", item)

        if match:
            source_cd = match.group(1).strip()
            count = int(match.group(2).replace(",", ""))
            pct = float(match.group(3))
            result.append((source_cd, count, pct))

    return result


def prepare_rule_source_cd_impact(df_filtered: pd.DataFrame, selected_task: str) -> pd.DataFrame:
    """Prepare Source CD impact for a selected rule."""
    rule_df = df_filtered[df_filtered["TASK"].astype(str) == str(selected_task)]
    source_rows = []

    if "SOURCE_CD_SUMMARY" in rule_df.columns:
        for _, row in rule_df.iterrows():
            for source_cd, count, _ in parse_source_summary(row["SOURCE_CD_SUMMARY"]):
                source_rows.append({"SOURCE_CD": source_cd, "FAILED_COUNT": count})

    elif "SOURCE_CD" in rule_df.columns:
        source_rows = (
            rule_df.dropna(subset=["SOURCE_CD"])
            .groupby("SOURCE_CD", as_index=False)["FAILED_COUNT"]
            .sum()
            .to_dict("records")
        )

    if not source_rows:
        return pd.DataFrame(
            columns=["SOURCE_CD", "FAILED_COUNT", "SOURCE_FAILED_SHARE_PERCENTAGE"]
        )

    source_df = (
        pd.DataFrame(source_rows)
        .groupby("SOURCE_CD", as_index=False)
        .agg(FAILED_COUNT=("FAILED_COUNT", "sum"))
    )

    total_failed = source_df["FAILED_COUNT"].sum()

    source_df["SOURCE_FAILED_SHARE_PERCENTAGE"] = source_df.apply(
        lambda row: round((row["FAILED_COUNT"] * 100 / total_failed), 2)
        if total_failed > 0
        else 0,
        axis=1,
    )

    return source_df.sort_values("SOURCE_FAILED_SHARE_PERCENTAGE", ascending=False)


def get_all_source_cds(df: pd.DataFrame) -> List[str]:
    """Collect all Source CD values available in SOURCE_CD_SUMMARY and SOURCE_CD."""
    source_values = set()

    if "SOURCE_CD_SUMMARY" in df.columns:
        for summary in df["SOURCE_CD_SUMMARY"].fillna(""):
            for source_cd, _, _ in parse_source_summary(summary):
                source_values.add(source_cd)

    if "SOURCE_CD" in df.columns:
        source_values.update(df["SOURCE_CD"].dropna().astype(str).unique().tolist())

    return sorted(list(source_values))


def parse_source_cd_metrics(value) -> List[Dict[str, object]]:
    """Parse pipeline-produced Source-CD population and failure metrics."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def get_metric_source_cds(df: pd.DataFrame) -> List[str]:
    values = set()
    if "SOURCE_CD_METRICS" in df.columns:
        for item in df["SOURCE_CD_METRICS"].fillna("[]"):
            for metric in parse_source_cd_metrics(item):
                source = str(metric.get("SOURCE_CD", "")).strip()
                if source:
                    values.add(source)
    return sorted(values)


def apply_source_metric_filter(df: pd.DataFrame, source_cd: str) -> pd.DataFrame:
    """Apply pipeline-calculated full-population Source-CD metrics to rule rows."""
    if source_cd == "Choose options":
        return df.copy()
    selected = str(source_cd).strip().casefold()
    rows = []
    for _, row in df.iterrows():
       

        for metric in parse_source_cd_metrics(row.get("SOURCE_CD_METRICS", "[]")):
            if str(metric.get("SOURCE_CD", "")).strip().casefold() != selected:
                continue
            updated = row.copy()
            failed = float(metric.get("SOURCE_CD_FAILED_COUNT", 0) or 0)
            total = float(metric.get("SOURCE_CD_TOTAL_ROW_COUNT", 0) or 0)
            updated["SOURCE_CD"] = metric.get("SOURCE_CD", source_cd)
            updated["FAILED_COUNT"] = failed
            updated["ROW_COUNT"] = total
            updated["IMPACTED_UNIQUE_SOURCE_ID_COUNT"] = failed
            if "IMPACTED_COUNT" in updated.index:
                updated["IMPACTED_COUNT"] = failed
            updated["FAILED_COUNT_FOR_RATE"] = min(failed, total) if total > 0 else 0
            updated["FAILED_PERCENTAGE"] = (failed / total * 100) if total > 0 else 0
            updated["PASSED_COUNT"] = max(total - failed, 0)
            updated["PASSED_PERCENTAGE"] = 100 - updated["FAILED_PERCENTAGE"] if total > 0 else 0
            updated["DATA_QUALITY_SCORE"] = updated["PASSED_PERCENTAGE"]
            updated["IMPACT_SCORE"] = updated["FAILED_PERCENTAGE"]
            updated["SOURCE_CD_SUMMARY"] = f"{updated['SOURCE_CD']}({int(failed)},100.00%)"
            rows.append(updated)
            break
    return pd.DataFrame(rows).reset_index(drop=True) if rows else df.iloc[0:0].copy()


# =============================================================================
# Streamlit UI helpers
# =============================================================================
def sync_generated_rule_selection(checkbox_key: str, task: str, selected_state_key: str) -> None:
    """Safely synchronize Mandatory DQC checkbox selections to session state."""
    if selected_state_key not in st.session_state:
        st.session_state[selected_state_key] = []

    selected_rules = st.session_state[selected_state_key]
    is_checked = st.session_state.get(checkbox_key, False)

    if is_checked:
        if task not in selected_rules:
            selected_rules.append(task)
    else:
        if task in selected_rules:
            selected_rules.remove(task)

    st.session_state[selected_state_key] = selected_rules


def apply_custom_css() -> None:
    """Apply custom CSS used by the dashboard header and input controls."""
    st.markdown(
        """
        <style>
        .ai-banner {
            background: linear-gradient(90deg, #2f74b5, #1f7acb);
            color: white;
            padding: 18px 24px;
            border-radius: 10px;
            font-size: 1.9rem;
            font-weight: 700;
            text-align: center;
            margin-top: 10px;
            margin-bottom: 22px;
        }

        div[data-baseweb="select"] > div {
            min-height: 44px;
            border-radius: 10px;
        }

        .stTextInput > div > div > input {
            min-height: 44px;
            border-radius: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# =============================================================================
# Dashboard renderer
# =============================================================================
def render_dashboard(file_path: str, key_prefix: str, source_cd_mode: str = "direct"):
    """Render the shared dashboard filters and charts for both pages."""
    try:
        df = load_data(file_path)
        # Preserve SOURCE_CD from the loaded result file when it already exists.
        # SAMPLE_RESULT is only a fallback because it contains a limited sample and
        # must not replace the rule-level Source CD information.
        if "SOURCE_CD" not in df.columns:
            df["SOURCE_CD"] = df["SAMPLE_RESULT"].apply(extract_source_cd)
    except Exception as e:
        st.error(traceback.format_exc())
        return None, None, None

    table_filter = st.sidebar.multiselect(
        "Table",
        df["TABLE"].dropna().unique(),
        default=df["TABLE"].dropna().unique(),
        key=f"{key_prefix}_table_filter",
    )

    column_filter = st.sidebar.multiselect(
        "Column",
        df["COLUMN"].dropna().unique(),
        default=df["COLUMN"].dropna().unique(),
        key=f"{key_prefix}_column_filter",
    )

    df_filtered = df[(df["TABLE"].isin(table_filter)) & (df["COLUMN"].isin(column_filter))]

    if "IMPACTED_UNIQUE_SOURCE_ID_COUNT" in df_filtered.columns:
        df_filtered = df_filtered[
            pd.to_numeric(
                df_filtered["IMPACTED_UNIQUE_SOURCE_ID_COUNT"],
                errors="coerce",
            ).fillna(0)
            > 0
        ]
    else:
        df_filtered = df_filtered[
            pd.to_numeric(df_filtered["FAILED_COUNT"], errors="coerce").fillna(0) > 0
        ]
    
    apply_custom_css()

    st.markdown(
        '<div class="ai-banner">📊 Data Quality Rule Portal</div>',
        unsafe_allow_html=True,
    )

    if "ENTITY" in df_filtered.columns:
        entity_col_name = "ENTITY"
    elif "ENTITY_TYPE" in df_filtered.columns:
        entity_col_name = "ENTITY_TYPE"
    else:
        entity_col_name = None

    if "REJECT_COLUMNNAME" in df_filtered.columns:
        reject_col_name = "REJECT_COLUMNNAME"
    elif "REJECT_COLUMN" in df_filtered.columns:
        reject_col_name = "REJECT_COLUMN"
    else:
        reject_col_name = "COLUMN"

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown("**Entity**")
        if entity_col_name:
            entity_options = ["Choose options"] + sorted(
                df_filtered[entity_col_name].dropna().astype(str).unique().tolist()
            )
        else:
            entity_options = ["Choose options"]

        entity_filter = st.selectbox(
            "Filter",
            entity_options,
            key=f"{key_prefix}_entity_filter",
            label_visibility="collapsed",
        )

    with c2:
        st.markdown("**Reject Column**")
        reject_options = ["Choose options"] + sorted(
            df_filtered[reject_col_name].dropna().astype(str).unique().tolist()
        )
        reject_column_filter = st.selectbox(
            "Filter",
            reject_options,
            key=f"{key_prefix}_reject_column_filter",
            label_visibility="collapsed",
        )

    with c3:
        st.markdown("**Source CD**")
        metric_sources = get_metric_source_cds(df)
        source_options = ["Choose options"] + (metric_sources or get_all_source_cds(df))
        df_filtered.attrs["SOURCE_NAME_OPTIONS"] = source_options[1:]
        source_filter = st.selectbox(
            "Filter",
            source_options,
            key=f"{key_prefix}_source_cd_filter",
            label_visibility="collapsed",
        )

    with c4:
        st.markdown("**🔎 Search Rule**")
        search_rule = st.text_input(
            "Search",
            placeholder="Search Rule",
            key=f"{key_prefix}_search_rule",
            label_visibility="collapsed",
        )

    if entity_col_name and entity_filter != "Choose options":
        df_filtered = df_filtered[df_filtered[entity_col_name].astype(str) == entity_filter]

    if reject_column_filter != "Choose options":
        df_filtered = df_filtered[df_filtered[reject_col_name].astype(str) == reject_column_filter]

    if source_filter != "Choose options":
        if "SOURCE_CD_METRICS" in df_filtered.columns:

            df_filtered = apply_source_metric_filter(df_filtered, source_filter)

        else:
            selected_source = str(source_filter).strip().casefold()
            df_filtered = df_filtered[
                df_filtered.get("SOURCE_CD", pd.Series("", index=df_filtered.index))
                .fillna("").astype(str).str.strip().str.casefold().eq(selected_source)
            ]

    if search_rule:
        searchable_cols = [
            col for col in ["TASK", "RULE_DESCRIPTION", "COLUMN", "TABLE"] if col in df_filtered.columns
        ]

        if searchable_cols:
            mask = False
            for col in searchable_cols:
                current_mask = df_filtered[col].astype(str).str.contains(
                    search_rule,
                    case=False,
                    na=False,
                )
                mask = current_mask if isinstance(mask, bool) else (mask | current_mask)
            df_filtered = df_filtered[mask]

    st.markdown("---")

    total_issues = (
        df_filtered["IMPACTED_UNIQUE_SOURCE_ID_COUNT"].sum()
        if "IMPACTED_UNIQUE_SOURCE_ID_COUNT" in df_filtered.columns
        else df_filtered["FAILED_COUNT"].sum()
    )

    st.subheader("📉 Rules with Highest Failure Rate")
    st.caption(
        "Compares rules on the same ground using failure rate = FAILED_COUNT / total ROW_COUNT of all involved tables. "
        "FAILED_COUNT is still shown in hover data for remediation volume."
    )

    top_rules, chart_label, unique_record_col = prepare_highest_volume_data(df_filtered)

    if not top_rules.empty:
        visible_bars = 20
        bar_height = 35
        chart_height = max(700, len(top_rules) * bar_height)
        visible_container_height = (visible_bars * bar_height) + 140

        fig1 = px.bar(
            top_rules,
            x="FAILED_PERCENTAGE",
            y="TASK_DISPLAY",
            orientation="h",
            text=top_rules["FAILED_PERCENTAGE"].map(lambda x: f"{x:.2f}%"),
            hover_data={
                "FAILED_COUNT": ":,",
                "ROW_COUNT": ":,",
                "FAILED_PERCENTAGE": ":.2f"
                
            },
        )

        fig1.update_traces(marker_color="#3B82F6", marker_line_width=0, textposition="outside")
        fig1.update_layout(
            height=chart_height,
            yaxis=dict(tickfont=dict(size=12)),
            xaxis_title="Failure Rate (%)",
            yaxis_title="Rule",
            showlegend=False,
            barmode="group",
        )

        with st.container(height=visible_container_height):
            st.plotly_chart(
                fig1,
                use_container_width=True,
                config={"displayModeBar": True, "scrollZoom": True},
            )

        if unique_record_col:
            st.caption(
                f"Bar values use normalized failure rate. Failed volume uses `{unique_record_col}` where available."
            )
        else:
            st.caption("Bar values use normalized failure rate = FAILED_COUNT / total ROW_COUNT.")
    else:
        st.info("No failed rules found for the selected filters.")

    st.markdown("---")
    st.subheader("🔎 Source CD Impact by Selected Rule")
    st.caption(
        "Select a rule to see which Source CDs are contributing to failures for that specific rule. "
        "This uses SOURCE_CD_SUMMARY where available."
    )

    if not top_rules.empty:
        selected_rule_for_source = st.selectbox(
            "Select Rule for Source CD Analysis",
            top_rules["TASK_DISPLAY"].dropna().unique(),
            key=f"{key_prefix}_source_cd_rule_selector",
        )

        rule_source_df = prepare_rule_source_cd_impact(df_filtered, selected_rule_for_source)

        if not rule_source_df.empty and rule_source_df["FAILED_COUNT"].sum() > 0:
            fig_rule_source = px.bar(
                rule_source_df,
                x="SOURCE_FAILED_SHARE_PERCENTAGE",
                y="SOURCE_CD",
                orientation="h",
                text=rule_source_df["SOURCE_FAILED_SHARE_PERCENTAGE"].map(lambda x: f"{x:.2f}%"),
                hover_data={
                    "FAILED_COUNT": ":,",
                    "SOURCE_FAILED_SHARE_PERCENTAGE": ":.2f",
                },
                title="Source CD Impact for Selected Rule",
            )

            fig_rule_source.update_traces(
                marker_color="#3B82F6",
                marker_line_width=0,
                textposition="outside",
            )
            fig_rule_source.update_layout(
                height=500,
                xaxis_title="Failed Share (%)",
                yaxis_title="Source CD",
                showlegend=False,
            )
            st.plotly_chart(fig_rule_source, use_container_width=True)

            top_source = rule_source_df.iloc[0]
            st.info(
                f"{top_source['SOURCE_CD']} contributes "
                f"{top_source['SOURCE_FAILED_SHARE_PERCENTAGE']:.2f}% "
                f"of failures for the selected rule."
            )

            with st.expander("View Source CD Detail Table"):
                st.dataframe(rule_source_df, use_container_width=True)

            st.caption(
                "Formula: Source CD failed share % and failed count are read directly from SOURCE_CD_SUMMARY for the selected rule. "
                "FAILED_COUNT is shown in the hover for remediation volume."
            )
        else:
            st.info("No Source CD-level failure data is available for the selected rule.")

    st.subheader("⚠️ Column Risk Distribution")
    st.caption(
        "Displays column risk using normalized failure rate so columns from different sized tables can be compared fairly."
    )

    if not df_filtered.empty and "COLUMN" in df_filtered.columns:
        col_row_counts = (
            df_filtered[["COLUMN", "TABLE", "ROW_COUNT"]]
            .drop_duplicates()
            .groupby("COLUMN", as_index=False)["ROW_COUNT"]
            .sum()
        )

        col_failed_counts = df_filtered.groupby("COLUMN", as_index=False)["FAILED_COUNT"].max()
        col_unique = col_failed_counts.merge(col_row_counts, on="COLUMN", how="left")

        col_unique["FAILED_COUNT_FOR_RATE"] = col_unique.apply(
            lambda row: min(row["FAILED_COUNT"], row["ROW_COUNT"])
            if row["ROW_COUNT"] > 0
            else 0,
            axis=1,
        )

        col_unique["FAILED_PERCENTAGE"] = col_unique.apply(
            lambda row: (row["FAILED_COUNT_FOR_RATE"] / row["ROW_COUNT"] * 100)
            if row["ROW_COUNT"] > 0
            else 0,
            axis=1,
        )

        if not col_unique.empty and col_unique["FAILED_PERCENTAGE"].sum() > 0:
            fig2 = px.treemap(
                col_unique,
                path=["COLUMN"],
                values="FAILED_PERCENTAGE",
                color="FAILED_PERCENTAGE",
                hover_data={
                    "FAILED_COUNT": ":,",
                    "ROW_COUNT": ":,",
                    "FAILED_PERCENTAGE": ":.2f",
                },
            )
            fig2.update_layout(height=600)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No column risk data available for the selected filters.")
    else:
        st.info("No column risk data available for the selected filters.")

    st.subheader("📊 Source CD Normalized Failure Distribution")
    st.caption(
        "Normalizes Source CD failed counts as a percentage share of total failed records. "
        "This avoids one high-volume source dominating the chart only because its absolute failed count is larger."
    )

    if source_cd_mode == "summary":
        source_rows = []

        if "SOURCE_CD_SUMMARY" in df_filtered.columns:
            for _, row in df_filtered.iterrows():
                for source_cd, count, pct in parse_source_summary(row["SOURCE_CD_SUMMARY"]):
                    source_rows.append(
                        {
                            "SOURCE_CD": source_cd,
                            "FAILED_COUNT": count,
                            "SOURCE_FAILED_SHARE_PERCENTAGE": pct,
                        }
                    )

        if source_rows:
            source_failed_df = (
                pd.DataFrame(source_rows)
                .groupby("SOURCE_CD", as_index=False)["FAILED_COUNT"]
                .sum()
            )
        else:
            source_failed_df = pd.DataFrame(columns=["SOURCE_CD", "FAILED_COUNT"])
    else:
        if "SOURCE_CD" in df_filtered.columns and "FAILED_COUNT" in df_filtered.columns:
            source_failed_df = (
                df_filtered.dropna(subset=["SOURCE_CD"])
                .groupby("SOURCE_CD", as_index=False)
                .agg(FAILED_COUNT=("FAILED_COUNT", "sum"))
            )
        else:
            source_failed_df = pd.DataFrame(columns=["SOURCE_CD", "FAILED_COUNT"])

    if not source_failed_df.empty and source_failed_df["FAILED_COUNT"].sum() > 0:
        total_source_failed = source_failed_df["FAILED_COUNT"].sum()
        source_failed_df["SOURCE_FAILED_SHARE_PERCENTAGE"] = source_failed_df.apply(
            lambda row: (row["FAILED_COUNT"] / total_source_failed * 100)
            if total_source_failed > 0
            else 0,
            axis=1,
        )

        source_failed_df = source_failed_df.sort_values(
            "SOURCE_FAILED_SHARE_PERCENTAGE",
            ascending=False,
        )

        fig_source = px.bar(
            source_failed_df,
            x="SOURCE_CD",
            y="SOURCE_FAILED_SHARE_PERCENTAGE",
            text=source_failed_df["SOURCE_FAILED_SHARE_PERCENTAGE"].map(lambda x: f"{x:.2f}%"),
            title="Source CD vs Normalized Failed Share",
            hover_data={
                "FAILED_COUNT": ":,",
                "SOURCE_FAILED_SHARE_PERCENTAGE": ":.2f",
            },
        )

        fig_source.update_layout(
            xaxis_title="Source CD",
            yaxis_title="Failed Share (%)",
            height=500,
            showlegend=False,
        )
        fig_source.update_traces(
            textposition="outside",
            marker_color="#3B82F6",
            marker_line_width=0,
        )
        st.plotly_chart(fig_source, use_container_width=True)

        st.caption(
            "Formula used: Source Failed Share % = Source CD FAILED_COUNT / Total FAILED_COUNT across all Source CDs x 100. "
            "Use this for normalized Source CD comparison. Keep FAILED_COUNT in hover for remediation volume."
        )
    else:
        st.info("No Source CD failure data available for the selected filters.")

    return df, df_filtered, total_issues


# =============================================================================
# Mandatory DQC Generated Rules renderer
# =============================================================================
def build_mandatory_qc_rule_row(source_row: pd.Series, ruleid: str = "") -> Dict[str, object]:
    """Build one QC_RULE_MASTER output row for Mandatory DQC."""
    table_name = str(source_row.get("TABLE", "")).strip().upper()
    ref = get_entity_job_reference(table_name)

    job_name = ref.get("JOBNAME", "")
    landing_table = ref.get("LND_TABLE", "")

    rule_sql = str(source_row.get("SQL", "")).strip()
    rule_sql = replace_model_table_with_landing_table(rule_sql, landing_table)

    job_info = get_job_details(job_name)

    if not job_info.empty:
        job_number = job_info.iloc[0]["JOBNUMBER"]
        rule_group = str(int(job_info.iloc[0]["MAX_RULEGROUP"]) + 1)
    else:
        job_number = ""
        rule_group = 1

    return {
        "RULEGROUP": rule_group,
        "RULEID": str(ruleid),
        "JOBNUMBER": job_number,
        "JOBNAME": job_name,
        "SOURCE_NAME": "",
        "ENTITY": ref.get("ENTITY", ""),
        "DG_KPI": "",
        "REJECT_COLUMNNAME": source_row.get("COLUMN", ""),
        "RULE_DESCRIPTION": source_row.get("RULE_DESCRIPTION", ""),
        "RULE_SQL": rule_sql,
        "THRESHOLD__": "",
        "CONTACT_DISTRIBUTION_LIST": "",
        "RULE_ACTIVE": RULE_ACTIVE_INACTIVE_VALUE,
        "UPDATE_QUERY": "",
    }


def render_generated_rules(
    df_filtered: pd.DataFrame,
    page_state_key: str,
    selected_state_key: str,
    widget_prefix: str,
    selected_title: str = "### ✅ Selected Rules",
) -> None:
    """Render Mandatory DQC rule list, selection, preview table, and downloads."""
    st.markdown("---")
    st.subheader("📋 Generated Rules")

    if df_filtered is None or df_filtered.empty:
        st.info("No rules available.")
        return

    rule_df = (
        df_filtered[["TASK", "RULE_DESCRIPTION"]]
        .fillna("")
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if rule_df.empty:
        st.info("No rules available.")
        return

    if page_state_key not in st.session_state:
        st.session_state[page_state_key] = 0

    if selected_state_key not in st.session_state:
        st.session_state[selected_state_key] = []

    total_rules = len(rule_df)
    total_pages = max(1, (total_rules + PAGE_SIZE - 1) // PAGE_SIZE)

    if st.session_state[page_state_key] > total_pages - 1:
        st.session_state[page_state_key] = total_pages - 1

    start_idx = st.session_state[page_state_key] * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_df = rule_df.iloc[start_idx:end_idx]

    for idx, rule_row in page_df.iterrows():
        task = str(rule_row["TASK"])
        description = str(rule_row["RULE_DESCRIPTION"])
        checkbox_key = f"{widget_prefix}_rule_checkbox_{idx}"

        if checkbox_key not in st.session_state:
            st.session_state[checkbox_key] = task in st.session_state[selected_state_key]

        col1, col2 = st.columns([1, 12])

        with col1:
            st.checkbox(
                "Select rule",
                key=checkbox_key,
                on_change=sync_generated_rule_selection,
                args=(checkbox_key, task, selected_state_key),
            )

        with col2:
            st.markdown(
                f"""
                **Task:** {task}

                **Description:** {description}
                """
            )

        st.markdown("---")

    prev_col, page_col, next_col = st.columns([2, 2, 2])

    with prev_col:
        if st.button(
            "⬅ Previous",
            disabled=(st.session_state[page_state_key] == 0),
            key=f"{widget_prefix}_prev",
        ):
            st.session_state[page_state_key] -= 1
            st.rerun()

    with page_col:
        st.markdown(
            f"""
            <center>
            <b>
            Page {st.session_state[page_state_key] + 1}
            of
            {total_pages}
            </b>
            </center>
            """,
            unsafe_allow_html=True,
        )

    with next_col:
        if st.button(
            "Next ➡",
            disabled=(st.session_state[page_state_key] >= total_pages - 1),
            key=f"{widget_prefix}_next",
        ):
            st.session_state[page_state_key] += 1
            st.rerun()

    selected_tasks = st.session_state[selected_state_key]
    selected_rows = []

    for ruleid_counter, task in enumerate(selected_tasks, start=1):
        source_row = df_filtered[df_filtered["TASK"] == task].iloc[0]
        selected_rows.append(build_mandatory_qc_rule_row(source_row, ruleid=ruleid_counter))

    selected_df = ensure_target_columns(pd.DataFrame(selected_rows))
    selected_df = apply_selected_source_name(selected_df, df_filtered.attrs.get("SOURCE_NAME_OPTIONS", get_all_source_cds(df_filtered)), widget_prefix)
    st.markdown(selected_title)
    selected_df = render_editable_selected_rules(selected_df, widget_prefix)

    # QC_RULE_MASTER insert preview is intentionally shown immediately after
    # selected rules so users can see and prepare the insert without scrolling
    # past download/export controls.
    render_qc_master_insert_preview(selected_df, widget_prefix)

    download_col1, download_col2 = st.columns(2)

    with download_col1:
        st.download_button(
            label="📥 Download Selected Rules",
            data=selected_df.to_csv(index=False),
            file_name="Mandatory_Selected_Rules.csv",
            mime="text/csv",
            key=f"{widget_prefix}_download_selected",
        )

    with download_col2:
        all_rules_rows = []

        for _, source_row in df_filtered.iterrows():
            all_rules_rows.append(build_mandatory_qc_rule_row(source_row, ruleid=""))

        all_rules_df = ensure_target_columns(pd.DataFrame(all_rules_rows))

        st.download_button(
            label="📥 Download All Rules",
            data=all_rules_df.to_csv(index=False),
            file_name="Mandatory_All_Rules.csv",
            mime="text/csv",
            key=f"{widget_prefix}_download_all",
        )


# =============================================================================
# By AI Generated Rules renderer
# =============================================================================
def build_ai_qc_master_df(ai_row: pd.Series) -> pd.DataFrame:
    """Build, enrich, map, and finalize a QC_RULE_MASTER row for a By AI rule."""
    
    qc_master_df = get_ai_qc_rule_master_row_df(ai_row)
    
    qc_master_df = enrich_qc_rule_master_display_row(qc_master_df, ai_row)
    
    qc_master_df = apply_mandatory_jobname_logic_to_ai_row(qc_master_df, ai_row)
    
    qc_master_df = qc_master_df.fillna("")
    for col in qc_master_df.columns:
        qc_master_df[col] = qc_master_df[col].astype(str)
    
    return ensure_target_columns(qc_master_df)


def get_selected_ai_rules_df() -> pd.DataFrame:
    """
    Return selected AI QC_RULE_MASTER rows as TARGET_COLUMNS only.

    This helper was added so the selected queries can be rendered at the end of
    the By AI page without changing the existing checkbox/session-state logic.
    """
    if (
        "selected_qc_rule_rows" in st.session_state
        and st.session_state.selected_qc_rule_rows is not None
        and not st.session_state.selected_qc_rule_rows.empty
    ):
        return ensure_target_columns(st.session_state.selected_qc_rule_rows.copy())

    return pd.DataFrame(columns=TARGET_COLUMNS)


def render_ai_generated_rules(
    df_filtered: pd.DataFrame,
    page_state_key: str,
    widget_prefix: str,
    selected_title: str = "### ✅ Selected Rules",
) -> None:
    """
    Render By AI rule list, QC_RULE_MASTER preview, selection, and downloads.

    Existing logic preserved:
    - qc7.py is used to generate QC_RULE_MASTER rows.
    - Mandatory DQC JOBNAME/JOBNUMBER/RULEGROUP/ENTITY mapping is applied.
    - Selected rows are maintained in session state using stable selection keys.
    - QC_RULE_MASTER insert preparation is preview-only and displayed below selected rows.
    """
    st.markdown("---")
    st.subheader("📋 Generated Rules")

    if df_filtered is None or df_filtered.empty:
        st.info("No rules available.")
        return

    ai_rule_df = df_filtered.fillna("").drop_duplicates().reset_index(drop=True)

    if ai_rule_df.empty:
        st.info("No rules available.")
        return

    if page_state_key not in st.session_state:
        st.session_state[page_state_key] = 0

    total_rules = len(ai_rule_df)
    total_pages = max(1, (total_rules + PAGE_SIZE - 1) // PAGE_SIZE)

    if st.session_state[page_state_key] > total_pages - 1:
        st.session_state[page_state_key] = total_pages - 1

    start_idx = st.session_state[page_state_key] * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_df = ai_rule_df.iloc[start_idx:end_idx]

    for idx, ai_row in page_df.iterrows():
        task = str(ai_row.get("TASK", ""))
        description = str(ai_row.get("RULE_DESCRIPTION", ""))
        table_name = str(ai_row.get("TABLE", ""))
        column_name = str(ai_row.get("COLUMN", ""))

        try:
            qc_master_df = build_ai_qc_master_df(ai_row)
        except Exception as e:

            qc_master_df = pd.DataFrame()
            st.warning(f"Unable to build QC_RULE_MASTER row for AI rule '{task}': {e}")
           

        if qc_master_df is not None and not qc_master_df.empty:
            selection_key = build_qc_rule_selection_key(ai_row, qc_master_df)
            checkbox_key = f"{widget_prefix}_qc_rule_checkbox_{make_stable_widget_key(selection_key)}"

            if checkbox_key not in st.session_state:
                st.session_state[checkbox_key] = selection_key in st.session_state.selected_qc_rule_rows_dict

            col1, col2 = st.columns([1, 12])

            with col1:
                st.checkbox(
                    "Select rule",
                    key=checkbox_key,
                    on_change=update_selected_qc_rule,
                    args=(checkbox_key, selection_key, qc_master_df),
                )

            with col2:
                st.markdown(
                    f"""
                    **Table:** {table_name}  
                    **Reject Column:** {column_name}  
                    **Task:** {task}

                    **Description:** {description}
                    """
                )

                with st.expander("Preview QC_RULE_MASTER row"):
                    
                    st.dataframe(qc_master_df, use_container_width=True)
        else:
            col1, col2 = st.columns([1, 12])

            with col1:
                st.checkbox("Select rule", key=f"{widget_prefix}_disabled_{idx}", disabled=True)

            with col2:
                st.markdown(
                    f"""
                    **Table:** {table_name}  
                    **Reject Column:** {column_name}  
                    **Task:** {task}

                    **Description:** {description}
                    """
                )
                st.info("QC_RULE_MASTER row could not be generated for this AI rule, so it cannot be selected.")

        st.markdown("---")

    prev_col, page_col, next_col = st.columns([2, 2, 2])

    with prev_col:
        if st.button(
            "⬅ Previous",
            disabled=(st.session_state[page_state_key] == 0),
            key=f"{widget_prefix}_prev",
        ):
            st.session_state[page_state_key] -= 1
            st.rerun()

    with page_col:
        st.markdown(
            f"""
            <center>
            <b>
            Page {st.session_state[page_state_key] + 1}
            of
            {total_pages}
            </b>
            </center>
            """,
            unsafe_allow_html=True,
        )

    with next_col:
        if st.button(
            "Next ➡",
            disabled=(st.session_state[page_state_key] >= total_pages - 1),
            key=f"{widget_prefix}_next",
        ):
            st.session_state[page_state_key] += 1
            st.rerun()

    selected_df = get_selected_ai_rules_df()
    selected_df = apply_selected_source_name(selected_df, df_filtered.attrs.get("SOURCE_NAME_OPTIONS", get_all_source_cds(df_filtered)), widget_prefix)
    if widget_prefix == "ai" and not selected_df.empty:
        st.session_state.selected_qc_rule_rows = selected_df.copy()
    st.markdown(selected_title)
    selected_df = render_editable_selected_rules(selected_df, widget_prefix)

    # QC_RULE_MASTER insert preview is intentionally shown immediately after
    # selected rules so users can see and prepare the insert without scrolling
    # past download/export controls.
    render_qc_master_insert_preview(selected_df, widget_prefix)

    st.download_button(
        label="📥 Download Selected Rules",
        data=selected_df.to_csv(index=False),
        file_name="AI_Selected_Rules.csv",
        mime="text/csv",
        key=f"{widget_prefix}_download_selected",
    )

    st.markdown("### 📦 All AI Rules Export")
    all_ai_rows = []

    for _, ai_row in df_filtered.iterrows():
        try:
            qc_master_df = build_ai_qc_master_df(ai_row)
            all_ai_rows.append(qc_master_df)
        except Exception:
            pass

    if all_ai_rows:
        all_rules_df = ensure_target_columns(pd.concat(all_ai_rows, ignore_index=True))
    else:
        all_rules_df = pd.DataFrame(columns=TARGET_COLUMNS)

    st.download_button(
        label="📥 Download All Rules",
        data=all_rules_df.to_csv(index=False),
        file_name="AI_All_Rules.csv",
        mime="text/csv",
        key=f"{widget_prefix}_download_all",
    )


# =============================================================================
# AI Insight and Drilldown
# =============================================================================
def generate_insight(row: pd.Series, total_issues: float) -> str:
    """Generate business-readable insight for a selected rule."""
    failed = row["FAILED_COUNT"]
    total = row["ROW_COUNT"]
    column = str(row["COLUMN"]).upper()
    sample = str(row.get("SAMPLE_RESULT", ""))

    failed_for_rate = min(failed, total) if total > 0 else 0
    contribution = (failed / total_issues * 100) if total_issues > 0 else 0
    rule_impact = (failed_for_rate / total * 100) if total > 0 else 0

    if contribution > 20:
        severity = "High"
    elif contribution > 5:
        severity = "Medium"
    elif contribution > 0:
        severity = "Low"
    else:
        severity = "No Impact"

    pattern_msg = ""

    if "NPI" in column:
        if ".0" in sample:
            pattern_msg = "Detected numeric-to-string conversion issue values ending with .0."
        else:
            pattern_msg = "NPI format inconsistencies detected."
    elif "EMAIL" in column:
        if "''" in sample or '"EMAIL": ""' in sample or "EMAIL': ''" in sample:
            pattern_msg = "Large number of empty email values detected."
        else:
            pattern_msg = "Invalid email format patterns observed."
    elif failed == 0:
        pattern_msg = "No issues detected for this rule."
    else:
        pattern_msg = f"{column} quality issue detected impacting data consistency."

    insight = f"""
**Overview:**  
This rule impacts **{int(failed):,} records** out of **{int(total):,} total records**, giving a normalized failure rate of **{rule_impact:.2f}%**. It contributes **{contribution:.2f}%** of total data issues by volume.

**Severity:** {severity}  



**Observation:**  
{pattern_msg}

**Recommendation:**  
- Investigate upstream data ingestion for {column}
- Apply validation rules before data entry
- Consider automated cleansing for recurring patterns
"""
    return insight.strip()


def render_ai_drilldown(df_filtered: pd.DataFrame, total_issues: float) -> None:
    """Render AI rule drilldown with description, insight, SQL, and sample records."""
    st.markdown("---")
    st.subheader("🔍 Rule Drill Down")
    st.caption("Explore individual rules, their logic, and sample impacted records to understand root causes.")

    if df_filtered is None or df_filtered.empty:
        st.info("No data available for selected filters.")
        return

    sorted_tasks = (
        df_filtered.sort_values("IMPACT_SCORE", ascending=False)["TASK"].dropna().unique()
    )

    if len(sorted_tasks) == 0:
        st.info("No rules available for drill down.")
        return

    selected_task = st.selectbox(
        "Select Rule",
        sorted_tasks,
        key="ai_drilldown_selected_rule",
    )

    row = df_filtered[df_filtered["TASK"] == selected_task].iloc[0]

    st.markdown("### 📌 Description")
    st.write(row.get("RULE_DESCRIPTION", "No description available."))

    auto_insight = generate_insight(row, total_issues)

    st.markdown("### 💡 AI-Generated Insight")
    st.markdown(auto_insight, unsafe_allow_html=True)

    with st.expander("🧾 SQL Logic"):
        st.code(str(row.get("SQL", "")), language="sql")

    with st.expander("📄 Sample Records"):
        try:
            sample = row.get("SAMPLE_RESULT", "")

            if isinstance(sample, str) and sample.strip():
                try:
                    cleaned_sample = sample.replace("'", '"').replace("nan", "null")
                    parsed = json.loads(cleaned_sample)

                    if isinstance(parsed, list):
                        displayed_sample_count = len(parsed)
                    elif isinstance(parsed, dict):
                        displayed_sample_count = 1
                    else:
                        displayed_sample_count = 0

                    total_impacted_records = int(row.get("FAILED_COUNT", 0))

                    st.markdown(
                        f"""
                        <span title="Showing {displayed_sample_count:,} sample record(s) out of {total_impacted_records:,} impacted record(s). Only limited samples are displayed due to dashboard space and readability constraints.">
                            ℹ️ Displaying sample records: <b>{displayed_sample_count:,}</b> of <b>{total_impacted_records:,}</b>
                        </span>
                        """,
                        unsafe_allow_html=True,
                    )

                    st.json(parsed)

                except Exception:
                    total_impacted_records = int(row.get("FAILED_COUNT", 0))
                    st.markdown(
                        f"""
                        <span title="Sample records are shown from available SAMPLE_RESULT data. Only limited samples are displayed due to dashboard space and readability constraints.">
                            ℹ️ Displaying available sample records out of <b>{total_impacted_records:,}</b> impacted record(s)
                        </span>
                        """,
                        unsafe_allow_html=True,
                    )
                    st.code(sample, language="json")
            else:
                st.info("No sample result available for this rule.")

        except Exception:
            st.write(row.get("SAMPLE_RESULT", ""))


# =============================================================================
# App bootstrap and routing
# =============================================================================
def initialize_session_state() -> None:
    """Initialize all Streamlit session-state defaults used by both pages."""
    session_defaults = {
        "selected_qc_rule_rows_dict": {},
        "selected_qc_rule_rows": pd.DataFrame(),
        "mandatory_rule_page": 0,
        "mandatory_selected_rules": [],
        "rule_page": 0,
        "selected_rules": [],
    }

    for key, value in session_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def render_app() -> None:
    """Main Streamlit app router."""
    st.warning("**Important:** AI-generated rules and outputs may contain inaccuracies. Human review and validation are required before use.")
    st.sidebar.title("📂 Navigation")
    page = st.sidebar.radio("Go to", ["Mandatory Data Quality Checks", "By AI"])

    initialize_session_state()

    st.sidebar.markdown("---")
    st.sidebar.subheader("🚀 Pipeline Execution")

    if st.sidebar.button("Run Full Pipeline"):
        status_placeholder = st.empty()
        status_placeholder.info("🚀 Starting DQC pipeline...")

        with st.expander("📋 Pipeline Progress Details", expanded=False):
            log_placeholder = st.empty()

        pipeline_steps = [
            "📊 Profiling source tables and collecting statistics...",
            "✅ Executing mandatory DQC checks...",
            "🤖 Generating AI-based DQ recommendations...",
            "📈 Calculating data quality scores and impacted records...",
            "📁 Preparing dashboard output files...",
            "🔗 Updating dashboard context with latest output files...",
            "✅ Finalizing dashboard results...",
        ]

        step_index = 0
        logs = []

        try:
            process = subprocess.Popen(
                [sys.executable, "Run_Pipeline.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            for line in process.stdout:
                line = str(line).strip()

                if not line:
                    continue

                # Keep raw technical output only inside hidden details section.
                logs.append(line)
                log_placeholder.code("\n".join(logs[-150:]), language="text")

                # Do not show coding/technical messages to business user.
                lower_line = line.lower()

                if (
                    "userwarning" in lower_line
                    or "futurewarning" in lower_line
                    or "deprecationwarning" in lower_line
                    or "traceback" in lower_line
                    or ".py:" in lower_line
                    or "site-packages" in lower_line
                    or "streamlit" in lower_line
                ):
                    continue

                # Show mapped business-friendly status only.
                if step_index < len(pipeline_steps):
                    status_placeholder.info(pipeline_steps[step_index])
                    step_index += 1
                else:
                    status_placeholder.info("⚙️ Continuing DQC processing and preparing results...")

            process.wait()

            if process.returncode == 0:
                status_placeholder.success("✅ Pipeline completed successfully. Dashboard is refreshing with latest results.")
                st.toast("Pipeline completed successfully", icon="✅")
                time.sleep(3)
                status_placeholder.empty()
                log_placeholder.empty()
                st.rerun()
            else:
                status_placeholder.error("❌ Pipeline failed. Please expand Pipeline Progress Details for technical logs.")

        except Exception as e:
            status_placeholder.error("❌ Pipeline execution failed.")
            st.sidebar.error(f"Pipeline execution failed: {e}")

    base_dir = os.path.dirname(os.path.abspath(__file__))

    if page == "Mandatory Data Quality Checks":
        if not is_pipeline_ready(base_dir, "mandatory_file"):
            st.info(
                "No Mandatory DQC results available yet. "
                "Click 'Run Full Pipeline' to generate data."
            )
            return

        file_path = resolve_runtime_file(
            base_dir,
            "mandatory_file",
            "MDQC.csv",
        )

        _, df_filtered, _ = render_dashboard(
            file_path=file_path,
            key_prefix="mandatory",
            source_cd_mode="summary",
        )

        if df_filtered is not None:
            render_generated_rules(
                df_filtered=df_filtered,
                page_state_key="mandatory_rule_page",
                selected_state_key="mandatory_selected_rules",
                widget_prefix="mandatory",
                selected_title="### ✅ Selected Rules",
            )

    elif page == "By AI":
        if not is_pipeline_ready(base_dir, "ai_file"):
            st.info(
                "No AI-generated results available yet. "
                "Click 'Run Full Pipeline' to generate data."
            )
            return

        file_path = resolve_runtime_file(
            base_dir,
            "ai_file",
            "AI_gen.xlsx",
        )

        _, df_filtered, total_issues = render_dashboard(
            file_path=file_path,
            key_prefix="ai",
            source_cd_mode="summary",
        )

        if df_filtered is not None:
            render_ai_drilldown(
                df_filtered=df_filtered,
                total_issues=total_issues if total_issues is not None else 0,
            )

            render_ai_generated_rules(
                df_filtered=df_filtered,
                page_state_key="rule_page",
                widget_prefix="ai",
                selected_title="### ✅ Selected Rules",
            )


render_app()
