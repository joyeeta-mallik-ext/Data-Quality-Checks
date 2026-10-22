"""
DQ Rule Execution Pipeline
--------------------------
Purpose:
    Executes pre-defined Data Quality rules from SQL_queries.RULE_QUERIES in Snowflake,
    maps them to KPIs from KPI_Mapping_list.KPI_MAPPING, calculates impacted unique SOURCE_ID
    counts, creates SOURCE_CD-wise impact summary, and saves results to CSV.

Production-friendly handoff change:
    - Saves output into output/mandatory/ folder.
    - Creates output folders automatically.
    - main() returns the exact generated CSV path so another script can consume it directly.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import snowflake.connector
import snowflake.connector.pandas_tools
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.converter_null import SnowflakeNoConverterToPython
from dotenv import load_dotenv

from KPI_Mapping_list import KPI_MAPPING
from SQL_queries import RULE_QUERIES


# =====================================================
# LOAD ENV
# =====================================================

load_dotenv(dotenv_path=".env")

DB_NAME = os.getenv("__DB_NAME__")

SNOWFLAKE_CREDS = {
    "user": os.getenv("SNOW_USER"),
    "password": os.getenv("private_key_text"),
    "account": os.getenv("SNOW_ACCOUNT"),
    "warehouse": os.getenv("SNOW_WH"),
    "database": os.getenv("SNOW_DB"),
    "schema": os.getenv("SNOW_SCHEMA"),
    "role": os.getenv("SNOW_ROLE"),
}


# =====================================================
# OUTPUT CONFIGURATION
# =====================================================

OUTPUT_ROOT_DIR = "output"
MANDATORY_OUTPUT_DIR = os.path.join(OUTPUT_ROOT_DIR, "mandatory")


def ensure_output_folders() -> None:
    """
    Definition:
        Creates output folders required by this script if they do not already exist.

    Input:
        None.

    Output:
        None. Creates folders on disk.
    """
    os.makedirs(MANDATORY_OUTPUT_DIR, exist_ok=True)


def keep_latest_files(folder_path: str, keep_count: int = 2) -> None:
    """Keep only the latest output files and delete older files."""
    if not os.path.isdir(folder_path):
        return
    files = [os.path.join(folder_path, name) for name in os.listdir(folder_path)
             if os.path.isfile(os.path.join(folder_path, name))]
    files.sort(key=os.path.getmtime, reverse=True)
    for old_file in files[keep_count:]:
        try:
            os.remove(old_file)
            print(f"Deleted old mandatory output: {old_file}")
        except OSError as exc:
            print(f"Unable to delete old mandatory output {old_file}: {exc}")


# =====================================================
# STATIC CONFIGURATION
# =====================================================

TABLE_ENTITY_MAP = {
    "MDS_HCP_PRFL_DATA_FULL": "HCP_PROFILE",
    "MDM_NON_MDS_PRFL_DATA_FULL": "HCP_PROFILE",
    "MDS_HCP_ADDR_DATA_FULL": "HCP_ADDRESS",
    "MDM_NON_MDS_ADDR_DT_FULL": "HCP_ADDRESS",
    "MDS_HCP_LIC_DATA_FULL": "HCP_LICENSE",
    "MDM_NON_MDS_LIC_DT_FULL": "HCP_LICENSE",
    "MDM_HCO_PRFL_DATA_FULL": "HCO_PROFILE",
    "MDM_HCO_ADDR_DATA_FULL": "HCO_ADDRESS",
    "MDM_HCO_LIC_DATA_FULL": "HCO_LICENSE",
}

ENTITY_COLUMNS = {
    "HCP_PROFILE": [
        "SOURCE_ID",
        "SOURCE_CD",
        "NPI",
        "EMAIL",
        "PHONE_NUMBER",
        "FIRST_NAME",
        "LAST_NAME",
        "FAX_NUMBER",
    ],
    "HCP_ADDRESS": [
        "ADDRESS_ID",
        "SOURCE_ID",
        "SOURCE_CD",
        "ADDRESS_TYPE",
        "ADDRESS1",
        "ADDRESS2",
        "CITY",
        "STATE",
        "ZIP",
        "ZIP4",
        "COUNTRY_CODE",
        "PHONE_NUMBER1",
        "PHONE_NUMBER2",
        "FAX_NUMBER",
    ],
    "HCP_LICENSE": [
        "IDENTIFIER_TYPE",
        "SOURCE_CD",
        "IDENTIFIER_VALUE",
        "SOURCE_ID",
        "LICENSE_ID",
    ],
    "HCO_PROFILE": [
        "SOURCE_ID",
        "SOURCE_CD",
        "ORGNAME",
        "ORG_EMAIL_ADDR",
        "ACCT_TYPE",
        "ORG_TYPE",
    ],
    "HCO_ADDRESS": [
        "ADDRESS_ID",
        "SOURCE_ID",
        "SOURCE_CD",
        "ADDRESS_TYPE",
        "ADDRESS1",
        "CITY",
        "STATE",
        "ZIP",
        "COUNTRY_CODE",
    ],
    "HCO_LICENSE": [
        "PRMRY_KEY",
        "IDENTIFIER_TYPE",
        "SOURCE_CD",
        "IDENTIFIER_VALUE",
        "SOURCE_ID",
    ],
}


# =====================================================
# COMMON HELPERS
# =====================================================

def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Definition:
        Creates a copy of the input DataFrame and standardizes all column names
        to uppercase with leading/trailing spaces removed.

    Input:
        df: Pandas DataFrame returned from Snowflake or created in the pipeline.

    Output:
        DataFrame copy with normalized column names.
    """
    normalized_df = df.copy()
    normalized_df.columns = [str(col).upper().strip() for col in normalized_df.columns]
    return normalized_df


def clean_text_series(series: pd.Series) -> pd.Series:
    """
    Definition:
        Converts a pandas Series to string values, trims spaces, removes blanks,
        and removes null values.

    Input:
        series: Pandas Series to clean.

    Output:
        Cleaned pandas Series containing non-null and non-blank string values.
    """
    return (
        series
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )


def format_sql_template(sql_template: str) -> str:
    """
    Definition:
        Applies database-name substitution to SQL templates. This preserves your
        existing replacement logic for {__DB_NAME__}.

    Input:
        sql_template: SQL text from RULE_QUERIES.

    Output:
        SQL string with {__DB_NAME__} replaced by DB_NAME.
    """
    if not sql_template:
        return ""

    return str(sql_template).replace("{__DB_NAME__}", str(DB_NAME))


# =====================================================
# SNOWFLAKE CONNECTION
# =====================================================

def get_connection(private_key_text, passphrase, account, username,
                      database, warehouse, schema,role, key='cursor'):
    """
    fn connects & creates sf connection object, sf cursor object & sf options
    for snowflake using private key & passphrase.
    :param private_key_text: sf private key as str
    :param passphrase: sf passphrase as str
    :param account: sf account as str
    :param username: sf username as str
    :param database: sf database as str
    :param warehouse: sf warehouse as str
    :param schema: sf schema as str
    :return sf_connector: sf connection object
    :return sf_cursor: sf cursor object
    :return sf_options: sf options as dict
    """
 
    # remove leading/trailing whitespace, then convert to bytes
    private_key_bytes = private_key_text.strip().encode()
    # convert to bytes
    passphrase = passphrase.encode()
 
    # load the private key object from PEM bytes
    private_key = serialization.load_pem_private_key(
        private_key_bytes,
        password=passphrase,
        backend=default_backend()
    )
 
    # convert private key to DER format bytes required by Snowflake connector
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
 
    # fetch sf connector object
    sf_connector = snowflake.connector.connect(
        account=account,
        user=username,
        private_key=private_key_der,
        database=database,
        warehouse=warehouse,
        schema=schema,
        role=role,
        converter_class=SnowflakeNoConverterToPython
    )
    # fetch sf cursor object
    sf_cursor = sf_connector.cursor()
    print('snowflake cursor created successfully')
 
    # convert private key to PEM format bytes required by Snowflake options
    pem_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()). \
        replace(b"-----BEGIN PRIVATE KEY-----\n", b"") \
        .replace(b"\n-----END PRIVATE KEY-----", b"").decode("utf-8")
 
    # create sf options
    sf_options = {
        "sfURL": account + '.snowflakecomputing.com',
        "sfAccount": account,
        "sfUser": username,
        "pem_private_key": pem_private_key,
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": warehouse,
        "usestagingtable": 'OFF',
        "truncate_table": "ON",
    }
    print('snowflake options created successfully')
 
    return sf_connector

# =====================================================
# COUNT UNIQUE IMPACTED RECORDS
# =====================================================

def get_unique_impacted_count(df: pd.DataFrame) -> int:
    """
    Definition:
        Calculates IMPACTED_UNIQUE_SOURCE_ID_COUNT from SQL query output.
        If SOURCE_ID exists, count distinct non-null/non-blank SOURCE_ID values.
        If SOURCE_ID is not present, preserve existing behavior and return row count.

    Input:
        df: DataFrame returned by the rule SQL query.

    Output:
        Integer impacted count.
    """
    if df is None or df.empty:
        return 0

    temp_df = normalize_dataframe_columns(df)

    if "SOURCE_ID" not in temp_df.columns:
        return len(temp_df)

    return clean_text_series(temp_df["SOURCE_ID"]).nunique()


# =====================================================
# SOURCE_CD SUMMARY
# =====================================================

def get_source_cd_summary(df: pd.DataFrame, impacted_count: Optional[int] = None) -> str:
    """
    Definition:
        Builds SOURCE_CD-wise impacted SOURCE_ID count and percentage.
        Percentage is calculated against total SOURCE_CD-wise impacted count,
        so SOURCE_CD_SUMMARY percentage does not exceed 100%.

    Input:
        df: DataFrame returned by the rule SQL query.
        impacted_count: Optional impacted unique SOURCE_ID count. Kept for compatibility.

    Output:
        String formatted like:
            IQV_LAAD(11231551,99.99%); PM(916,0.01%)
        Returns empty string if SOURCE_ID or SOURCE_CD is missing.
    """
    if df is None or df.empty:
        return ""

    temp_df = normalize_dataframe_columns(df)

    required_cols = {"SOURCE_ID", "SOURCE_CD"}
    if not required_cols.issubset(set(temp_df.columns)):
        return ""

    temp_df = temp_df.copy()
    temp_df["SOURCE_ID"] = temp_df["SOURCE_ID"].astype(str).str.strip().replace("", pd.NA)
    temp_df["SOURCE_CD"] = temp_df["SOURCE_CD"].astype(str).str.strip().replace("", pd.NA)
    temp_df = temp_df.dropna(subset=["SOURCE_ID", "SOURCE_CD"])

    if temp_df.empty:
        return ""

    summary_df = (
        temp_df
        .groupby("SOURCE_CD", dropna=True)["SOURCE_ID"]
        .nunique()
        .reset_index(name="IMPACTED_UNIQUE_SOURCE_ID_COUNT")
        .sort_values(by="IMPACTED_UNIQUE_SOURCE_ID_COUNT", ascending=False)
    )

    total_source_cd_impacted_count = summary_df["IMPACTED_UNIQUE_SOURCE_ID_COUNT"].sum()

    if total_source_cd_impacted_count == 0:
        return ""

    output_parts = []

    for _, row in summary_df.iterrows():
        source_cd = str(row["SOURCE_CD"])
        source_count = int(row["IMPACTED_UNIQUE_SOURCE_ID_COUNT"])
        source_pct = round((source_count * 100) / total_source_cd_impacted_count, 2)
        output_parts.append(f"{source_cd}({source_count},{source_pct}%)")

    return "; ".join(output_parts)


def get_source_cd_metrics(conn, table_name: str, failed_df: pd.DataFrame) -> str:
    """Return full-population and failed unique SOURCE_ID counts by SOURCE_CD as JSON."""
    if failed_df is None or failed_df.empty:
        return "[]"
    failed = normalize_dataframe_columns(failed_df)
    if not {"SOURCE_CD", "SOURCE_ID"}.issubset(failed.columns):
        return "[]"
    failed = failed.copy()
    failed["SOURCE_CD"] = failed["SOURCE_CD"].astype(str).str.strip().replace("", pd.NA)
    failed["SOURCE_ID"] = failed["SOURCE_ID"].astype(str).str.strip().replace("", pd.NA)
    failed = failed.dropna(subset=["SOURCE_CD", "SOURCE_ID"])
    failed_counts = failed.groupby("SOURCE_CD")["SOURCE_ID"].nunique().to_dict()

    if not failed_counts:
        return "[]"
    full_table = f'{SNOWFLAKE_CREDS["database"]}.{SNOWFLAKE_CREDS["schema"]}.{table_name}'
    source_values = list(failed_counts)
    quoted = ", ".join("'" + value.replace("'", "''") + "'" for value in source_values)
    sql = f"""
    SELECT TRIM(TO_VARCHAR(SOURCE_CD)) AS SOURCE_CD,
           COUNT(DISTINCT NULLIF(TRIM(TO_VARCHAR(SOURCE_ID)), '')) AS SOURCE_CD_TOTAL_ROW_COUNT
    FROM {full_table}
    WHERE SOURCE_CD IS NOT NULL
      AND TRIM(TO_VARCHAR(SOURCE_CD)) IN ({quoted})
    GROUP BY TRIM(TO_VARCHAR(SOURCE_CD))
    """
    population_df, error = execute_query(conn, sql)
    population = {}
    if error is None and not population_df.empty:
        population_df = normalize_dataframe_columns(population_df)
        population = dict(zip(population_df["SOURCE_CD"].astype(str), population_df["SOURCE_CD_TOTAL_ROW_COUNT"].astype(int)))
    metrics = []
    for source_cd, failed_count in failed_counts.items():
        total_count = int(population.get(source_cd, 0))
        metrics.append({
            "SOURCE_CD": source_cd,
            "SOURCE_CD_TOTAL_ROW_COUNT": total_count,
            "SOURCE_CD_FAILED_COUNT": int(failed_count),
            "SOURCE_CD_FAILURE_PERCENTAGE": round((failed_count * 100 / total_count), 4) if total_count else 0,
        })
    import json
    return json.dumps(metrics, ensure_ascii=False)


# =====================================================
# EXECUTE SQL
# =====================================================

def execute_query(conn, sql: str) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Definition:
        Executes a Snowflake SQL query and returns query output as a DataFrame.

    Input:
        conn: Active Snowflake connection.
        sql: SQL query string to execute.

    Output:
        Tuple of DataFrame and error string. Error is None when successful.
    """
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        df = cursor.fetch_pandas_all()
        return df, None

    except Exception as exc:
        return pd.DataFrame(), str(exc)

    finally:
        cursor.close()


# =====================================================
# GET TOTAL TABLE ROW COUNT
# =====================================================

def get_total_table_row_count(conn, table_name: str) -> int:
    """
    Definition:
        Gets total row count for source table. This value is used as
        TOTAL_TABLE_ROWS and should represent full table size.

    Input:
        conn: Active Snowflake connection.
        table_name: Table name without database/schema prefix.

    Output:
        Integer total row count. Returns 0 if query fails.
    """
    cursor = None
    sql = f"""
    SELECT COUNT(*) AS TOTAL_ROWS
    FROM {SNOWFLAKE_CREDS["database"]}.{SNOWFLAKE_CREDS["schema"]}.{table_name}
    """

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        return int(result[0]) if result else 0

    except Exception:
        return 0

    finally:
        if cursor is not None:
            cursor.close()


# =====================================================
# FIND RELEVANT KPIs
# =====================================================

def find_column_kpis(entity: str, column: str) -> List[str]:
    """
    Definition:
        Finds KPI names applicable to a specific entity and column based on KPI_MAPPING.

    Input:
        entity: Entity name such as HCP_PROFILE or HCO_ADDRESS.
        column: Column name being evaluated.

    Output:
        List of matching KPI names.
    """
    matched_kpis = []
    entity_upper = str(entity).upper()
    column_upper = str(column).upper()

    for kpi_name, columns in KPI_MAPPING.items():
        if entity_upper not in str(kpi_name).upper():
            continue

        mapped_columns = [str(mapped_col).upper() for mapped_col in columns]

        if column_upper in mapped_columns:
            matched_kpis.append(kpi_name)

    return matched_kpis


def should_execute_rule(rule: Dict[str, Any], entity: str, column: str) -> bool:
    """
    Definition:
        Determines whether a rule from RULE_QUERIES should run for the current entity and column.

    Input:
        rule: Rule dictionary from RULE_QUERIES.
        entity: Current entity name.
        column: Current column name.

    Output:
        True if the rule should execute, otherwise False.
    """
    rule_kpi = str(rule.get("kpi", ""))
    rule_columns = [
        col.strip().upper()
        for col in str(rule.get("column", "")).split(",")
        if col.strip()
    ]

    entity_upper = str(entity).upper()
    column_upper = str(column).upper()

    if rule_kpi.startswith("Consistency"):
        if entity_upper not in rule_kpi.upper():
            return False

        if column_upper != "SOURCE_ID":
            return False

        return True

    if column_upper not in rule_columns:
        return False

    entity_kpis = find_column_kpis(entity, column)

    if rule_kpi not in entity_kpis:
        return False

    return True


def build_sample_result(df: pd.DataFrame, column: str, sample_size: int = 10) -> str:
    """
    Definition:
        Builds SAMPLE_RESULT from the query output. It keeps SOURCE_ID and the
        current column when available.

    Input:
        df: DataFrame returned by rule SQL.
        column: Current column being processed.
        sample_size: Number of sample rows to store.

    Output:
        String representation of sample records. Returns empty string if df is empty.
    """
    if df is None or df.empty:
        return ""

    temp_df = normalize_dataframe_columns(df)
    column_upper = str(column).upper().strip()

    cols_to_keep = []

    if "SOURCE_ID" in temp_df.columns:
        cols_to_keep.append("SOURCE_ID")

    if column_upper in temp_df.columns:
        cols_to_keep.append(column_upper)

    if not cols_to_keep:
        cols_to_keep = list(temp_df.columns[: min(5, len(temp_df.columns))])

    return str(temp_df[cols_to_keep].head(sample_size).to_dict("records"))


# =====================================================
# RUN PIPELINE
# =====================================================

def run_pipeline(table_name: str, column: str, entity: str) -> List[Dict[str, Any]]:
    """
    Definition:
        Runs all eligible rules for one table/entity/column combination.

    Input:
        table_name: Snowflake source table name.
        column: Column currently being evaluated.
        entity: Entity name linked to table_name.

    Output:
        List of result dictionaries to be saved in final CSV.
    """
    print(f"\nProcessing {table_name}.{column}")

    results = []
    conn = get_connection(os.getenv("private_key_text"), os.getenv("passphrase"), os.getenv("SNOW_ACCOUNT"), os.getenv("SNOW_USER"),
                      os.getenv("SNOW_DB"), os.getenv("SNOW_WH"), os.getenv("SNOW_SCHEMA"),os.getenv("SNOW_ROLE"), key='cursor')
    print("##################Snflk Estb")
    try:
        table_row_count = get_total_table_row_count(conn, table_name)
        
        for rule_id, rule in RULE_QUERIES.items():
            rule_kpi = rule.get("kpi", "")
            rule_task = rule.get("task", "")
            sql = format_sql_template(rule.get("query", ""))

            if not sql:
                continue

            if not should_execute_rule(rule, entity, column):
                continue

            df, error = execute_query(conn, sql)
            impacted_count = get_unique_impacted_count(df) if error is None else 0

            if impacted_count == 0:
                continue

            source_cd_summary = get_source_cd_summary(df, impacted_count) if error is None else ""
            source_cd_metrics = get_source_cd_metrics(conn, table_name, df) if error is None else "[]"
            sample_result = build_sample_result(df, column) if error is None else ""

            results.append(
                {
                    "TABLE": table_name,
                    "ENTITY": entity,
                    "COLUMN": column,
                    "TASK": rule_task,
                    "RULE_ID": rule_id,
                    "KPI": rule_kpi,
                    "TOTAL_TABLE_ROWS": table_row_count,
                    "IMPACTED_UNIQUE_SOURCE_ID_COUNT": impacted_count,
                    "STATUS": "SUCCESS" if error is None else "FAILED",
                    "ERROR": "" if error is None else error,
                    "SQL_QUERY": sql,
                    "SOURCE_CD_SUMMARY": source_cd_summary,
                    "SOURCE_CD_METRICS": source_cd_metrics,
                    "SAMPLE_RESULT": sample_result,
                }
            )

    finally:
        conn.close()

    return results


# =====================================================
# SAVE RESULTS
# =====================================================

def save_results(results: List[Dict[str, Any]], run_id: Optional[str] = None) -> Optional[str]:
    """
    Definition:
        Saves final pipeline output to timestamped CSV file.

    Input:
        results: List of result dictionaries generated by run_pipeline().
        run_id: Optional common run id supplied by orchestrator.

    Output:
        Generated CSV file path if saved, otherwise None.
    """
    if not results:
        print("No results found.")
        return None

    ensure_output_folders()

    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    df = pd.DataFrame(results)
    file_name = os.path.join(MANDATORY_OUTPUT_DIR, f"Predifined_Rules_{run_id}.csv")
    df.to_csv(file_name, index=False)

    print(f"\nResults saved: {file_name}")
    return file_name


# =====================================================
# FOR UI SELECTING THE CORRECT JOB NUMBER AND GROUP
# =====================================================

def get_job_details(job_name: str, conn=None) -> pd.DataFrame:
    """
    Definition:
        Retrieves JOBNUMBER and maximum RULEGROUP for a given job name from QC_RULE_MASTER.

    Input:
        job_name: Job name to search in QC_RULE_MASTER.
        conn: Optional active Snowflake connection.

    Output:
        DataFrame with JOBNUMBER and MAX_RULEGROUP.
    """
    query = f"""
    SELECT
        JOBNUMBER,
        MAX(RULEGROUP) AS MAX_RULEGROUP
    FROM PHCDW_DEV.QC.QC_RULE_MASTER
    WHERE JOBNAME = '{job_name}'
    GROUP BY JOBNUMBER
    """

    should_close_conn = False

    if conn is None:
        conn = get_connection (os.getenv("private_key_text"), os.getenv("passphrase"), os.getenv("SNOW_ACCOUNT"), os.getenv("SNOW_USER"),
                      os.getenv("SNOW_DB"), os.getenv("SNOW_WH"), os.getenv("SNOW_SCHEMA"),os.getenv("SNOW_ROLE"), key='cursor')
        should_close_conn = True

    try:
        return pd.read_sql(query, conn)

    finally:
        if should_close_conn:
            conn.close()


# =====================================================
# MAIN
# =====================================================

def main(run_id: Optional[str] = None) -> Optional[str]:
    """
    Definition:
        Main entry point for the predefined DQ rule execution pipeline.

    Input:
        run_id: Optional common run id supplied by orchestrator.

    Output:
        Generated mandatory CSV file path. This path is passed to LLM_Rule_Checks.py.
    """
    ensure_output_folders()

    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    all_results = []

    print("\nDQ Rule Execution Started\n")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_task = {}

        for table_name, entity in TABLE_ENTITY_MAP.items():
            columns = ENTITY_COLUMNS.get(entity, [])

            for col in columns:
                future = executor.submit(run_pipeline, table_name, col, entity)
                future_to_task[future] = (table_name, col)

        for future in as_completed(future_to_task):
            table_name, col = future_to_task[future]

            try:
                result = future.result()
                all_results.extend(result)

                print(f"Completed: {table_name}.{col}")

            except Exception as exc:
                print(f"Error in {table_name}.{col}: {exc}")

    output_file = save_results(all_results, run_id=run_id)

    print("\nPredefined rule execution DONE")
    return output_file


if __name__ == "__main__":
    main()
