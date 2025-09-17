CREATE OR REPLACE PROCEDURE IDENTIFY_HASH_COLUMNS(
    DB_NAME STRING,
    SCHEMA_NAME STRING,
    TABLE_NAME STRING,
    SAMPLE_PCT FLOAT DEFAULT 0.02,
    UNIQUENESS_THRESHOLD FLOAT DEFAULT 0.9999,
    CONFIG_TABLE STRING DEFAULT 'EXPORT_KEYS'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
from snowflake.snowpark import Session
import json

def run(session: Session, DB_NAME, SCHEMA_NAME, TABLE_NAME,
        SAMPLE_PCT, UNIQUENESS_THRESHOLD, CONFIG_TABLE):

    # Normalize for INFORMATION_SCHEMA; keep originals for quoted identifiers
    db = DB_NAME.strip().strip('"')
    sch = SCHEMA_NAME.strip().strip('"')
    tab = TABLE_NAME.strip().strip('"')
    db_u, sch_u, tab_u = db.upper(), sch.upper(), tab.upper()

    fq_tbl = f'"{db}"."{sch}"."{tab}"'
    fq_cfg = f'"{db}"."{sch}"."{CONFIG_TABLE}"'

    # 0) Fetch column list (UPPERCASE filters)
    cols_df = session.sql(f"""
        SELECT COLUMN_NAME, ORDINAL_POSITION
        FROM {db_u}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{sch_u}'
          AND TABLE_NAME   = '{tab_u}'
        ORDER BY ORDINAL_POSITION
    """).collect()

    if not cols_df:
        return (f"No columns found via INFORMATION_SCHEMA for {db}.{sch}.{tab}. "
                f"Check name/case; expected TABLE_SCHEMA='{sch_u}', TABLE_NAME='{tab_u}'.")

    col_names = [r["COLUMN_NAME"] for r in cols_df]

    # 1) Total rows
    total_rows = session.sql(f"SELECT COUNT(*) AS C FROM {fq_tbl}").collect()[0]["C"]
    if total_rows == 0:
        return f"Table {fq_tbl} is empty; nothing to infer."

    # 2) Profile each column on a SAMPLE (approx NDV + null rate)
    stats = []
    for c in col_names:
        prof = session.sql(f"""
            SELECT
              APPROX_COUNT_DISTINCT("{c}") AS NDV,
              AVG(IFF("{c}" IS NULL, 1, 0)) AS NULL_RATE
            FROM {fq_tbl} SAMPLE ({SAMPLE_PCT*100} PERCENT)
        """).collect()[0]
        ndv = prof["NDV"] or 0
        null_rate = float(prof["NULL_RATE"] or 0.0)
        ratio = (ndv / total_rows) if total_rows else 0.0
        stats.append({"col": c, "ndv": ndv, "null_rate": null_rate, "ratio": ratio})

    # 3) Rank: high cardinality, low nulls
    ranked = sorted(stats, key=lambda r: (-r["ratio"], r["null_rate"]))

    # 4) Greedy selection with full-table DISTINCT verification
    selected = []
    distinct_rows = 0
    for r in ranked:
        selected.append(r["col"])
        obj_pairs = ", ".join([f"'{c}', \"{c}\"" for c in selected])
        chk = session.sql(f"""
            SELECT COUNT(DISTINCT HASH(OBJECT_CONSTRUCT_KEEP_NULL({obj_pairs}))) AS D
            FROM {fq_tbl}
        """).collect()[0]
        distinct_rows = int(chk["D"])
        if distinct_rows / total_rows >= float(UNIQUENESS_THRESHOLD):
            break

    # Safety: if nothing reached the threshold, still persist the best-so-far
    # (could be empty if INFORMATION_SCHEMA was empty, but we guarded above)
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {fq_cfg} (
        DB_NAME STRING, SCHEMA_NAME STRING, TABLE_NAME STRING,
        KEY_COLS ARRAY, TOTAL_ROWS NUMBER, DISTINCT_ROWS NUMBER, AS_OF TIMESTAMP_LTZ
      )
    """).collect()

    session.sql(f"""
      INSERT INTO {fq_cfg}
      SELECT '{db}','{sch}','{tab}',
             PARSE_JSON('{json.dumps(selected)}'),
             {total_rows},{distinct_rows},CURRENT_TIMESTAMP()
    """).collect()

    if not selected:
        return (f"Could not reach uniqueness threshold; persisted empty KEY_COLS. "
                f"Check sampling percent or data quality.")

    return (f"Selected KEY_COLS={selected}; uniqueness={distinct_rows/total_rows:.6f} "
            f"for {fq_tbl} (sample={SAMPLE_PCT*100:.2f}%).")
$$;
