CREATE OR REPLACE PROCEDURE IDENTIFY_HASH_COLUMNS(
    DB_NAME STRING,
    SCHEMA_NAME STRING,
    TABLE_NAME STRING,
    SAMPLE_PCT FLOAT DEFAULT 0.02,              -- 2% sample for profiling
    UNIQUENESS_THRESHOLD FLOAT DEFAULT 0.9999,  -- 99.99% "unique enough"
    CONFIG_TABLE STRING DEFAULT 'EXPORT_KEYS'   -- target table
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
from snowflake.snowpark import Session
import json
from math import isfinite

def run(session: Session, DB_NAME, SCHEMA_NAME, TABLE_NAME,
        SAMPLE_PCT, UNIQUENESS_THRESHOLD, CONFIG_TABLE):

    # Normalize identifiers
    db = DB_NAME.strip().strip('"')
    sch = SCHEMA_NAME.strip().strip('"')
    tab = TABLE_NAME.strip().strip('"')
    db_u, sch_u, tab_u = db.upper(), sch.upper(), tab.upper()

    fq_tbl = f'"{db}"."{sch}"."{tab}"'
    fq_cfg = f'"{db}"."{sch}"."{CONFIG_TABLE}"'

    # 0) Column list (case-safe)
    cols_df = session.sql(f"""
        SELECT COLUMN_NAME, ORDINAL_POSITION
        FROM {db_u}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{sch_u}' AND TABLE_NAME = '{tab_u}'
        ORDER BY ORDINAL_POSITION
    """).collect()
    if not cols_df:
        return (f"[ERROR] No columns found for {db}.{sch}.{tab}. "
                f"Check name/case (expected {sch_u}.{tab_u}).")

    col_names = [r["COLUMN_NAME"] for r in cols_df]
    obj_pairs_all = ", ".join([f"'{c}', \"{c}\"" for c in col_names])

    # 1) Totals and basic sample info
    total_rows = session.sql(f"SELECT COUNT(*) AS C FROM {fq_tbl}").collect()[0]["C"]
    if total_rows == 0:
        # Ensure config table exists and write an empty marker row
        session.sql(f"""
          CREATE TABLE IF NOT EXISTS {fq_cfg} (
            DB_NAME STRING, SCHEMA_NAME STRING, TABLE_NAME STRING,
            KEY_COLS ARRAY, TOTAL_ROWS NUMBER, DISTINCT_ROWS NUMBER,
            UNIQUENESS_RATIO FLOAT, EXACT_UNIQUE BOOLEAN,
            SAMPLE_PCT FLOAT, SAMPLE_ROWS NUMBER, AVG_ROW_BYTES FLOAT,
            UNIQUENESS_THRESHOLD FLOAT,
            COL_STATS VARIANT, SELECTED_PATH VARIANT,
            CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
          )
        """).collect()
        session.sql(f"""
          INSERT INTO {fq_cfg}(DB_NAME,SCHEMA_NAME,TABLE_NAME,KEY_COLS,
                               TOTAL_ROWS,DISTINCT_ROWS,UNIQUENESS_RATIO,EXACT_UNIQUE,
                               SAMPLE_PCT,SAMPLE_ROWS,AVG_ROW_BYTES,UNIQUENESS_THRESHOLD,
                               COL_STATS,SELECTED_PATH)
          SELECT '{db}','{sch}','{tab}', PARSE_JSON('[]'),
                 0,0,0.0,false,
                 {SAMPLE_PCT},0,0.0,{UNIQUENESS_THRESHOLD},
                 PARSE_JSON('[]'), PARSE_JSON('[]')
        """).collect()
        return f"[OK] Table {fq_tbl} is empty; persisted placeholder row."

    # Sample rows count + avg row bytes (for sizing shards later)
    sample_info = session.sql(f"""
        SELECT
          COUNT(*) AS SAMP_ROWS,
          AVG(LENGTH(TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL({obj_pairs_all})))) AS AVG_ROW_BYTES
        FROM {fq_tbl} SAMPLE ({SAMPLE_PCT*100} PERCENT)
    """).collect()[0]
    sample_rows = int(sample_info["SAMP_ROWS"] or 0)
    avg_row_bytes = float(sample_info["AVG_ROW_BYTES"] or 0.0)

    # 2) Per-column profiling on sample
    col_stats = []
    for c in col_names:
        prof = session.sql(f"""
            SELECT
              APPROX_COUNT_DISTINCT("{c}") AS NDV,
              AVG(IFF("{c}" IS NULL,1,0))  AS NULL_RATE
            FROM {fq_tbl} SAMPLE ({SAMPLE_PCT*100} PERCENT)
        """).collect()[0]
        ndv = int(prof["NDV"] or 0)
        null_rate = float(prof["NULL_RATE"] or 0.0)
        ratio = (ndv/total_rows) if total_rows else 0.0
        col_stats.append({"col": c, "ndv_sample": ndv,
                          "null_rate_sample": null_rate,
                          "cardinality_ratio_sample": ratio})

    # 3) Rank columns: high cardinality, low nulls
    ranked = sorted(
        col_stats,
        key=lambda r: (-r["cardinality_ratio_sample"], r["null_rate_sample"])
    )

    # 4) Greedy selection path with full-table distinct verification at each step
    selected = []
    selected_path = []  # [{k:int, cols:[...], distinct_rows:int, ratio:float}]
    distinct_rows = 0

    for r in ranked:
        selected.append(r["col"])
        obj_pairs_sel = ", ".join([f"'{c}', \"{c}\"" for c in selected])
        chk = session.sql(f"""
            SELECT COUNT(DISTINCT HASH(OBJECT_CONSTRUCT_KEEP_NULL({obj_pairs_sel}))) AS D
            FROM {fq_tbl}
        """).collect()[0]
        distinct_rows = int(chk["D"])
        ratio = (distinct_rows/total_rows) if total_rows else 0.0
        selected_path.append({
            "k": len(selected),
            "cols": selected.copy(),
            "distinct_rows": distinct_rows,
            "uniqueness_ratio": ratio
        })
        if ratio >= float(UNIQUENESS_THRESHOLD):
            break

    exact_unique = (distinct_rows == total_rows)
    final_ratio = (distinct_rows/total_rows) if total_rows else 0.0

    # 5) Persist (creates table with rich stats if not exists)
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {fq_cfg} (
        DB_NAME STRING, SCHEMA_NAME STRING, TABLE_NAME STRING,
        KEY_COLS ARRAY, TOTAL_ROWS NUMBER, DISTINCT_ROWS NUMBER,
        UNIQUENESS_RATIO FLOAT, EXACT_UNIQUE BOOLEAN,
        SAMPLE_PCT FLOAT, SAMPLE_ROWS NUMBER, AVG_ROW_BYTES FLOAT,
        UNIQUENESS_THRESHOLD FLOAT,
        COL_STATS VARIANT,        -- array of per-column stats from sample
        SELECTED_PATH VARIANT,    -- step-by-step greedy path
        CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
      )
    """).collect()

    payload_cols = json.dumps([c for c in selected])
    payload_stats = json.dumps(col_stats)
    payload_path = json.dumps(selected_path)

    session.sql(f"""
      INSERT INTO {fq_cfg} (
        DB_NAME, SCHEMA_NAME, TABLE_NAME,
        KEY_COLS, TOTAL_ROWS, DISTINCT_ROWS,
        UNIQUENESS_RATIO, EXACT_UNIQUE,
        SAMPLE_PCT, SAMPLE_ROWS, AVG_ROW_BYTES,
        UNIQUENESS_THRESHOLD,
        COL_STATS, SELECTED_PATH
      )
      SELECT
        '{db}','{sch}','{tab}',
        PARSE_JSON('{payload_cols}'),
        {total_rows},{distinct_rows},
        {final_ratio},{str(exact_unique).lower()},
        {SAMPLE_PCT},{sample_rows},{avg_row_bytes},
        {UNIQUENESS_THRESHOLD},
        PARSE_JSON('{payload_stats}'),
        PARSE_JSON('{payload_path}')
    """).collect()

    return (f"[OK] {fq_tbl} → selected {len(selected)} key column(s): {selected} | "
            f"uniqueness={final_ratio:.6f} | sample_rows={sample_rows} | avg_row_bytes={avg_row_bytes:.1f}")
$$;
-------




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
