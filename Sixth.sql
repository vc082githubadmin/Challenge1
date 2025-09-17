CREATE OR REPLACE PROCEDURE IDENTIFY_HASH_COLUMNS(
    DB_NAME STRING,
    SCHEMA_NAME STRING,
    TABLE_NAME STRING,
    SAMPLE_PCT FLOAT DEFAULT 0.02,        -- % of rows to sample
    UNIQUENESS_THRESHOLD FLOAT DEFAULT 0.9999, -- stop when ~99.99% unique
    CONFIG_TABLE STRING DEFAULT 'EXPORT_KEYS'  -- where to persist chosen cols
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
from snowflake.snowpark.functions import col, approx_count_distinct, avg, iff, lit, hash
import json

def run(session, db_name, schema_name, table_name, sample_pct, uniqueness_threshold, config_table):

    fq_table = f'"{db_name}"."{schema_name}"."{table_name}"'
    fq_config = f'"{db_name}"."{schema_name}"."{config_table}"'

    # 1. total rows
    total_rows = session.sql(f"SELECT COUNT(*) AS c FROM {fq_table}").collect()[0]["C"]

    # 2. fetch column list
    cols = session.sql(f"""
        SELECT COLUMN_NAME, ORDINAL_POSITION
        FROM {db_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """).collect()
    col_names = [r["COLUMN_NAME"] for r in cols]

    # 3. profile columns (on sample)
    stats = []
    for c in col_names:
        q = f"""
          SELECT
            APPROX_COUNT_DISTINCT("{c}") AS ndv,
            AVG(IFF("{c}" IS NULL,1,0))  AS null_rate
          FROM {fq_table} SAMPLE ({sample_pct*100} PERCENT)
        """
        row = session.sql(q).collect()[0]
        stats.append({
            "col": c,
            "ndv": row["NDV"],
            "null_rate": row["NULL_RATE"],
            "ratio": row["NDV"]/total_rows
        })

    # 4. rank columns: high cardinality, low nulls
    ranked = sorted(stats, key=lambda r: (-r["ratio"], r["null_rate"]))

    # 5. greedy selection
    selected = []
    distinct_rows = 0
    for r in ranked:
        selected.append(r["col"])
        obj_pairs = ", ".join([f"'{c}', \"{c}\"" for c in selected])
        check_sql = f"""
          SELECT COUNT(DISTINCT HASH(OBJECT_CONSTRUCT_KEEP_NULL({obj_pairs}))) AS d
          FROM {fq_table}
        """
        distinct_rows = session.sql(check_sql).collect()[0]["D"]
        if distinct_rows/total_rows >= uniqueness_threshold:
            break

    # 6. persist result into config table
    session.sql(f"""
      CREATE TABLE IF NOT EXISTS {fq_config} (
        DB_NAME STRING, SCHEMA_NAME STRING, TABLE_NAME STRING,
        KEY_COLS ARRAY, TOTAL_ROWS NUMBER, DISTINCT_ROWS NUMBER, AS_OF TIMESTAMP_LTZ
      )
    """).collect()

    ins = f"""
      INSERT INTO {fq_config}
      SELECT '{db_name}','{schema_name}','{table_name}',
             PARSE_JSON('{json.dumps(selected)}'),
             {total_rows},{distinct_rows},CURRENT_TIMESTAMP()
    """
    session.sql(ins).collect()

    return f"Selected key columns for {fq_table}: {selected}, uniqueness={distinct_rows/total_rows:.6f}"

$$;
