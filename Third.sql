# --- Split and normalize DB.SCHEMA.TABLE ---
src = str(source_table).strip()
db, sch, tab = [p.strip().strip('"') for p in src.split('.')]
db_u, sch_u, tab_u = db.upper(), sch.upper(), tab.upper()

# --- Build obj_pairs:  'COL1', "COL1", 'COL2', "COL2", ... in ordinal order ---
obj_pairs_sql = f"""
SELECT LISTAGG(
         QUOTE_LITERAL(COLUMN_NAME) || ', "' || COLUMN_NAME || '"', ', '
       ) WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS OBJ_PAIRS
FROM {db_u}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{sch_u}'
  AND TABLE_NAME   = '{tab_u}';
"""
row = session.sql(obj_pairs_sql).collect()[0]
obj_pairs = row['OBJ_PAIRS']
if not obj_pairs:
    raise ValueError(f"No columns found for {source_table}")

# --- Create the fingerprint view (numeric, suitable for MOD/ABS) ---
# Use HASH(...) on the VARIANT; do NOT wrap with TO_JSON.
create_fp_view_sql = f"""
CREATE OR REPLACE VIEW "{db}"."{sch}"."V_{tab_u}_WITH_FP" AS
SELECT t.*,
       HASH(OBJECT_CONSTRUCT_KEEP_NULL({obj_pairs})) AS REC_FP
FROM "{db}"."{sch}"."{tab}" AS t;
"""
session.sql(create_fp_view_sql).collect()
