-- Pick your shard plan for this run
SET N_SHARDS = 256;
SET SHARD_ID = 0;   -- change to any 0..N_SHARDS-1

-- A) How big is one shard (quick dry-run)
WITH s AS (SELECT REC_FP FROM V_<TAB>_WITH_FP)
SELECT $N_SHARDS AS n_shards,
       $SHARD_ID AS shard_id,
       COUNT(*)   AS rows_in_shard
FROM s
WHERE MOD(ABS(REC_FP), $N_SHARDS) = $SHARD_ID;

-- B) Distribution across ALL shards (checks for skew)
WITH s AS (
  SELECT MOD(ABS(REC_FP), $N_SHARDS) AS shard_id
  FROM V_<TAB>_WITH_FP
)
SELECT shard_id,
       COUNT(*)                                   AS rows_in_shard,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM s
GROUP BY 1
ORDER BY 1;

-- C) Coverage / totals (optional)
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT REC_FP) AS distinct_row_fps   -- if you plan to dedupe exact duplicates
FROM V_<TAB>_WITH_FP;
-------
WITH s AS (
  SELECT CONV(HASH_SHA2(OBJECT_CONSTRUCT_KEEP_NULL(
           'col1', col1, 'col2', col2, ...), 256), 16, 10) AS rec_fp
  FROM DB.SCHEMA.TABLE
)
SELECT $N_SHARDS  AS n_shards,
       $SHARD_ID  AS shard_id,
       COUNT(*)   AS rows_in_shard
FROM s
WHERE MOD(rec_fp, $N_SHARDS) = $SHARD_ID;

