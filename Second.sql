-- How big is a shard? (dry run idea)
WITH s AS (
  SELECT HASH_SHA2(OBJECT_CONSTRUCT_KEEP_NULL(/* same obj_pairs */),256) AS rec_fp
  FROM DB.SCHEMA.TABLE
)
SELECT :N_SHARDS AS n_shards, :SHARD_ID AS shard_id, COUNT(*) AS rows_in_shard
FROM s
WHERE MOD(ABS(rec_fp), :N_SHARDS) = :SHARD_ID;
