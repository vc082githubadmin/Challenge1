result_message.append(f"  Records: {offset:,} to {min(offset + batch_size, total_records):,}")


cnt_sql = f"""
SELECT COUNT(*) AS rows_in_shard
FROM {db}.{sch}.v_{tab}_with_fp
WHERE MOD(ABS(REC_FP), {N_SHARDS}) = {SHARD_ID}
"""
cnt = session.sql(cnt_sql).collect()[0]['ROWS_IN_SHARD']

result_message.append(f"Shard {SHARD_ID+1}/{N_SHARDS} → {cnt:,} rows")
