cnt_sql = f"""
                SELECT COUNT(*) AS rows_in_shard
                FROM {db}.{sch}.V_{tab_u}_WITH_FP
                WHERE MOD(ABS(REC_FP), {N_SHARDS}) = {SHARD_ID}
            """
