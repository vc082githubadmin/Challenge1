select mod(abs(rec_fp), 256) as shard_id, count(*) from CUR_BDA.VIVID.v_CNVR_PAR_DATA_MTHLY_Test4_with_fp
group by 1 order by 1;

Output below:

SHARD_ID| COUNT(*)
12|950182314
