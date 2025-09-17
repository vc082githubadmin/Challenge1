N_SHARDS = 256
        
        # for batch_idx in range(num_batches):
        for SHARD_ID in range (N_SHARDS):
            # offset = batch_idx * batch_size
            # batch_number = f"{batch_idx+1:03d}"  # Zero-padded batch number
            batch_number = f"{SHARD_ID+1:03d}"  # Zero-padded batch number
            
            # result_message.append(f"Processing Batch {batch_idx+1} of {num_batches}")
            result_message.append(f"Processing Batch {SHARD_ID+1} of {num_batches}")
            
            #result_message.append(f"  Records: {offset:,} to {min(offset + batch_size, total_records):,}")
            cnt_sql = f"""
            SELECT COUNT(*) AS rows_in_shard
            FROM {db}.{sch}.v_{tab}_with_fp
            WHERE MOD(ABS(REC_FP), {N_SHARDS}) = {SHARD_ID}
            """
            cnt = session.sql(cnt_sql).collect()[0]['ROWS_IN_SHARD']
            
            result_message.append(f"Shard {SHARD_ID}/{N_SHARDS} → {cnt:,} rows")
            # ================================================================
            # STEP 4A: DATA EXTRACTION
            # ================================================================
            
            # Define file names and paths for this batch
            file_extension = '.csv' if file_format == 'CSV' else '.parquet'
            temp_file_name = f"{file_prefix}_{date_time_str}_{batch_number}{file_extension}"
            temp_file_path = f"{temp_stage}{temp_file_name}"
            
            # Build appropriate COPY command based on file format
            if file_format == 'CSV':
                copy_command = f"""
                COPY INTO {temp_file_path}
                FROM (select * from {db}.{sch}.v_{tab}_with_fp where mod(abs(REC_FP), {N_SHARDS}) = {SHARD_ID})
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '{delimiter}'
                    EMPTY_FIELD_AS_NULL = FALSE
                    NULL_IF = (),
                    COMPRESSION = 'NONE'
                )
                HEADER = {'TRUE' if include_header else 'FALSE'}
                MAX_FILE_SIZE = 5368709120
                OVERWRITE = TRUE
                SINGLE = TRUE
                """
            else:  # PARQUET format
                copy_command = f"""
                COPY INTO {temp_file_path}
                FROM (select * from {db}.{sch}.v_{tab}_with_fp where mod(abs(REC_FP), {N_SHARDS}) = {SHARD_ID})
                FILE_FORMAT = (TYPE = 'PARQUET')
                MAX_FILE_SIZE = 5368709120
                OVERWRITE = TRUE
                SINGLE = TRUE
                """
            try:
                # Execute data extraction
                copy_result = session.sql(copy_command).collect()
                result_message.append(f"  Data extracted to temporary file: {temp_file_name}")
                
            except Exception as e:
                error_msg = f"ERROR: Data extraction failed for batch {SHARD_ID+1} - {str(e)}"
                result_message.append(f"  ✗ {error_msg}")
                # cleanup_temp_dirs(temp_dirs)
                # send_error_email(session, proc_name, error_msg)
                log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                return "\\n".join(result_message)
