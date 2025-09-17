for batch_idx in range(num_batches):
            offset = batch_idx * batch_size
            batch_number = f"{batch_idx+1:03d}"  # Zero-padded batch number
            
            result_message.append(f"Processing Batch {batch_idx+1} of {num_batches}")
            result_message.append(f"  Records: {offset:,} to {min(offset + batch_size, total_records):,}")
            
            # ================================================================
            # STEP 4A: DATA EXTRACTION
            # ================================================================
            
            # Define file names and paths for this batch
            file_extension = '.csv' if file_format == 'CSV' else '.parquet'
            temp_file_name = f"{file_prefix}_{date_time_str}_{batch_number}{file_extension}"
            temp_file_path = f"{temp_stage}{temp_file_name}"
            # ================================================================
            # 64/128/256 256 for 900 M works well
            # ================================================================
            
            N_SHARDS = 256 
            SHARD_ID = batch_idx
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
