CREATE OR REPLACE PROCEDURE CUR_BDA.DATA_COMM.PGP_ENCRYPTION(
    "SOURCE_TABLE" VARCHAR, 
    "FILE_FORMAT" VARCHAR, 
    "TEMP_STAGE_PATH" VARCHAR, 
    "OUTPUT_STAGE_PATH" VARCHAR, 
    "WORLDPAY_KEY_PATH" VARCHAR, 
    "CLIENT_KEY_PATH" VARCHAR, 
    "FILE_PREFIX" VARCHAR, 
    "BATCH_SIZE" NUMBER(38,0) DEFAULT 15000000, 
    "DELIMITER" VARCHAR DEFAULT '|', 
    "INCLUDE_HEADER" BOOLEAN DEFAULT FALSE
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python','python-gnupg')
HANDLER = 'encrypt_table_data_handler'
EXECUTE AS CALLER
AS '
# ============================================================================
# IMPORTS AND INITIALIZATION
# ============================================================================

import gnupg                          # For PGP encryption operations
import os                            # For file system operations
import tempfile                      # For temporary directory management
import datetime                      # For timestamp generation
import csv                          # For CSV file processing
import math                         # For batch calculations
import shutil                       # For directory cleanup
import re                          # For email extraction from keys
from snowflake.snowpark import Session
from snowflake.snowpark.files import SnowflakeFile

# ============================================================================
# MAIN ENCRYPTION HANDLER
# ============================================================================

def encrypt_table_data_handler(session, source_table, file_format, temp_stage_path, 
                              output_stage_path, worldpay_key_path, client_key_path, 
                              file_prefix, batch_size, delimiter, include_header):
    """
    Main handler for PGP encryption process.
    
    This function orchestrates the entire encryption workflow:
    1. Validates access to all required resources
    2. Sets up GnuPG environment and imports keys
    3. Processes data in batches to manage memory usage
    4. Encrypts each batch with multi-recipient encryption
    5. Creates Tag File files and sends notifications
    
    Args:
        session: Snowflake session object
        source_table: Full name of source table to encrypt
        file_format: ''CSV'' or ''PARQUET'' - determines output format
        temp_stage_path: Staging area for temporary files
        output_stage_path: Final destination for encrypted files
        worldpay_key_path: Location of Worldpay''s public key
        client_key_path: Location of Client''s public key
        file_prefix: Prefix for all output files
        batch_size: Number of records to process per batch
        delimiter: Field separator for CSV files
        include_header: Whether to include column headers in CSV
    
    Returns:
        str: Detailed summary of execution results
    """
    
    # Initialize process tracking variables
    result_message = []                    # Detailed log of all operations
    proc_name = f"{file_prefix.upper()}_PGP_ENCRYPTION"
    inserted_rows = 0                      # Total records processed
    total_files_processed = 0              # Count of encrypted files created
    temp_dirs = []                         # Track temp directories for cleanup
    tag_file_entries = []                  # Tag File of created files (filename|rowcount)
    gpg = None                            # GnuPG instance
    gpg_home = None                       # Temporary GnuPG home directory
    
    # Get current time in EST for consistent timestamping
    est_now = session.sql("SELECT CONVERT_TIMEZONE(''America/Los_Angeles'', ''America/New_York'', CURRENT_TIMESTAMP()) AS est_time").collect()[0][''EST_TIME'']
    time_called = est_now.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # ====================================================================
        # PHASE 1: ACCESS VALIDATION AND ENVIRONMENT SETUP
        # ====================================================================
        
        result_message.append("=" * 60)
        result_message.append("STARTING PGP ENCRYPTION PROCESS")
        result_message.append("=" * 60)
        result_message.append(f"Process Name: {proc_name}")
        result_message.append(f"Start Time: {time_called}")
        result_message.append(f"Source Table: {source_table}")
        result_message.append(f"File Format: {file_format}")
        result_message.append(f"Batch Size: {batch_size:,} records")
        result_message.append("")
        
        # Validate access to all required resources before starting
        try:
            result_message.append("PHASE 1: Validating Access to Resources")
            result_message.append("-" * 40)
            
            # Check source table accessibility
            result_message.append(f"• Verifying access to source table: {source_table}")
            access_check = session.sql(f"SELECT COUNT(*) FROM {source_table} LIMIT 1").collect()
            result_message.append("  ✓ Source table is accessible")
            
            # Validate temporary staging area
            temp_stage = temp_stage_path if temp_stage_path.startswith(''@'') else f''@{temp_stage_path}''
            result_message.append(f"• Verifying access to temporary stage: {temp_stage}")
            session.sql(f"LIST {temp_stage}").collect()
            result_message.append("  ✓ Temporary stage is accessible")
            
            # Validate output destination
            output_stage = output_stage_path if output_stage_path.startswith(''@'') else f''@{output_stage_path}''
            result_message.append(f"• Verifying access to output stage: {output_stage}")
            session.sql(f"LIST {output_stage}").collect()
            result_message.append("  ✓ Output stage is accessible")
            
            # Clean temporary stage to ensure fresh start
            result_message.append(f"• Clearing temporary stage for clean processing")
            session.sql(f"remove {temp_stage}").collect()
            result_message.append("  ✓ Temporary stage cleared")
            result_message.append("")
            
        except Exception as e:
            error_msg = f"CRITICAL ERROR: Access validation failed - {str(e)}"
            result_message.append(error_msg)
            log_process(session, proc_name, time_called, 0, ''F'', error_msg, result_message)
            send_error_email(session, proc_name, error_msg)
            return "\\n".join(result_message)
        
        # ====================================================================
        # PHASE 2: GNUPG SETUP AND KEY MANAGEMENT
        # ====================================================================
        
        result_message.append("PHASE 2: Setting Up Encryption Environment")
        result_message.append("-" * 40)
        
        # Generate timestamp for file naming (consistent across all files)
        date_str = est_now.strftime("%Y%m%d")
        time_str = est_now.strftime("%H%M%S")
        date_time_str = f"{date_str}_{time_str}"
        result_message.append(f"• Using timestamp for file naming: {date_time_str}")
        
        # Initialize GnuPG with temporary keyring
        try:
            gpg_home = tempfile.mkdtemp()          # Create isolated keyring directory
            temp_dirs.append(gpg_home)             # Track for cleanup
            gpg = gnupg.GPG(gnupghome=gpg_home)    # Initialize GnuPG instance
            result_message.append(f"• GnuPG environment initialized successfully")
        except Exception as e:
            error_msg = f"CRITICAL ERROR: GnuPG initialization failed - {str(e)}"
            result_message.append(error_msg)
            cleanup_temp_dirs(temp_dirs)
            send_error_email(session, proc_name, error_msg)
            log_process(session, proc_name, time_called, 0, ''F'', error_msg, result_message)
            return "\\n".join(result_message)
        
        # Import and validate both encryption keys
        key_fingerprints = []                      # Store key fingerprints for encryption
        key_emails = []                           # Store associated email addresses
        key_paths = [worldpay_key_path, client_key_path]
        key_names = ["WORLDPAY", "CLIENT"]
        
        result_message.append("")
        result_message.append("Importing PGP Public Keys:")
        
        for key_idx, (key_path, key_name) in enumerate(zip(key_paths, key_names)):
            try:
                result_message.append(f"• Processing {key_name} key from: {key_path}")
                
                # Create temporary workspace for key download
                temp_key_dir = tempfile.mkdtemp()
                temp_dirs.append(temp_key_dir)
                
                # Download key from Snowflake stage
                key_stage_path = key_path if key_path.startswith(''@'') else f''@{key_path}''
                session.file.get(key_stage_path, temp_key_dir)
                
                # Locate downloaded key file
                downloaded_files = os.listdir(temp_key_dir)
                if not downloaded_files:
                    raise Exception(f"Key file not found after download from {key_stage_path}")
                
                local_key_path = os.path.join(temp_key_dir, downloaded_files[0])
                
                # Import key into GnuPG keyring
                with open(local_key_path, ''rb'') as key_file:
                    key_data = key_file.read()
                
                import_result = gpg.import_keys(key_data)
                
                if import_result.count == 0:
                    raise Exception(f"Failed to import key from {key_path}")
                
                # Extract key information for logging
                imported_key = import_result.fingerprints[0]
                key_fingerprints.append(imported_key)
                
                # Extract email addresses from key user IDs
                key_details = gpg.list_keys(keys=[imported_key])
                if key_details:
                    key_info = key_details[0]
                    user_ids = key_info.get(''uids'', [])
                    emails_found = []
                    
                    # Parse email addresses from user ID strings
                    for uid in user_ids:
                        # Look for standard format: "Name <email@domain.com>"
                        email_match = re.search(r''<([^>]+@[^>]+)>'', uid)
                        if email_match:
                            emails_found.append(email_match.group(1))
                        # Handle standalone email addresses
                        elif ''@'' in uid and ''.'' in uid:
                            emails_found.append(uid.strip())
                    
                    # Log key import results
                    if emails_found:
                        key_emails.extend(emails_found)
                        email_list = '', ''.join(emails_found)
                        result_message.append(f"  ✓ Key imported successfully")
                        result_message.append(f"    Fingerprint: {imported_key[:16]}...")
                        result_message.append(f"    Email(s): {email_list}")
                    else:
                        key_emails.append("No email found")
                        result_message.append(f"  ✓ Key imported successfully")
                        result_message.append(f"    Fingerprint: {imported_key[:16]}...")
                        result_message.append(f"    Email(s): No email address found in key")
                else:
                    key_emails.append("Key details not available")
                    result_message.append(f"  ✓ Key imported successfully")
                    result_message.append(f"    Fingerprint: {imported_key[:16]}...")
                    result_message.append(f"    Email(s): Key details not available")
                
            except Exception as e:
                error_msg = f"CRITICAL ERROR: Failed to import {key_name} key from {key_path} - {str(e)}"
                result_message.append(f"  ✗ {error_msg}")
                cleanup_temp_dirs(temp_dirs)
                send_error_email(session, proc_name, error_msg)
                log_process(session, proc_name, time_called, 0, ''F'', error_msg, result_message)
                return "\\n".join(result_message)
        
        # Verify both keys were imported successfully
        if len(key_fingerprints) != 2:
            error_msg = f"CRITICAL ERROR: Key import incomplete - only {len(key_fingerprints)} of 2 keys imported successfully"
            result_message.append(error_msg)
            cleanup_temp_dirs(temp_dirs)
            send_error_email(session, proc_name, error_msg)
            log_process(session, proc_name, time_called, 0, ''F'', error_msg, result_message)
            return error_msg
        
        result_message.append("")
        result_message.append("✓ Both keys imported successfully")
        result_message.append(f"✓ Encryption recipients: {'', ''.join(key_emails) if key_emails else ''Email information not available''}")
        result_message.append("")
        
        # ====================================================================
        # PHASE 3: DATA ANALYSIS AND BATCH PLANNING
        # ====================================================================
        
        result_message.append("PHASE 3: Analyzing Source Data")
        result_message.append("-" * 40)
        
        # Determine total record count for batch planning
        count_query = f"SELECT COUNT(*) AS total_count FROM {source_table}"
        result_message.append(f"• Executing row count query: {count_query}")
        
        count_result = session.sql(count_query).collect()
        total_records = count_result[0][''TOTAL_COUNT'']
        inserted_rows = total_records  # For logging purposes
        
        # Calculate optimal batch processing plan
        num_batches = math.ceil(total_records / batch_size)
        result_message.append(f"• Total records found: {total_records:,}")
        result_message.append(f"• Batch size configured: {batch_size:,} records")
        result_message.append(f"• Number of batches required: {num_batches}")
        
        # Validate file format support
        file_format = file_format.upper()
        if file_format not in [''CSV'', ''PARQUET'']:
            error_msg = f"CRITICAL ERROR: Unsupported file format ''{file_format}'' - Only CSV and PARQUET are supported"
            result_message.append(error_msg)
            cleanup_temp_dirs(temp_dirs)
            send_error_email(session, proc_name, error_msg)
            log_process(session, proc_name, time_called, 0, ''F'', error_msg, result_message)
            return error_msg
        
        result_message.append(f"• File format validated: {file_format}")
        result_message.append("")
        
        # ====================================================================
        # PHASE 4: BATCH PROCESSING AND ENCRYPTION
        # ====================================================================
        
        result_message.append("PHASE 4: Processing Data Batches")
        result_message.append("-" * 40)
        result_message.append("Beginning batch-by-batch processing to optimize memory usage...")
        result_message.append("")
        
        # Process each batch sequentially
        for batch_idx in range(num_batches):
            offset = batch_idx * batch_size
            batch_number = f"{batch_idx+1:03d}"  # Zero-padded batch number
            
            result_message.append(f"Processing Batch {batch_idx+1} of {num_batches}")
            result_message.append(f"  Records: {offset:,} to {min(offset + batch_size, total_records):,}")
            
            # ================================================================
            # STEP 4A: DATA EXTRACTION
            # ================================================================
            
            # Define file names and paths for this batch
            file_extension = ''.csv'' if file_format == ''CSV'' else ''.parquet''
            temp_file_name = f"{file_prefix}_{date_time_str}_{batch_number}{file_extension}"
            temp_file_path = f"{temp_stage}/{temp_file_name}"
            
            # Build appropriate COPY command based on file format
            if file_format == ''CSV'':
                copy_command = f"""
                COPY INTO {temp_file_path}
                FROM (SELECT * FROM {source_table} LIMIT {batch_size} OFFSET {offset})
                FILE_FORMAT = (
                    TYPE = ''CSV''
                    FIELD_DELIMITER = ''{delimiter}''
                    EMPTY_FIELD_AS_NULL = FALSE
                    NULL_IF = (),
                    COMPRESSION = ''NONE''
                )
                HEADER = {''TRUE'' if include_header else ''FALSE''}
                MAX_FILE_SIZE = 5368709120
                OVERWRITE = TRUE
                SINGLE = TRUE
                """
            else:  # PARQUET format
                copy_command = f"""
                COPY INTO {temp_file_path}
                FROM (SELECT * FROM {source_table} LIMIT {batch_size} OFFSET {offset})
                FILE_FORMAT = (TYPE = ''PARQUET'')
                MAX_FILE_SIZE = 5368709120
                OVERWRITE = TRUE
                SINGLE = TRUE
                """
            
            try:
                # Execute data extraction
                copy_result = session.sql(copy_command).collect()
                result_message.append(f"  Data extracted to temporary file: {temp_file_name}")
                
            except Exception as e:
                error_msg = f"ERROR: Data extraction failed for batch {batch_idx+1} - {str(e)}"
                result_message.append(f"  ✗ {error_msg}")
                cleanup_temp_dirs(temp_dirs)
                send_error_email(session, proc_name, error_msg)
                log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                return "\\n".join(result_message)
            
            # ================================================================
            # STEP 4B: FILE PROCESSING AND ENCRYPTION
            # ================================================================
            
            # Create isolated workspace for this batch
            temp_file_dir = tempfile.mkdtemp()
            temp_dirs.append(temp_file_dir)
            
            try:
                # Download extracted file to local processing area
                session.file.get(temp_file_path, temp_file_dir)
                
                # Locate the downloaded file
                downloaded_files = os.listdir(temp_file_dir)
                if not downloaded_files:
                    error_msg = f"ERROR: Downloaded file not found for batch {batch_idx+1}: {temp_file_name}"
                    result_message.append(f"  ✗ {error_msg}")
                    cleanup_temp_dirs(temp_dirs)
                    send_error_email(session, proc_name, error_msg)
                    log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                    return "\\n".join(result_message)
                
                local_file_path = os.path.join(temp_file_dir, downloaded_files[0])
                
                # Prepare encrypted file naming
                is_csv = file_format == ''CSV''
                encrypted_extension = ".csv.pgp" if is_csv else ".parquet.pgp"
                encrypted_file_name = f"{file_prefix}_{date_time_str}_{batch_number}{encrypted_extension}"
                encrypted_file_path = os.path.join(temp_file_dir, encrypted_file_name)
                
                # Count rows for CSV files (for Tag File tracking)
                row_count = 0
                if is_csv:
                    try:
                        # Use CSV module for accurate row counting
                        with open(local_file_path, ''r'', newline='''') as csvfile:
                            csv_reader = csv.reader(csvfile, delimiter=delimiter)
                            row_count = sum(1 for row in csv_reader)
                        
                        result_message.append(f"  Row count: {row_count:,} rows")
                        tag_file_entries.append(f"{encrypted_file_name}|{row_count}")
                        
                    except Exception as e:
                        # Fallback counting method
                        try:
                            with open(local_file_path, ''r'') as f:
                                row_count = sum(1 for line in f)
                            result_message.append(f"  Row count determined (fallback method): {row_count:,} rows")
                            tag_file_entries.append(f"{encrypted_file_name}|{row_count}")
                        except Exception as e2:
                            error_msg = f"ERROR: Row counting failed for batch {batch_idx+1} - {str(e2)}"
                            result_message.append(f"  ✗ {error_msg}")
                            cleanup_temp_dirs(temp_dirs)
                            send_error_email(session, proc_name, error_msg)
                            log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                            return "\\n".join(result_message)
                
                # ============================================================
                # CRITICAL: MULTI-RECIPIENT PGP ENCRYPTION
                # ============================================================
                
                try:
                    with open(local_file_path, ''rb'') as f:
                        # Encrypt file for both recipients simultaneously
                        # This creates ONE encrypted file that can be decrypted by EITHER key
                        encrypted_data = gpg.encrypt_file(
                            f, 
                            recipients=key_fingerprints,  # Both Worldpay and Client keys
                            always_trust=True,            # Trust imported keys
                            armor=False                   # Binary output (.pgp format)
                        )
                    
                    # Verify encryption was successful
                    if not encrypted_data.ok:
                        raise Exception(f"Encryption operation failed: {encrypted_data.status}")
                    
                    # Write encrypted data to output file
                    with open(encrypted_file_path, ''wb'') as f:
                        f.write(encrypted_data.data)
                    
                    result_message.append(f"  File encrypted with both keys")
                    
                except Exception as e:
                    error_msg = f"ERROR: Encryption failed for batch {batch_idx+1} - {str(e)}"
                    result_message.append(f" ✗ {error_msg}")
                    cleanup_temp_dirs(temp_dirs)
                    send_error_email(session, proc_name, error_msg)
                    log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                    return "\\n".join(result_message)
                
                # ============================================================
                # STEP 4C: UPLOAD TO FINAL DESTINATION
                # ============================================================
                
                try:
                    # Upload encrypted file to output stage
                    session.file.put(encrypted_file_path, output_stage, overwrite=True, auto_compress=False)
                    result_message.append(f"  Encrypted file uploaded: {encrypted_file_name}")
                    
                except Exception as stage_error:
                    error_msg = f"ERROR: Failed to upload encrypted file for batch {batch_idx+1} - {str(stage_error)}"
                    result_message.append(f" ✗ {error_msg}")
                    cleanup_temp_dirs(temp_dirs)
                    send_error_email(session, proc_name, error_msg)
                    log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                    return "\\n".join(result_message)
                
                total_files_processed += 1
                result_message.append(f"  Batch {batch_idx+1} completed successfully")
                result_message.append("")
                
            except Exception as e:
                error_msg = f"ERROR: Batch processing failed for batch {batch_idx+1} - {str(e)}"
                result_message.append(f" ✗ {error_msg}")
                cleanup_temp_dirs(temp_dirs)
                send_error_email(session, proc_name, error_msg)
                log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                return "\\n".join(result_message)
            
            # Clean up batch workspace before proceeding to next batch
            try:
                shutil.rmtree(temp_file_dir, ignore_errors=True)
                temp_dirs.remove(temp_file_dir)
            except Exception as e:
                result_message.append(f"Note: Could not clean up temporary directory - {str(e)}")
        
        # ====================================================================
        # PHASE 5: Tag File FILE CREATION AND FINAL CLEANUP
        # ====================================================================
        
        result_message.append("PHASE 5: Creating Tag File and Final Cleanup")
        result_message.append("-" * 40)
        
        # Create tag file (Tag File) for CSV files only
        if tag_file_entries:
            try:
                tag_file_name = f"{file_prefix}_{date_time_str}.tag"
                
                # Create workspace for tag file
                tag_temp_dir = tempfile.mkdtemp()
                temp_dirs.append(tag_temp_dir)
                tag_file_path = os.path.join(tag_temp_dir, tag_file_name)
                
                # Write Tag File content (filename|rowcount format)
                with open(tag_file_path, ''w'') as f:
                    f.write(''\\n''.join(tag_file_entries))
                
                # Upload Tag File file
                session.file.put(tag_file_path, output_stage, overwrite=True, auto_compress=False)
                result_message.append(f"✓ Tag File file created and uploaded: {tag_file_name}")
                result_message.append(f"  Contains {len(tag_file_entries)} file entries")
                
            except Exception as e:
                error_msg = f"ERROR: Tag File file creation failed - {str(e)}"
                result_message.append(f"• ✗ {error_msg}")
                cleanup_temp_dirs(temp_dirs)
                send_error_email(session, proc_name, error_msg)
                log_process(session, proc_name, time_called, total_files_processed, ''F'', error_msg, result_message)
                return "\\n".join(result_message)
        else:
            result_message.append("• No CSV files processed - Tag File file not required")
        
        # Final cleanup of temporary staging area
        try:
            result_message.append(f"•Performing final cleanup of temporary stage: {temp_stage}")
            session.sql(f"remove {temp_stage}").collect()
            result_message.append("✓ Final temporary stage cleanup completed")
        except Exception as e:
            # Log but don''t fail - cleanup is not critical at this point
            result_message.append(f"• Note: Could not complete final stage cleanup - {str(e)}")
        
        # Clean up all local temporary directories
        cleanup_temp_dirs(temp_dirs)
        
        # ====================================================================
        # PHASE 6: SUCCESS LOGGING AND NOTIFICATIONS
        # ====================================================================
        
        result_message.append("")
        result_message.append("=" * 60)
        result_message.append("ENCRYPTION PROCESS COMPLETED SUCCESSFULLY")
        result_message.append("=" * 60)
        
        # Create comprehensive success summary
        success_message = f"PGP Encryption completed successfully. Processed {total_files_processed} files with {total_records:,} total records using multi-recipient encryption."
        success_message += f"\\nEncryption recipients: {'', ''.join(key_emails) if key_emails else ''Email information not available''}"
        
        result_details = "\\n".join(result_message)
        full_success_message = f"{success_message}\\n\\nDetailed Log:\\n{result_details}"
        
        # Log successful completion to database
        log_process(session, proc_name, time_called, inserted_rows, ''C'', 
                   f"PGP Encryption completed successfully. Files processed: {total_files_processed}", 
                   result_message)
        
        # Send success notification email
        send_success_email(session, proc_name, full_success_message)
        
        return full_success_message
    
    except Exception as e:
        # ====================================================================
        # CRITICAL ERROR HANDLING
        # ====================================================================
        
        # Capture detailed error information for troubleshooting
        import traceback
        error_details = traceback.format_exc()
        error_message = f"CRITICAL SYSTEM ERROR: {str(e)}\\n\\nTechnical Details:\\n{error_details}"
        
        result_message.append("")
        result_message.append("=" * 60)
        result_message.append("CRITICAL ERROR OCCURRED")
        result_message.append("=" * 60)
        result_message.append(error_message)
        
        # Perform emergency cleanup
        cleanup_temp_dirs(temp_dirs)
        
        # Attempt to clean temporary stage on error
        try:
            session.sql(f"remove {temp_stage}").collect()
            result_message.append("• cleanup of temporary stage completed")
        except:
            result_message.append("• Warning: Could not clean temporary stage during error recovery")
        
        # Log error to database for tracking
        log_process(session, proc_name, time_called, 0, ''F'', error_message, result_message)
        
        # Send urgent error notification
        send_error_email(session, proc_name, error_message)
        
        return error_message

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_process(session, proc_name, time_called, inserted_rows, process_flag, message, result_message=None):
    """
    Log process execution status to the database log table.
    
    Args:
        session: Snowflake session object
        proc_name: Process name for identification
        time_called: Process start timestamp
        inserted_rows: Number of records processed
        process_flag: ''C'' for Complete, ''F'' for Failed
        message: Primary status message
        result_message: Detailed execution log (optional)
    """
    try:
        # Combine primary message with detailed log if available
        full_message = message
        if result_message:
            full_message = message + "\\n\\nDetailed Execution Log:\\n" + "\\n".join(result_message)
        
        # Escape single quotes for SQL insertion
        escaped_message = full_message.replace("''", "''''")
        
        # Insert log record
        log_query = f"""
        INSERT INTO CUR_BDA.DATA_COMM.REPORT_PROCESS_LOGS
        VALUES (
            ''{proc_name}'',                           -- PROCESS_NAME
            CURRENT_DATE(),                          -- PROCESS_DATE
            ''{time_called}'',                         -- PROCESS_START_TIME
            CURRENT_TIMESTAMP(3)::timestamp_ntz,     -- PROCESS_END_TIME (EST)
            ''{process_flag}'',                        -- PROCESS_FLAG (''C''=Complete, ''F''=Failed)
            {inserted_rows},                         -- PROCESS_ROWS_INSERTED
            ''{escaped_message}''                      -- PROCESS_ERROR_MESSAGE (or success details)
        )
        """
        session.sql(log_query).collect()
        
    except Exception as e:
        # If audit logging fails, print to console but don''t raise exception
        # This prevents log failures from masking the original process result
        print(f"WARNING: Failed to write audit log to REPORT_PROCESS_LOGS table: {str(e)}")

def send_success_email(session, proc_name, success_message):
    """
    Send success notification email to the team.
    
    Args:
        session: Snowflake session object
        proc_name: Process name for email subject
        success_message: Detailed success summary
    """
    try:
        # Escape single quotes for SQL string literals
        escaped_message = success_message.replace("''", "''''")
        
        # Send notification using Snowflake email system
        email_query = f"""
        CALL SYSTEM$SEND_EMAIL(
            ''SF_SEND_EMAIL_NOTIFICATION_MERCHANTS'',     -- Email integration name
            ''datalicensesupport@worldpay.com'',          -- Recipient email
            ''{proc_name} - PGP Encryption Process Completed Successfully'',  -- Subject
            ''Process: {proc_name}\\n\\n{escaped_message}'',  -- Body content
            ''text/plain''                                -- Content type
        )
        """
        session.sql(email_query).collect()
        
    except Exception as e:
        # If email notification fails, log warning but don''t raise exception
        # Email failures should not impact the core encryption process
        print(f"WARNING: Failed to send success notification email: {str(e)}")

def send_error_email(session, proc_name, error_message):
    """
    
    This notification alerts the team to encryption failures that require
    immediate attention and investigation.
    
    Args:
        session: Snowflake session object
        proc_name: Process name for email subject
        error_message: Detailed error information
    """
    try:
        escaped_message = error_message.replace("''", "''''")
        
        # Send notification with clear error flag in subject
        email_query = f"""
        CALL SYSTEM$SEND_EMAIL(
            ''SF_SEND_EMAIL_NOTIFICATION_MERCHANTS'',     -- Email integration name
            ''datalicensesupport@worldpay.com'',          -- Recipient email
            ''URGENT: {proc_name} - PGP Encryption Process Failed'',  -- Clear error subject
            ''Process: {proc_name}\\n\\nERROR DETAILS:\\n{escaped_message}'',  -- Error details
            ''text/plain''                                -- Content type
        )
        """
        session.sql(email_query).collect()
        
    except Exception as e:
        # If error email fails, log warning but don''t raise exception
        # Email failures should not mask the original error
        print(f"WARNING: Failed to send error notification email: {str(e)}")

def cleanup_temp_dirs(temp_dirs):
    """
    Clean up all temporary directories created during processing.
    
    Args:
        temp_dirs: List of temporary directory paths to clean up
    """
    for dir_path in temp_dirs:
        try:
            if os.path.exists(dir_path):
                # Recursively remove directory and all contents
                shutil.rmtree(dir_path, ignore_errors=True)
        except Exception:
            # Ignore cleanup failures - they''re not critical for process success
            # but we don''t want cleanup failures to mask other issues
            pass

# ============================================================================
# END OF STORED PROCEDURE
# ============================================================================

';
