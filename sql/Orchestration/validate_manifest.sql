-- Chicago 311 Service Intelligence
-- File: sql/orchestration/validate_manifest.sql
--
-- Validates all pending stream records against their manifests.
-- Supports multiple ingestion runs and multiple files per run.
--
-- PASS    : All pending batches match; notify AWS SNS.
-- NO_DATA : No pending stream records.
-- FAILURE : Raise an exception so Snowflake counts the task failure.
--
-- This procedure does not consume the Snowflake stream.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE
    CHICAGO_311_DEV.BRONZE.VALIDATE_MANIFEST()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_stream_files NUMBER DEFAULT 0;
    v_matched_rows NUMBER DEFAULT 0;
    v_missing_manifests NUMBER DEFAULT 0;
    v_invalid_manifests NUMBER DEFAULT 0;
    v_incomplete_runs NUMBER DEFAULT 0;
    v_excessive_runs NUMBER DEFAULT 0;
    v_expected_count NUMBER DEFAULT 0;
    v_actual_count NUMBER DEFAULT 0;
    v_delete_count NUMBER DEFAULT 0;
    v_message VARCHAR;

    -- Exceptions count as task failures.
    e_duplicate_manifest EXCEPTION
        (-20001, 'Duplicate manifest matches');

    e_stream_deletes EXCEPTION
        (-20002, 'Stream contains deletes');

    e_invalid_manifest EXCEPTION
        (-20003, 'Invalid manifest metadata');

    e_excessive_records EXCEPTION
        (-20004, 'Unexpected files or record counts');

    e_missing_manifest EXCEPTION
        (-20005, 'Manifest missing');

    e_incomplete_batch EXCEPTION
        (-20006, 'Batch incomplete');

    e_count_mismatch EXCEPTION
        (-20007, 'Total record count mismatch');

BEGIN
    WITH manifest_files AS (
        SELECT
            m.$1:run_id::VARCHAR AS run_id,
            TRY_TO_NUMBER(
                m.$1:record_count::VARCHAR
            ) AS expected_count,
            ARRAY_SIZE(m.$1:files) AS manifest_file_count,
            f.value:s3_key::VARCHAR AS source_file
        FROM @CHICAGO_311_DEV.BRONZE.S3_CHICAGO_311_MANIFEST m,
             LATERAL FLATTEN(INPUT => m.$1:files) f
    ),

    -- Count pending records by source file.
    stream_files AS (
        SELECT
            SOURCE_FILE,
            COALESCE(
                COUNT_IF(METADATA$ACTION = 'INSERT'), 0
            ) AS insert_count,
            COALESCE(
                COUNT_IF(METADATA$ACTION = 'DELETE'), 0
            ) AS delete_count
        FROM CHICAGO_311_DEV.BRONZE.SERVICE_REQUEST_RAW_STREAM
        GROUP BY SOURCE_FILE
    ),

    -- Associate each pending source file with its manifest.
    matched_files AS (
        SELECT
            s.source_file,
            s.insert_count,
            s.delete_count,
            m.run_id,
            m.expected_count,
            m.manifest_file_count
        FROM stream_files s
        LEFT JOIN manifest_files m
            ON s.source_file = m.source_file
    ),

    -- Validate counts independently for each ingestion run.
    run_summary AS (
        SELECT
            run_id,
            COUNT(DISTINCT source_file) AS present_files,
            MIN(manifest_file_count) AS min_manifest_files,
            MAX(manifest_file_count) AS expected_files,
            MIN(expected_count) AS min_expected_records,
            MAX(expected_count) AS expected_records,
            COALESCE(
                COUNT_IF(
                    expected_count IS NULL
                    OR manifest_file_count IS NULL
                ), 0
            ) AS invalid_metadata_rows,
            COALESCE(
                SUM(insert_count), 0
            ) AS actual_records
        FROM matched_files
        WHERE run_id IS NOT NULL
        GROUP BY run_id
    ),

    -- Stream-level statistics.
    stream_metrics AS (
        SELECT
            COUNT(*) AS stream_file_count,
            COALESCE(SUM(delete_count), 0) AS delete_count
        FROM stream_files
    ),

    -- Detect missing or duplicated manifest matches.
    match_metrics AS (
        SELECT
            COUNT(*) AS matched_row_count,
            COALESCE(
                COUNT_IF(run_id IS NULL), 0
            ) AS missing_manifest_count
        FROM matched_files
    ),

    -- Combine validation results from all ingestion runs.
    run_metrics AS (
        SELECT
            COALESCE(
                SUM(expected_records), 0
            ) AS total_expected,
            COALESCE(
                SUM(actual_records), 0
            ) AS total_actual,
            COALESCE(
                COUNT_IF(
                    invalid_metadata_rows > 0
                    OR expected_files IS NULL
                    OR expected_files < 1
                    OR expected_records IS NULL
                    OR expected_records <= 0
                    OR min_manifest_files != expected_files
                    OR min_expected_records != expected_records
                ), 0
            ) AS invalid_runs,
            COALESCE(
                COUNT_IF(
                    present_files < expected_files
                    OR actual_records < expected_records
                ), 0
            ) AS incomplete_runs,
            COALESCE(
                COUNT_IF(
                    present_files > expected_files
                    OR actual_records > expected_records
                ), 0
            ) AS excessive_runs
        FROM run_summary
    )

    SELECT
        s.stream_file_count,
        m.matched_row_count,
        m.missing_manifest_count,
        r.invalid_runs,
        r.incomplete_runs,
        r.excessive_runs,
        r.total_expected,
        r.total_actual,
        s.delete_count
    INTO
        :v_stream_files,
        :v_matched_rows,
        :v_missing_manifests,
        :v_invalid_manifests,
        :v_incomplete_runs,
        :v_excessive_runs,
        :v_expected_count,
        :v_actual_count,
        :v_delete_count
    FROM stream_metrics s
    CROSS JOIN match_metrics m
    CROSS JOIN run_metrics r;

    -- Nothing pending.
    IF (v_stream_files = 0) THEN
        RETURN 'NO_DATA';
    END IF;

    -- Detect duplicate manifest matches.
    IF (v_matched_rows != v_stream_files) THEN
        RAISE e_duplicate_manifest;
    END IF;

    -- Current consumer supports inserts only.
    IF (v_delete_count > 0) THEN
        RAISE e_stream_deletes;
    END IF;

    -- Invalid manifest metadata.
    IF (v_invalid_manifests > 0) THEN
        RAISE e_invalid_manifest;
    END IF;

    -- More files or records than expected.
    IF (v_excessive_runs > 0) THEN
        RAISE e_excessive_records;
    END IF;

    -- One or more source files lack a manifest.
    IF (v_missing_manifests > 0) THEN
        RAISE e_missing_manifest;
    END IF;

    -- One or more ingestion runs are incomplete.
    IF (v_incomplete_runs > 0) THEN
        RAISE e_incomplete_batch;
    END IF;

    -- Final consistency check.
    IF (v_actual_count != v_expected_count) THEN
        RAISE e_count_mismatch;
    END IF;

    -- All pending ingestion runs passed validation.
    -- Construct the notification expected by Lambda.
    SELECT TO_JSON(
        OBJECT_CONSTRUCT(
            'event', 'BATCH_READY',
            'expected_record_count', :v_expected_count
        )
    )
    INTO :v_message;

    -- Publish one notification for all validated records.
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
        SNOWFLAKE.NOTIFICATION.APPLICATION_JSON(:v_message),
        SNOWFLAKE.NOTIFICATION.INTEGRATION(
            'CHICAGO_311_SNS_NOTIFICATION_INT'
        )
    );

    RETURN 'PASS';
END;
$$;