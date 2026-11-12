-- Chicago 311 Service Intelligence
-- File: sql/orchestration/service_request_process_task.sql
-- Updates the existing triggered task to call manifest validation.

USE ROLE ACCOUNTADMIN;

-- Suspend the task before modifying it.
ALTER TASK
    CHICAGO_311_DEV.BRONZE.SERVICE_REQUEST_PROCESS_TASK
    SUSPEND;

-- Replace the placeholder SQL with our validation procedure.
ALTER TASK
    CHICAGO_311_DEV.BRONZE.SERVICE_REQUEST_PROCESS_TASK
    MODIFY AS
    CALL CHICAGO_311_DEV.BRONZE.VALIDATE_MANIFEST();