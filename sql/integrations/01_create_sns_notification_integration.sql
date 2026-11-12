-- Chicago 311: Snowflake -> AWS SNS Notification Integration
-- Allows Snowflake to publish batch-ready notifications to SNS.

CREATE NOTIFICATION INTEGRATION CHICAGO_311_SNS_NOTIFICATION_INT
    ENABLED = TRUE
    TYPE = QUEUE
    DIRECTION = OUTBOUND
    NOTIFICATION_PROVIDER = AWS_SNS
    AWS_SNS_TOPIC_ARN = 'YOUR_SNS_TOPIC_ARN'
    AWS_SNS_ROLE_ARN = 'YOUR_IAM_ROLE_ARN'
    COMMENT = 'Sends Chicago 311 batch-ready notifications to AWS SNS';

-- Retrieve Snowflake's IAM user ARN and external ID.
DESCRIBE INTEGRATION CHICAGO_311_SNS_NOTIFICATION_INT;