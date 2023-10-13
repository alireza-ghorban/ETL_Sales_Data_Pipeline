-- AWS S3 Bucket and Snowflake Role Configuration
----------------------------------------------------------
-- Note: Ensure that the S3 bucket with the name midterm-data-dump-ag is created in AWS.
-- And also create a policy and role in AWS to allow Snowflake access to this bucket.

-- Snowflake S3 Storage Integration
----------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION S3_WCD_MIDTERM_LOADING_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::480222816344:role/midterm-snowflake-stage-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://midterm-data-dump-ag');

-- Describe the storage integration to obtain IAM User ARN and External ID
----------------------------------------------------------
DESC STORAGE INTEGRATION S3_WCD_MIDTERM_LOADING_INTEGRATION;

-- Note: After getting the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID, 
-- attach them back to the IAM role in AWS.

-- Grant Permissions for Storage Integration and File Format
----------------------------------------------------------
GRANT CREATE STAGE ON SCHEMA RAW TO ROLE accountadmin;
GRANT USAGE ON INTEGRATION S3_WCD_MIDTERM_LOADING_INTEGRATION TO ROLE accountadmin;

-- Snowflake Stage Creation for S3 Integration
----------------------------------------------------------
CREATE OR REPLACE STAGE S3_WCD_MIDTERM_LOADING_STAGE
  STORAGE_INTEGRATION = S3_WCD_MIDTERM_LOADING_INTEGRATION
  URL = 's3://midterm-data-dump-ag'
  FILE_FORMAT = csv_comma_skip1_format;

-- Describe the stage to check its configuration
----------------------------------------------------------
DESC STAGE S3_WCD_MIDTERM_LOADING_STAGE;

-- List contents of the stage
----------------------------------------------------------
LIST @S3_WCD_MIDTERM_LOADING_STAGE;
