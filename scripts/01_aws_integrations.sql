/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 01_aws_integrations.sql
 * Purpose: Create AWS storage integration, Glue REST catalog integration, and 
 *          external volume for Iceberg table access via Catalog Linked Database
 *
 * Prerequisites:
 *   - AWS S3 bucket created
 *   - AWS Glue database with Iceberg tables created
 *   - IAM role created with trust policy for Snowflake
 *   - Lake Formation configured (see LAKE_FORMATION_SETUP section below)
 *
 * Run as: ACCOUNTADMIN
 ******************************************************************************/

-- ============================================================================
-- CONFIGURATION VARIABLES
-- Update these values to match your AWS environment
-- ============================================================================

SET aws_account_id = '<YOUR_AWS_ACCOUNT_ID>';                 -- e.g., '123456789012'
SET aws_region = '<YOUR_AWS_REGION>';                          -- e.g., 'us-west-2'
SET s3_bucket_name = '<YOUR_S3_BUCKET>';                       -- e.g., 'my-iceberg-bucket'
SET glue_database_name = 'iceberg_advertising_db';             -- Glue database name
SET iam_role_arn = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/<YOUR_ROLE_NAME>';

-- Derived paths
SET s3_base_path = 's3://' || $s3_bucket_name || '/iceberg/warehouse/';

-- ============================================================================
-- SECTION 1: Create Storage Integration
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION AWS_ICEBERG_STORAGE_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = $iam_role_arn
    STORAGE_ALLOWED_LOCATIONS = ($s3_base_path)
    COMMENT = 'Storage integration for Iceberg data in S3';

-- IMPORTANT: Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- These must be added to the IAM role trust policy
DESCRIBE INTEGRATION AWS_ICEBERG_STORAGE_INT;

-- ============================================================================
-- SECTION 2: Create REST Catalog Integration for AWS Glue
-- This is required for Catalog Linked Database (CLD)
-- ============================================================================

CREATE OR REPLACE CATALOG INTEGRATION REST_GLUE_CATALOG_INT
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = $glue_database_name
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.' || $aws_region || '.amazonaws.com/iceberg'
    CATALOG_API_TYPE = AWS_GLUE
    CATALOG_NAME = $aws_account_id
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = $iam_role_arn
    SIGV4_SIGNING_REGION = $aws_region
  )
  ENABLED = TRUE
  COMMENT = 'REST catalog integration for AWS Glue with vended credentials';

-- IMPORTANT: Copy API_AWS_EXTERNAL_ID for trust policy
DESCRIBE CATALOG INTEGRATION REST_GLUE_CATALOG_INT;

-- ============================================================================
-- SECTION 3: Create External Volume
-- ============================================================================

CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_EXT_VOLUME
    STORAGE_LOCATIONS = (
        (
            NAME = 'aws_s3_iceberg'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = $s3_base_path
            STORAGE_AWS_ROLE_ARN = $iam_role_arn
        )
    )
    COMMENT = 'External volume for Iceberg table storage in S3';

-- IMPORTANT: Copy STORAGE_AWS_EXTERNAL_ID for trust policy
DESCRIBE EXTERNAL VOLUME ICEBERG_EXT_VOLUME;

-- ============================================================================
-- SECTION 4: Grant Privileges on Integrations
-- ============================================================================

GRANT USAGE ON INTEGRATION AWS_ICEBERG_STORAGE_INT TO ROLE ICEBERG_ADMIN;
GRANT USAGE ON INTEGRATION REST_GLUE_CATALOG_INT TO ROLE ICEBERG_ADMIN;
GRANT USAGE ON EXTERNAL VOLUME ICEBERG_EXT_VOLUME TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 5: Verification
-- ============================================================================

SHOW INTEGRATIONS;
SHOW CATALOG INTEGRATIONS;
SHOW EXTERNAL VOLUMES;

/*******************************************************************************
 * AWS IAM ROLE TRUST POLICY
 * 
 * The IAM role must trust:
 * 1. The Snowflake IAM user (from DESCRIBE INTEGRATION outputs)
 * 2. Lake Formation service (for credential vending)
 *
 * Example trust policy:
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {
 *       "Effect": "Allow",
 *       "Principal": {
 *         "AWS": "<STORAGE_AWS_IAM_USER_ARN from DESCRIBE commands>"
 *       },
 *       "Action": "sts:AssumeRole"
 *     },
 *     {
 *       "Effect": "Allow",
 *       "Principal": {
 *         "Service": "lakeformation.amazonaws.com"
 *       },
 *       "Action": "sts:AssumeRole"
 *     }
 *   ]
 * }
 *
 * NOTE: For production, add External ID conditions:
 * "Condition": {
 *   "StringEquals": {
 *     "sts:ExternalId": ["<external_id_1>", "<external_id_2>", ...]
 *   }
 * }
 ******************************************************************************/

/*******************************************************************************
 * AWS IAM ROLE PERMISSIONS POLICY
 * 
 * The IAM role needs these permissions:
 *
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {
 *       "Effect": "Allow",
 *       "Action": [
 *         "s3:GetObject",
 *         "s3:GetObjectVersion",
 *         "s3:PutObject",
 *         "s3:DeleteObject",
 *         "s3:ListBucket",
 *         "s3:GetBucketLocation"
 *       ],
 *       "Resource": [
 *         "arn:aws:s3:::<YOUR_BUCKET>",
 *         "arn:aws:s3:::<YOUR_BUCKET>/*"
 *       ]
 *     },
 *     {
 *       "Effect": "Allow",
 *       "Action": [
 *         "glue:GetDatabase",
 *         "glue:GetDatabases",
 *         "glue:GetTable",
 *         "glue:GetTables",
 *         "glue:GetTableVersion",
 *         "glue:GetTableVersions",
 *         "glue:GetPartitions"
 *       ],
 *       "Resource": [
 *         "arn:aws:glue:<REGION>:<ACCOUNT>:catalog",
 *         "arn:aws:glue:<REGION>:<ACCOUNT>:database/*",
 *         "arn:aws:glue:<REGION>:<ACCOUNT>:table/*/*"
 *       ]
 *     },
 *     {
 *       "Effect": "Allow",
 *       "Action": "lakeformation:GetDataAccess",
 *       "Resource": "*"
 *     }
 *   ]
 * }
 ******************************************************************************/

/*******************************************************************************
 * LAKE FORMATION SETUP (CRITICAL!)
 * 
 * For CLD with Glue REST API to work, Lake Formation must be configured:
 *
 * 1. REGISTER S3 LOCATION
 *    Lake Formation → Data lake locations → Register location
 *    - S3 path: s3://<your-bucket>/iceberg/warehouse/
 *    - IAM role: <your-iam-role>
 *
 * 2. ADD DATA LAKE ADMINISTRATOR
 *    Lake Formation → Administrative roles and tasks → Data lake administrators
 *    - Add your IAM role as administrator
 *
 * 3. GRANT DATA PERMISSIONS
 *    Lake Formation → Data permissions → Grant
 *    - Principal: Your IAM role
 *    - Database: iceberg_advertising_db
 *    - Tables: All tables
 *    - Permissions: Select, Describe
 *
 * 4. *** CRITICAL: ENABLE EXTERNAL ENGINE ACCESS ***
 *    Lake Formation → Application integration settings
 *    - Check: "Allow external engines to access data in Amazon S3 locations 
 *             with full table access"
 *    
 *    This setting allows third-party query engines (like Snowflake) to 
 *    access Iceberg tables via Lake Formation credential vending.
 *
 * 5. GRANT DATA LOCATION ACCESS
 *    aws lakeformation grant-permissions \
 *      --principal DataLakePrincipalIdentifier=<role-arn> \
 *      --resource '{"DataLocation":{"ResourceArn":"arn:aws:s3:::<bucket>/iceberg/warehouse"}}' \
 *      --permissions "DATA_LOCATION_ACCESS" \
 *      --region <region>
 ******************************************************************************/

/*******************************************************************************
 * NEXT STEPS:
 * 1. Configure AWS IAM role trust policy with values from DESCRIBE commands
 * 2. Configure AWS IAM role permissions policy
 * 3. Configure Lake Formation (especially Application integration settings!)
 * 4. Run 02_generate_iceberg_data.py to generate sample data
 * 5. Run 03_load_iceberg_aws.py or PyIceberg script to create Iceberg tables
 * 6. Run 11_catalog_linked_database.sql to create CLD
 ******************************************************************************/
