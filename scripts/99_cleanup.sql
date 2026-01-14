/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 99_cleanup.sql
 * Purpose: Remove all demo objects from the Snowflake account
 *
 * WARNING: This script will permanently delete:
 * - Databases (ICEBERG_DEMO_EXT, ICEBERG_DEMO_CLD, ICEBERG_PROD)
 * - Roles (ICEBERG_ADMIN, ICEBERG_ENGINEER, ICEBERG_ANALYST)
 * - Integrations (AWS_ICEBERG_STORAGE_INT, REST_GLUE_CATALOG_INT)
 * - External volumes (ICEBERG_EXT_VOLUME)
 * - Failover groups (ICEBERG_BCDR_ACCOUNT_FG, ICEBERG_BCDR_VOLUME_FG, ICEBERG_BCDR_DB_FG)
 * - Warehouses (ICEBERG_DEMO_WH, TASK_WH)
 *
 * Note: This does NOT delete data from AWS S3 or Glue catalog.
 *
 * Run as: ACCOUNTADMIN
 ******************************************************************************/

-- ============================================================================
-- CONFIRMATION
-- ============================================================================

/*
 * BEFORE RUNNING THIS SCRIPT:
 * 1. Ensure you want to remove ALL demo objects
 * 2. Back up any data or configurations you want to keep
 * 3. If using failover, run cleanup on SECONDARY account first
 *
 * To proceed, uncomment the sections below.
 */

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- SECTION 1: Drop Failover Groups
-- ============================================================================

-- Must drop failover groups before dropping objects they contain

-- Drop database failover group
DROP FAILOVER GROUP IF EXISTS ICEBERG_BCDR_DB_FG;

-- Drop external volume failover group
DROP FAILOVER GROUP IF EXISTS ICEBERG_BCDR_VOLUME_FG;

-- Drop account failover group  
DROP FAILOVER GROUP IF EXISTS ICEBERG_BCDR_ACCOUNT_FG;

-- Verify
SHOW FAILOVER GROUPS LIKE 'ICEBERG_BCDR%';

-- ============================================================================
-- SECTION 2: Drop Databases
-- ============================================================================

-- Drop Production database (INDEPENDENT on both accounts)
DROP DATABASE IF EXISTS ICEBERG_PROD;

-- Drop External Tables database
DROP DATABASE IF EXISTS ICEBERG_DEMO_EXT;

-- Drop Catalog Linked Database
DROP DATABASE IF EXISTS ICEBERG_DEMO_CLD;

-- Drop secondary CLD if it exists (legacy naming)
DROP DATABASE IF EXISTS ICEBERG_DEMO_CLD_SECONDARY;

-- Verify
SHOW DATABASES LIKE 'ICEBERG%';

-- ============================================================================
-- SECTION 3: Drop External Volume
-- ============================================================================

-- Drop external volume
DROP EXTERNAL VOLUME IF EXISTS ICEBERG_EXT_VOLUME;

-- Drop secondary external volume if it exists
DROP EXTERNAL VOLUME IF EXISTS ICEBERG_EXT_VOLUME_SECONDARY;

-- Verify
SHOW EXTERNAL VOLUMES LIKE 'ICEBERG%';

-- ============================================================================
-- SECTION 4: Drop Integrations
-- ============================================================================

-- Drop storage integration
DROP INTEGRATION IF EXISTS AWS_ICEBERG_STORAGE_INT;
DROP INTEGRATION IF EXISTS ICEBERG_S3_INT;

-- Drop catalog integrations
DROP INTEGRATION IF EXISTS REST_GLUE_CATALOG_INT;
DROP INTEGRATION IF EXISTS GLUE_CATALOG_INT;

-- Drop secondary integrations if they exist (legacy naming)
DROP INTEGRATION IF EXISTS AWS_ICEBERG_STORAGE_INT_SECONDARY;
DROP INTEGRATION IF EXISTS GLUE_CATALOG_INT_SECONDARY;

-- Verify
SHOW INTEGRATIONS LIKE '%ICEBERG%';
SHOW INTEGRATIONS LIKE '%GLUE%';
SHOW CATALOG INTEGRATIONS;

-- ============================================================================
-- SECTION 5: Drop Warehouses
-- ============================================================================

-- Drop primary warehouse
DROP WAREHOUSE IF EXISTS ICEBERG_DEMO_WH;

-- Drop task warehouse
DROP WAREHOUSE IF EXISTS TASK_WH;

-- Drop secondary warehouse if it exists
DROP WAREHOUSE IF EXISTS ICEBERG_DEMO_WH_SECONDARY;

-- Verify
SHOW WAREHOUSES LIKE 'ICEBERG%';
SHOW WAREHOUSES LIKE 'TASK%';

-- ============================================================================
-- SECTION 6: Drop Roles
-- ============================================================================

/*
 * Roles must be dropped in reverse hierarchy order.
 * First revoke any grants, then drop.
 */

-- Revoke role grants
REVOKE ROLE ICEBERG_ANALYST FROM ROLE ICEBERG_ENGINEER;
REVOKE ROLE ICEBERG_ENGINEER FROM ROLE ICEBERG_ADMIN;
REVOKE ROLE ICEBERG_ADMIN FROM ROLE ACCOUNTADMIN;

-- Drop roles (order matters - children first)
DROP ROLE IF EXISTS ICEBERG_ANALYST;
DROP ROLE IF EXISTS ICEBERG_ENGINEER;
DROP ROLE IF EXISTS ICEBERG_ADMIN;

-- Verify
SHOW ROLES LIKE 'ICEBERG%';

-- ============================================================================
-- SECTION 7: Verification
-- ============================================================================

-- Final verification - all should return empty
SELECT 'Checking for remaining objects...' AS status;

SHOW ROLES LIKE 'ICEBERG%';
SHOW WAREHOUSES LIKE 'ICEBERG%';
SHOW DATABASES LIKE 'ICEBERG%';
SHOW INTEGRATIONS LIKE '%ICEBERG%';
SHOW INTEGRATIONS LIKE '%GLUE%';
SHOW EXTERNAL VOLUMES LIKE 'ICEBERG%';
SHOW FAILOVER GROUPS LIKE 'ICEBERG%';

-- ============================================================================
-- SECTION 8: AWS Cleanup (Manual Steps)
-- ============================================================================

/*
 * This script does NOT clean up AWS resources. To fully clean up:
 *
 * 1. Delete S3 data:
 *    aws s3 rm s3://your-bucket/iceberg/ --recursive
 *
 * 2. Delete Glue database:
 *    aws glue delete-database --name iceberg_advertising_db
 *
 * 3. Delete IAM role (if no longer needed):
 *    aws iam delete-role --role-name snowflake-iceberg-role
 *
 * 4. Remove IAM policies attached to the role first:
 *    aws iam list-attached-role-policies --role-name snowflake-iceberg-role
 *    aws iam detach-role-policy --role-name snowflake-iceberg-role --policy-arn <arn>
 */

-- ============================================================================
-- SECTION 9: Secondary Account Cleanup
-- ============================================================================

/*
 * If you have a secondary account configured, run these commands there:
 *
 * USE ROLE ACCOUNTADMIN;
 * 
 * -- Drop replica failover groups
 * DROP FAILOVER GROUP IF EXISTS ICEBERG_BCDR_DB_FG;
 * DROP FAILOVER GROUP IF EXISTS ICEBERG_BCDR_VOLUME_FG;
 * DROP FAILOVER GROUP IF EXISTS ICEBERG_BCDR_ACCOUNT_FG;
 * 
 * -- Drop INDEPENDENT databases (not replicated)
 * DROP DATABASE IF EXISTS ICEBERG_PROD;
 * DROP DATABASE IF EXISTS ICEBERG_DEMO_CLD;
 * 
 * -- Drop secondary warehouses
 * DROP WAREHOUSE IF EXISTS ICEBERG_DEMO_WH;
 * DROP WAREHOUSE IF EXISTS TASK_WH;
 * 
 * -- Drop catalog integration (not replicated)
 * DROP INTEGRATION IF EXISTS REST_GLUE_CATALOG_INT;
 * 
 * -- Note: ICEBERG_DEMO_EXT, roles, and storage integrations are automatically
 * -- removed when the primary failover group is dropped.
 */

/*******************************************************************************
 * CLEANUP COMPLETE
 *
 * All Snowflake demo objects have been removed.
 * 
 * AWS resources must be cleaned up separately using AWS CLI or Console:
 * - S3 bucket data
 * - Glue database and tables
 * - IAM roles and policies
 *
 * To recreate the demo, start from script 00_prereqs_rbac.sql
 ******************************************************************************/

