/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 20_failover_groups_primary.sql
 * Purpose: Configure failover groups on the PRIMARY Snowflake account
 *
 * This script sets up Business Continuity and Disaster Recovery (BCDR) by:
 * 1. Creating a failover group for ACCOUNT objects (integrations, roles)
 * 2. Creating a failover group for DATABASE objects (External Tables DB only)
 *
 * IMPORTANT: Catalog Linked Databases (CLD) CANNOT be included in failover groups!
 * CLDs must be created independently on each account, pointing to the same
 * Glue catalog. See script 30 for secondary CLD setup.
 *
 * Failover Groups enable:
 * - Continuous replication to secondary account(s)
 * - Seamless failover in disaster scenarios
 * - Minimal RPO (Recovery Point Objective)
 *
 * Prerequisites:
 *   - Snowflake Enterprise Edition or higher
 *   - Two accounts in the same Snowflake organization
 *   - ACCOUNTADMIN role on primary account
 *   - All demo objects created (scripts 00, 01, 10, 11)
 *
 * Run as: ACCOUNTADMIN (on PRIMARY account)
 ******************************************************************************/

-- ============================================================================
-- CONFIGURATION
-- ============================================================================
/*
 * BEFORE RUNNING: Replace these placeholders with your actual values
 *
 * SECONDARY_ACCOUNT_FQN = '<YOUR_ORG>.<YOUR_SECONDARY_ACCOUNT>'
 *
 * Example: sfsenorthamerica.ozc55031
 *
 * Find your account identifiers:
 *   - Run: SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();
 *   - Or check: SHOW ORGANIZATION ACCOUNTS;
 *
 * NOTE: SQL variables ($var) cannot be used in DDL statements like 
 *       CREATE FAILOVER GROUP. You must use literal account names.
 */

-- Reference values (for verification only, cannot be used in DDL)
-- Use ACCOUNT NAME, not account locator!
SET org_name = 'SFSENORTHAMERICA';
SET primary_account_name = 'SNOW_BCDR_PRIMARY';      -- Primary account NAME (this account)
SET secondary_account_name = 'SNOW_BCDR_SECONDARY';  -- Secondary account NAME

-- ============================================================================
-- SECTION 1: Verify Organization Setup
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Show organization details
SHOW ORGANIZATION ACCOUNTS;

-- Verify current account
SELECT CURRENT_ACCOUNT(), CURRENT_ORGANIZATION_NAME();

-- Verify we're on the PRIMARY account
-- The account locator should match $primary_account

-- ============================================================================
-- SECTION 2: Verify Objects to Replicate
-- ============================================================================

-- Check roles exist
SHOW ROLES LIKE 'ICEBERG%';

-- Check integrations exist
SHOW INTEGRATIONS;
SHOW CATALOG INTEGRATIONS;

-- Check external volumes exist
SHOW EXTERNAL VOLUMES;

-- Check databases exist
SHOW DATABASES LIKE 'ICEBERG_DEMO%';

-- ============================================================================
-- SECTION 3: Create Account Object Failover Group
-- ============================================================================

/*
 * Account Object Failover Group includes:
 * - Roles and role grants
 * - Storage Integrations (for S3 access)
 * - Warehouses (optional)
 * - Network policies (optional)
 * - Resource monitors (optional)
 *
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  NOTE: CATALOG INTEGRATIONS CANNOT BE REPLICATED!                         ║
 * ║                                                                           ║
 * ║  Like CLDs, catalog integrations must be created independently on each   ║
 * ║  account. See script 30 for secondary account setup.                     ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 */

-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  REPLACE with YOUR org and secondary ACCOUNT NAME (not locator!)           │
-- │  Format: <ORG_NAME>.<ACCOUNT_NAME>                                          │
-- │  Find with: SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();    │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE OR REPLACE FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG
    OBJECT_TYPES = ROLES, INTEGRATIONS
    ALLOWED_INTEGRATION_TYPES = STORAGE INTEGRATIONS
    ALLOWED_ACCOUNTS = SFSENORTHAMERICA.SNOW_BCDR_SECONDARY
    REPLICATION_SCHEDULE = '10 MINUTE'
    COMMENT = 'Failover group for Iceberg BCDR - Account objects (roles, storage integrations)';

-- Verify failover group creation
SHOW FAILOVER GROUPS;

-- Show details
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG;

-- ============================================================================
-- SECTION 4: Create External Volume Failover Group
-- ============================================================================

/*
 * External Volumes are replicated separately.
 * This ensures secondary has access to the same S3 locations.
 */

-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  REPLACE with YOUR org and secondary ACCOUNT NAME (not locator!)           │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE OR REPLACE FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG
    OBJECT_TYPES = EXTERNAL VOLUMES
    ALLOWED_ACCOUNTS = SFSENORTHAMERICA.SNOW_BCDR_SECONDARY
    REPLICATION_SCHEDULE = '10 MINUTE'
    COMMENT = 'Failover group for Iceberg BCDR - External Volumes';

-- Verify
SHOW FAILOVER GROUPS;
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG;

-- ============================================================================
-- SECTION 5: Create Database Failover Group (External Tables ONLY)
-- ============================================================================

/*
 * Database Failover Group includes:
 * - Database objects (schemas, tables, views)
 *
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  IMPORTANT: CLD CANNOT BE INCLUDED IN FAILOVER GROUPS!                    ║
 * ║                                                                           ║
 * ║  Catalog Linked Databases must be created independently on each account. ║
 * ║  Both accounts will point to the same Glue catalog, ensuring data        ║
 * ║  consistency without replication.                                         ║
 * ║                                                                           ║
 * ║  Only ICEBERG_DEMO_EXT (External Tables) is replicated here.             ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 */

-- Only replicate the External Tables database, NOT the CLD
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  REPLACE with YOUR org and secondary ACCOUNT NAME (not locator!)           │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE OR REPLACE FAILOVER GROUP ICEBERG_BCDR_DB_FG
    OBJECT_TYPES = DATABASES
    ALLOWED_DATABASES = ICEBERG_DEMO_EXT
    ALLOWED_ACCOUNTS = SFSENORTHAMERICA.SNOW_BCDR_SECONDARY
    REPLICATION_SCHEDULE = '10 MINUTE'
    COMMENT = 'Failover group for Iceberg BCDR - External Tables database only (CLD not supported)';

-- Verify failover group creation
SHOW FAILOVER GROUPS;

-- Show details
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_DB_FG;

-- ============================================================================
-- SECTION 6: Add Warehouses (Optional)
-- ============================================================================

/*
 * You can also replicate warehouse configurations.
 * This is useful if you want consistent warehouse settings on secondary.
 */

-- Uncomment to add warehouses to account failover group
-- ALTER FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG
--     SET OBJECT_TYPES = ROLES, INTEGRATIONS, WAREHOUSES;

-- ============================================================================
-- SECTION 7: Trigger Initial Replication
-- ============================================================================

/*
 * Trigger an immediate refresh to start replication.
 * This syncs current state to secondary before scheduled replication begins.
 */

-- Refresh all failover groups
ALTER FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG REFRESH;
ALTER FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG REFRESH;
ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG REFRESH;

-- ============================================================================
-- SECTION 8: Monitor Replication Status
-- ============================================================================

-- Check failover group status
SHOW FAILOVER GROUPS;

-- Describe each failover group for detailed info
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG;
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG;
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_DB_FG;

-- Check replication history from Account Usage (may have ~2hr latency)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
ORDER BY START_TIME DESC
LIMIT 20;

-- ============================================================================
-- SECTION 9: View Replicated Objects
-- ============================================================================

-- Show what's in each failover group
SHOW DATABASES IN FAILOVER GROUP ICEBERG_BCDR_DB_FG;
SHOW INTEGRATIONS IN FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG;
SHOW ROLES IN FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG;
SHOW EXTERNAL VOLUMES IN FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG;

-- ============================================================================
-- SECTION 10: Failover Group Status Queries
-- ============================================================================

-- Overall status of all failover groups (simple approach)
SHOW FAILOVER GROUPS;

-- Detailed status with last refresh time (Account Usage - may have latency)
SELECT 
    REPLICATION_GROUP_NAME,
    START_TIME,
    END_TIME,
    CREDITS_USED,
    BYTES_TRANSFERRED
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
ORDER BY START_TIME DESC
LIMIT 10;

/*******************************************************************************
 * FAILOVER GROUP SUMMARY:
 *
 * After running this script, you have created:
 *
 * 1. ICEBERG_BCDR_ACCOUNT_FG
 *    - Roles: ICEBERG_ADMIN, ICEBERG_ENGINEER, ICEBERG_ANALYST
 *    - Storage Integrations: AWS_ICEBERG_STORAGE_INT
 *    - NOTE: Catalog integrations CANNOT be replicated!
 *
 * 2. ICEBERG_BCDR_VOLUME_FG
 *    - External Volumes: ICEBERG_EXT_VOLUME
 *
 * 3. ICEBERG_BCDR_DB_FG
 *    - Databases: ICEBERG_DEMO_EXT (External Tables only)
 *    - NOTE: ICEBERG_DEMO_CLD CANNOT be replicated
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  WHAT CANNOT BE REPLICATED (must create on each account):              │
 * │                                                                         │
 * │  ✗ Catalog Integrations (REST_GLUE_CATALOG_INT)                        │
 * │  ✗ Catalog Linked Databases (ICEBERG_DEMO_CLD)                         │
 * │                                                                         │
 * │  Both accounts must create their own catalog integration and CLD       │
 * │  that point to the SAME Glue catalog. See script 30.                   │
 * │                                                                         │
 * │  PRIMARY ACCOUNT              SECONDARY ACCOUNT                        │
 * │  ┌──────────────────┐        ┌──────────────────┐                     │
 * │  │ ICEBERG_DEMO_CLD │        │ ICEBERG_DEMO_CLD │                     │
 * │  │ REST_GLUE_CAT_INT│        │ REST_GLUE_CAT_INT│                     │
 * │  └────────┬─────────┘        └────────┬─────────┘                     │
 * │           │                           │                                │
 * │           └───────────┬───────────────┘                                │
 * │                       ▼                                                │
 * │              ┌────────────────┐                                        │
 * │              │  AWS GLUE      │  ← Same catalog, same data            │
 * │              │  CATALOG       │                                        │
 * │              └────────────────┘                                        │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * NEXT STEPS:
 * 1. Run 21_failover_groups_secondary.sql on SECONDARY account
 * 2. Run 30_cld_secondary_setup.sql to create catalog integration AND CLD
 * 3. Update AWS IAM trust policy with secondary account's IAM user ARNs
 * 4. Run 32_migrate_prod_db_independent.sql to create independent ICEBERG_PROD
 * 5. Run 33_schema_sync_task.sql to set up daily schema drift detection
 * 6. Run 31_sync_task_secondary.sql to set up heartbeat validation task
 * 7. Verify replication is working
 * 8. Test failover procedure
 ******************************************************************************/
