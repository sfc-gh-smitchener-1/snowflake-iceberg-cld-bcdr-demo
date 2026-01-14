/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 21_failover_groups_secondary.sql
 * Purpose: Configure the SECONDARY Snowflake account to receive replication
 *
 * This script sets up the secondary account to:
 * 1. Accept replication from the primary account's failover groups
 * 2. Create secondary (read-only) copies of replicated objects
 * 3. Enable failover capability
 *
 * IMPORTANT: This script does NOT set up the CLD!
 * CLDs cannot be replicated via failover groups. After running this script,
 * you MUST run 30_cld_secondary_setup.sql to create the CLD on secondary.
 *
 * Prerequisites:
 *   - Primary account has failover groups configured (script 20)
 *   - ACCOUNTADMIN role on secondary account
 *   - Accounts are in the same Snowflake organization
 *
 * Run as: ACCOUNTADMIN (on SECONDARY account)
 ******************************************************************************/

-- ============================================================================
-- CONFIGURATION
-- ============================================================================
/*
 * BEFORE RUNNING: Update these account identifiers for your environment
 *
 * PRIMARY_ACCOUNT_FQN = '<YOUR_ORG>.<YOUR_PRIMARY_ACCOUNT>'
 *
 * Example: MYORGNAME.MY_PRIMARY_ACCOUNT
 *
 * Find your account identifiers:
 *   - Run: SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();
 *   - Or check: SHOW ORGANIZATION ACCOUNTS;
 *
 * NOTE: SQL variables ($var) cannot be used in DDL statements.
 *       You must use literal account names in CREATE FAILOVER GROUP statements.
 */

-- Reference values (for verification only)
-- Use ACCOUNT NAME, not account locator!
SET org_name = '<YOUR_ORG_NAME>';                    -- e.g., 'MYORGNAME'
SET primary_account_name = '<YOUR_PRIMARY_ACCOUNT>'; -- Primary account NAME
SET secondary_account_name = '<YOUR_SECONDARY_ACCOUNT>'; -- Secondary account NAME (this account)

-- ============================================================================
-- SECTION 1: Verify Current Account
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Confirm we're on the secondary account
SELECT CURRENT_ACCOUNT(), CURRENT_ORGANIZATION_NAME();

-- Should match $secondary_account
-- If not, connect to the correct account before proceeding

-- ============================================================================
-- SECTION 2: Create Secondary Failover Group for Account Objects
-- ============================================================================

/*
 * Create the secondary failover group that mirrors the primary's account objects.
 * This creates replicas of roles and integrations on this account.
 */

-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  REPLACE with YOUR org and primary ACCOUNT NAME (not locator!)             │
-- │  Format: <ORG_NAME>.<ACCOUNT_NAME>.FAILOVER_GROUP_NAME                     │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE FAILOVER GROUP IF NOT EXISTS ICEBERG_BCDR_ACCOUNT_FG
    AS REPLICA OF <YOUR_ORG_NAME>.<YOUR_PRIMARY_ACCOUNT>.ICEBERG_BCDR_ACCOUNT_FG;
    -- e.g., AS REPLICA OF MYORGNAME.MY_PRIMARY_ACCOUNT.ICEBERG_BCDR_ACCOUNT_FG;

-- Verify creation
SHOW FAILOVER GROUPS;

-- ============================================================================
-- SECTION 3: Create Secondary Failover Group for External Volumes
-- ============================================================================

/*
 * Create the secondary failover group for external volumes.
 */

-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  REPLACE with YOUR org and primary ACCOUNT NAME (not locator!)             │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE FAILOVER GROUP IF NOT EXISTS ICEBERG_BCDR_VOLUME_FG
    AS REPLICA OF <YOUR_ORG_NAME>.<YOUR_PRIMARY_ACCOUNT>.ICEBERG_BCDR_VOLUME_FG;
    -- e.g., AS REPLICA OF MYORGNAME.MY_PRIMARY_ACCOUNT.ICEBERG_BCDR_VOLUME_FG;

-- Verify creation
SHOW FAILOVER GROUPS;

-- ============================================================================
-- SECTION 4: Create Secondary Failover Group for Databases
-- ============================================================================

/*
 * Create the secondary failover group that mirrors the primary's databases.
 * This ONLY includes ICEBERG_DEMO_EXT (External Tables).
 *
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  REMINDER: CLD IS NOT REPLICATED!                                         ║
 * ║                                                                           ║
 * ║  After this script completes, run 30_cld_secondary_setup.sql to create   ║
 * ║  the Catalog Linked Database on this secondary account.                  ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 */

-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  REPLACE with YOUR org and primary ACCOUNT NAME (not locator!)             │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE FAILOVER GROUP IF NOT EXISTS ICEBERG_BCDR_DB_FG
    AS REPLICA OF <YOUR_ORG_NAME>.<YOUR_PRIMARY_ACCOUNT>.ICEBERG_BCDR_DB_FG;
    -- e.g., AS REPLICA OF MYORGNAME.MY_PRIMARY_ACCOUNT.ICEBERG_BCDR_DB_FG;

-- Verify all failover groups
SHOW FAILOVER GROUPS;

-- ============================================================================
-- SECTION 5: Initial Refresh
-- ============================================================================

/*
 * Trigger an immediate refresh to sync data from primary.
 * This may take some time depending on the amount of metadata.
 * 
 * Order matters: Account objects first, then volumes, then databases.
 */

-- Refresh account objects first (roles, integrations)
ALTER FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG REFRESH;

-- Wait a moment, then refresh external volumes
ALTER FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG REFRESH;

-- Finally refresh databases (they depend on integrations and volumes)
ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG REFRESH;

-- ============================================================================
-- SECTION 6: Verify Replicated Objects
-- ============================================================================

-- Wait for refresh to complete, then verify objects

-- Show replicated databases (should show ICEBERG_DEMO_EXT only, NOT CLD)
SHOW DATABASES LIKE 'ICEBERG_DEMO%';

-- Show replicated integrations
SHOW INTEGRATIONS;
SHOW CATALOG INTEGRATIONS;

-- Show replicated roles
SHOW ROLES LIKE 'ICEBERG%';

-- Show replicated external volumes
SHOW EXTERNAL VOLUMES;

-- ============================================================================
-- SECTION 7: Check Replication Status
-- ============================================================================

-- View all failover groups
SHOW FAILOVER GROUPS;

-- View failover group details
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG;
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG;
DESCRIBE FAILOVER GROUP ICEBERG_BCDR_DB_FG;

-- View replication history (Account Usage - may have ~2hr latency)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_REFRESH_HISTORY
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
ORDER BY START_TIME DESC
LIMIT 20;

-- ============================================================================
-- SECTION 8: Create Warehouse on Secondary
-- ============================================================================

/*
 * Warehouses are not replicated by default.
 * Create a warehouse on secondary to enable querying.
 */

CREATE WAREHOUSE IF NOT EXISTS ICEBERG_DEMO_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Iceberg demo on secondary account';

-- Grant usage to replicated roles
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE ICEBERG_ANALYST;
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 9: Verify External Tables Access
-- ============================================================================

/*
 * Test that replicated External Tables database is accessible.
 * Note: Secondary databases are READ-ONLY until failover.
 */

-- Switch to analyst role
USE ROLE ICEBERG_ANALYST;
USE WAREHOUSE ICEBERG_DEMO_WH;

-- Test External Tables database (should work - it was replicated)
USE DATABASE ICEBERG_DEMO_EXT;
SHOW SCHEMAS;
SELECT COUNT(*) AS campaign_count FROM ADVERTISING.EXT_CAMPAIGNS;

-- ============================================================================
-- SECTION 10: CLD Status Check
-- ============================================================================

USE ROLE ACCOUNTADMIN;

/*
 * The CLD should NOT exist yet on secondary.
 * It must be created separately using script 30.
 */

-- This should show NO results or fail (CLD doesn't exist yet)
SHOW DATABASES LIKE 'ICEBERG_DEMO_CLD';

-- ============================================================================
-- SECTION 11: Failover Procedure (Execute Only During Actual Failover)
-- ============================================================================

/*
 * !!! CAUTION: Only execute this section during an actual failover event !!!
 * 
 * Promoting the secondary to primary will:
 * 1. Make this account the source of truth
 * 2. Allow writes to the replicated databases
 * 3. Require reconfiguration of the original primary
 *
 * NOTE: The CLD doesn't need to be "promoted" - it already points to
 * the same Glue catalog and works independently.
 */

-- FAILOVER COMMAND (commented out for safety)
-- Execute these in order:
/*
-- Step 1: Promote account objects (roles, integrations)
ALTER FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG PRIMARY;

-- Step 2: Promote external volumes
ALTER FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG PRIMARY;

-- Step 3: Promote databases (External Tables only)
ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG PRIMARY;

-- Step 4: CLD already works - no action needed!
-- The CLD on secondary is already pointing to the same Glue catalog.
*/

-- ============================================================================
-- SECTION 12: Post-Failover Validation
-- ============================================================================

/*
 * After failover, verify that:
 * 1. Failover groups show IS_PRIMARY = true
 * 2. External Tables database is writable
 * 3. CLD continues to work (it should - same Glue catalog)
 */

-- Check that we are now primary
SHOW FAILOVER GROUPS;
-- Look for IS_PRIMARY = true in the results

-- ============================================================================
-- SECTION 13: Failback Procedure (Return to Normal Operations)
-- ============================================================================

/*
 * After the original primary account is recovered, you can failback:
 * 
 * ON ORIGINAL PRIMARY (now secondary):
 * 1. Create replica failover groups pointing to new primary
 * 2. Wait for sync to complete
 * 3. Promote back to primary
 *
 * NOTE: CLDs on both accounts continue to work throughout - no action needed.
 */

-- FAILBACK COMMANDS (run on original primary account):
/*
-- Create replicas pointing to this account (which is now primary)
CREATE FAILOVER GROUP IF NOT EXISTS ICEBERG_BCDR_ACCOUNT_FG
    AS REPLICA OF <new_primary_fqn>.ICEBERG_BCDR_ACCOUNT_FG;

CREATE FAILOVER GROUP IF NOT EXISTS ICEBERG_BCDR_VOLUME_FG
    AS REPLICA OF <new_primary_fqn>.ICEBERG_BCDR_VOLUME_FG;

CREATE FAILOVER GROUP IF NOT EXISTS ICEBERG_BCDR_DB_FG
    AS REPLICA OF <new_primary_fqn>.ICEBERG_BCDR_DB_FG;

-- Trigger refresh
ALTER FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG REFRESH;
ALTER FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG REFRESH;
ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG REFRESH;

-- After sync completes, promote back to primary:
ALTER FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG PRIMARY;
ALTER FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG PRIMARY;
ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG PRIMARY;
*/

/*******************************************************************************
 * SECONDARY ACCOUNT STATUS:
 *
 * After running this script, the secondary account will have:
 * ✓ Replica failover groups configured
 * ✓ Continuous replication from primary
 * ✓ Replicated roles and integrations
 * ✓ Replicated external volumes
 * ✓ Replicated ICEBERG_DEMO_EXT database (External Tables)
 * ✓ Local warehouse for querying
 * ✗ CLD NOT YET CREATED - Run script 30 next!
 *
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  NEXT STEP REQUIRED:                                                      ║
 * ║                                                                           ║
 * ║  Run 30_cld_secondary_setup.sql to create the CLD on this account.       ║
 * ║  The CLD will use the replicated catalog integration to connect to       ║
 * ║  the same Glue catalog as primary.                                       ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 *
 * BCDR BENEFITS FOR ICEBERG:
 *
 * 1. Fast Failover: Since Iceberg data lives in S3/Glue, only metadata
 *    needs to be replicated. Actual data is already accessible.
 *
 * 2. CLD Independence: Both accounts have their own CLD pointing to the
 *    same Glue catalog. No CLD replication needed or possible!
 *
 * 3. Shared Storage: Both accounts read from the same S3 bucket.
 *    No data duplication needed.
 *
 * 4. Zero-Copy Failover: After promoting failover groups, applications
 *    can immediately query the same data via either External Tables or CLD.
 *
 * NEXT STEPS:
 * 1. Run 30_cld_secondary_setup.sql to CREATE CLD on this account
 * 2. Test the failover procedure in a non-production environment
 * 3. Document the failover runbook for operations team
 ******************************************************************************/
