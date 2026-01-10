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
-- CONFIGURATION VARIABLES
-- Update these values for your environment
-- ============================================================================

SET org_name = '<YOUR_ORG_NAME>';                       -- Your Snowflake organization name
SET primary_account = '<YOUR_PRIMARY_ACCOUNT>';         -- Primary account locator
SET secondary_account = '<YOUR_SECONDARY_ACCOUNT>';     -- Secondary account locator

-- Fully qualified account names
SET primary_fqn = $org_name || '.' || $primary_account;
SET secondary_fqn = $org_name || '.' || $secondary_account;

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
 * - Integrations (storage, catalog)
 * - Warehouses (optional)
 * - Network policies (optional)
 * - Resource monitors (optional)
 *
 * For Iceberg BCDR, we replicate integrations so secondary can
 * access the same Glue catalog and S3 storage.
 *
 * NOTE: Even though we replicate the REST_GLUE_CATALOG_INT, the secondary
 * account still needs to create its own CLD using this integration.
 */

CREATE OR REPLACE FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG
    OBJECT_TYPES = ROLES, INTEGRATIONS
    ALLOWED_ACCOUNTS = $secondary_fqn
    REPLICATION_SCHEDULE = '10 MINUTE'
    COMMENT = 'Failover group for Iceberg BCDR - Account objects (roles, integrations)';

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

CREATE OR REPLACE FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG
    OBJECT_TYPES = EXTERNAL VOLUMES
    ALLOWED_ACCOUNTS = $secondary_fqn
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
CREATE OR REPLACE FAILOVER GROUP ICEBERG_BCDR_DB_FG
    OBJECT_TYPES = DATABASES
    ALLOWED_DATABASES = ICEBERG_DEMO_EXT
    ALLOWED_ACCOUNTS = $secondary_fqn
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

-- Check replication progress for account objects
SELECT * FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS('ICEBERG_BCDR_ACCOUNT_FG'));

-- Check replication progress for external volumes
SELECT * FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS('ICEBERG_BCDR_VOLUME_FG'));

-- Check replication progress for databases
SELECT * FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS('ICEBERG_BCDR_DB_FG'));

-- View replication history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
ORDER BY PHASE_START_TIME DESC
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

-- Overall status of all failover groups
SELECT 
    REPLICATION_GROUP_NAME,
    CREATED_ON,
    ACCOUNT_NAME,
    IS_PRIMARY,
    OBJECT_TYPES,
    REPLICATION_ALLOWED_TO_ACCOUNTS
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUPS())
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%';

-- Lag monitoring - time since last successful sync
SELECT 
    REPLICATION_GROUP_NAME,
    PHASE,
    PHASE_START_TIME,
    PHASE_END_TIME,
    DATEDIFF('second', PHASE_START_TIME, COALESCE(PHASE_END_TIME, CURRENT_TIMESTAMP())) AS PHASE_DURATION_SECONDS
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
    AND PHASE = 'COMPLETED'
ORDER BY PHASE_END_TIME DESC
LIMIT 10;

/*******************************************************************************
 * FAILOVER GROUP SUMMARY:
 *
 * After running this script, you have created:
 *
 * 1. ICEBERG_BCDR_ACCOUNT_FG
 *    - Roles: ICEBERG_ADMIN, ICEBERG_ENGINEER, ICEBERG_ANALYST
 *    - Integrations: AWS_ICEBERG_STORAGE_INT, REST_GLUE_CATALOG_INT
 *
 * 2. ICEBERG_BCDR_VOLUME_FG
 *    - External Volumes: ICEBERG_EXT_VOLUME
 *
 * 3. ICEBERG_BCDR_DB_FG
 *    - Databases: ICEBERG_DEMO_EXT (External Tables only)
 *    - NOTE: ICEBERG_DEMO_CLD is NOT included (CLDs cannot be replicated)
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  CLD REPLICATION STRATEGY:                                              │
 * │                                                                         │
 * │  Since CLDs cannot be in failover groups, each account creates its     │
 * │  own CLD that points to the SAME Glue catalog:                         │
 * │                                                                         │
 * │  PRIMARY ACCOUNT              SECONDARY ACCOUNT                        │
 * │  ┌──────────────────┐        ┌──────────────────┐                     │
 * │  │ ICEBERG_DEMO_CLD │        │ ICEBERG_DEMO_CLD │                     │
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
 * 2. Run 30_cld_secondary_setup.sql to create CLD on secondary
 * 3. Verify replication is working
 * 4. Test failover procedure
 ******************************************************************************/
