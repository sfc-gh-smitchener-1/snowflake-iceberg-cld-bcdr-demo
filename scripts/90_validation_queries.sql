/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 90_validation_queries.sql
 * Purpose: Comprehensive validation queries to verify the demo setup
 *
 * This script validates:
 * 1. Role hierarchy and permissions
 * 2. Integrations and external volumes
 * 3. External tables and CLD access
 * 4. Data consistency between approaches
 * 5. Failover group status
 * 6. Lake Formation connectivity (for CLD)
 *
 * IMPORTANT: This script can be run on EITHER primary or secondary account.
 * Some queries may fail on secondary if CLD hasn't been created yet (script 30).
 *
 * Run as: ACCOUNTADMIN (for full visibility), or individual roles to test access
 ******************************************************************************/

-- ============================================================================
-- SECTION 1: Identify Current Account
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Which account are we on?
SELECT 
    CURRENT_ACCOUNT() AS account_locator,
    CURRENT_ORGANIZATION_NAME() AS organization,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUPS()) 
            WHERE IS_PRIMARY = TRUE AND REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
        ) THEN 'PRIMARY'
        WHEN EXISTS (
            SELECT 1 FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUPS()) 
            WHERE IS_PRIMARY = FALSE AND REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
        ) THEN 'SECONDARY'
        ELSE 'UNKNOWN (no failover groups)'
    END AS account_role;

-- ============================================================================
-- SECTION 2: Role and Permission Validation
-- ============================================================================

-- Verify role hierarchy
SHOW ROLES LIKE 'ICEBERG%';

-- Check grants to each role
SHOW GRANTS TO ROLE ICEBERG_ADMIN;
SHOW GRANTS TO ROLE ICEBERG_ENGINEER;
SHOW GRANTS TO ROLE ICEBERG_ANALYST;

-- Role hierarchy verification
SHOW GRANTS OF ROLE ICEBERG_ADMIN;
SHOW GRANTS OF ROLE ICEBERG_ENGINEER;
SHOW GRANTS OF ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 3: Integration Validation
-- ============================================================================

-- List all integrations
SHOW INTEGRATIONS;
SHOW CATALOG INTEGRATIONS;

-- Storage integration details
DESCRIBE INTEGRATION AWS_ICEBERG_STORAGE_INT;

-- REST Catalog integration details (for CLD)
DESCRIBE CATALOG INTEGRATION REST_GLUE_CATALOG_INT;

-- External volume details
SHOW EXTERNAL VOLUMES;
DESCRIBE EXTERNAL VOLUME ICEBERG_EXT_VOLUME;

-- ============================================================================
-- SECTION 4: Database Validation
-- ============================================================================

-- List databases
SHOW DATABASES LIKE 'ICEBERG_DEMO%';

-- External Tables database (should exist on both primary and secondary)
SHOW SCHEMAS IN DATABASE ICEBERG_DEMO_EXT;
SHOW ICEBERG TABLES IN SCHEMA ICEBERG_DEMO_EXT.ADVERTISING;

-- CLD database (should exist if created - script 11 on primary, script 30 on secondary)
-- Note: This may fail on secondary if CLD hasn't been created yet
SHOW SCHEMAS IN DATABASE ICEBERG_DEMO_CLD;
SHOW ICEBERG TABLES IN DATABASE ICEBERG_DEMO_CLD;

-- CLD link status
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD') AS cld_status;

-- ============================================================================
-- SECTION 5: External Tables Data Validation
-- ============================================================================

USE ROLE ICEBERG_ANALYST;
USE WAREHOUSE ICEBERG_DEMO_WH;
USE DATABASE ICEBERG_DEMO_EXT;
USE SCHEMA ADVERTISING;

-- Row counts for external tables
SELECT 
    'EXT_CAMPAIGNS' AS table_name, COUNT(*) AS row_count FROM EXT_CAMPAIGNS
UNION ALL SELECT 'EXT_IMPRESSIONS', COUNT(*) FROM EXT_IMPRESSIONS
UNION ALL SELECT 'EXT_CLICKS', COUNT(*) FROM EXT_CLICKS
UNION ALL SELECT 'EXT_CONVERSIONS', COUNT(*) FROM EXT_CONVERSIONS;

-- Sample data
SELECT * FROM EXT_CAMPAIGNS LIMIT 3;

-- ============================================================================
-- SECTION 6: CLD Data Validation
-- ============================================================================

/*
 * Note: CLD is NOT replicated via failover groups.
 * - On PRIMARY: CLD created by script 11
 * - On SECONDARY: CLD created by script 30
 *
 * Both CLDs point to the SAME Glue catalog, so data should be identical.
 */

USE DATABASE ICEBERG_DEMO_CLD;

-- Row counts for CLD tables
SELECT 
    'CAMPAIGNS' AS table_name, COUNT(*) AS row_count 
    FROM ICEBERG_ADVERTISING_DB.CAMPAIGNS
UNION ALL SELECT 'IMPRESSIONS', COUNT(*) FROM ICEBERG_ADVERTISING_DB.IMPRESSIONS
UNION ALL SELECT 'CLICKS', COUNT(*) FROM ICEBERG_ADVERTISING_DB.CLICKS
UNION ALL SELECT 'CONVERSIONS', COUNT(*) FROM ICEBERG_ADVERTISING_DB.CONVERSIONS;

-- Sample data
SELECT * FROM ICEBERG_ADVERTISING_DB.CAMPAIGNS LIMIT 3;

-- ============================================================================
-- SECTION 7: Data Consistency Check (EXT vs CLD)
-- ============================================================================

/*
 * Compare data between External Tables and CLD approaches.
 * They should be identical since they point to the same Iceberg tables.
 */

WITH ext_counts AS (
    SELECT 'CAMPAIGNS' AS tbl, COUNT(*) AS cnt FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CAMPAIGNS
    UNION ALL SELECT 'IMPRESSIONS', COUNT(*) FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_IMPRESSIONS
    UNION ALL SELECT 'CLICKS', COUNT(*) FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CLICKS
    UNION ALL SELECT 'CONVERSIONS', COUNT(*) FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CONVERSIONS
),
cld_counts AS (
    SELECT 'CAMPAIGNS' AS tbl, COUNT(*) AS cnt FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS
    UNION ALL SELECT 'IMPRESSIONS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS
    UNION ALL SELECT 'CLICKS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS
    UNION ALL SELECT 'CONVERSIONS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS
)
SELECT 
    e.tbl AS table_name,
    e.cnt AS ext_count,
    c.cnt AS cld_count,
    CASE WHEN e.cnt = c.cnt THEN '✓ MATCH' ELSE '✗ MISMATCH' END AS status
FROM ext_counts e
JOIN cld_counts c ON e.tbl = c.tbl
ORDER BY e.tbl;

-- ============================================================================
-- SECTION 8: Failover Group Validation
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- List failover groups
SHOW FAILOVER GROUPS;

-- Failover group details
SELECT 
    REPLICATION_GROUP_NAME,
    CREATED_ON,
    IS_PRIMARY,
    OBJECT_TYPES,
    REPLICATION_ALLOWED_TO_ACCOUNTS
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUPS())
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
ORDER BY REPLICATION_GROUP_NAME;

-- Replication history
SELECT 
    REPLICATION_GROUP_NAME,
    PHASE,
    START_TIME,
    END_TIME,
    DATEDIFF('second', START_TIME, COALESCE(END_TIME, CURRENT_TIMESTAMP())) AS DURATION_SECONDS
FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
ORDER BY START_TIME DESC
LIMIT 10;

-- Objects in failover groups
SHOW DATABASES IN FAILOVER GROUP ICEBERG_BCDR_DB_FG;
SHOW INTEGRATIONS IN FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG;
SHOW ROLES IN FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG;
SHOW EXTERNAL VOLUMES IN FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG;

-- ============================================================================
-- SECTION 9: CLD Health Check
-- ============================================================================

-- CLD link status (should show success or errors)
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD') AS cld_status;

-- Refresh CLD to ensure latest state from Glue
ALTER DATABASE ICEBERG_DEMO_CLD REFRESH;

-- Check status again
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD') AS cld_status_after_refresh;

-- ============================================================================
-- SECTION 10: Access Control Validation
-- ============================================================================

-- Test analyst role access
USE ROLE ICEBERG_ANALYST;
USE WAREHOUSE ICEBERG_DEMO_WH;

-- Should succeed (analyst can read)
SELECT COUNT(*) AS ext_campaigns FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CAMPAIGNS;
SELECT COUNT(*) AS cld_campaigns FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS;

-- ============================================================================
-- SECTION 11: Validation Summary Report
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Comprehensive validation report
SELECT '═══════════════════════════════════════════════════════════════════════' AS report
UNION ALL SELECT 'ICEBERG CLD BCDR DEMO - VALIDATION SUMMARY'
UNION ALL SELECT '═══════════════════════════════════════════════════════════════════════'
UNION ALL SELECT ''
UNION ALL SELECT '▸ ACCOUNT: ' || CURRENT_ACCOUNT() || ' (' || CURRENT_ORGANIZATION_NAME() || ')'
UNION ALL SELECT ''
UNION ALL SELECT '▸ ACCOUNT OBJECTS (Replicated via Failover Groups)'
UNION ALL SELECT '  Roles:                ICEBERG_ADMIN, ICEBERG_ENGINEER, ICEBERG_ANALYST'
UNION ALL SELECT '  Storage Integration:  AWS_ICEBERG_STORAGE_INT'
UNION ALL SELECT '  Catalog Integration:  REST_GLUE_CATALOG_INT'
UNION ALL SELECT '  External Volume:      ICEBERG_EXT_VOLUME'
UNION ALL SELECT ''
UNION ALL SELECT '▸ DATABASES'
UNION ALL SELECT '  External Tables:      ICEBERG_DEMO_EXT.ADVERTISING.* (✓ Replicated)'
UNION ALL SELECT '  Catalog Linked DB:    ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.* (✗ NOT replicated)'
UNION ALL SELECT ''
UNION ALL SELECT '▸ FAILOVER GROUPS'
UNION ALL SELECT '  ICEBERG_BCDR_ACCOUNT_FG:  Roles, Integrations'
UNION ALL SELECT '  ICEBERG_BCDR_VOLUME_FG:   External Volumes'
UNION ALL SELECT '  ICEBERG_BCDR_DB_FG:       ICEBERG_DEMO_EXT only (NOT CLD!)'
UNION ALL SELECT ''
UNION ALL SELECT '▸ CLD ARCHITECTURE'
UNION ALL SELECT '  CLDs cannot be replicated via failover groups.'
UNION ALL SELECT '  Each account creates its own CLD pointing to the SAME Glue catalog.'
UNION ALL SELECT '  PRIMARY:   CLD created via script 11'
UNION ALL SELECT '  SECONDARY: CLD created via script 30'
UNION ALL SELECT ''
UNION ALL SELECT '═══════════════════════════════════════════════════════════════════════';

-- Data summary
SELECT 
    'Data Summary' AS category,
    (SELECT COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS) AS campaigns,
    (SELECT COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS) AS impressions,
    (SELECT COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS) AS clicks,
    (SELECT COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS) AS conversions;

/*******************************************************************************
 * VALIDATION CHECKLIST:
 *
 * SNOWFLAKE OBJECTS:
 * □ Roles created (ICEBERG_ADMIN, ICEBERG_ENGINEER, ICEBERG_ANALYST)
 * □ Role hierarchy correct (ANALYST → ENGINEER → ADMIN → ACCOUNTADMIN)
 * □ Warehouse accessible by all roles
 * □ Storage integration working
 * □ REST Catalog integration working (VENDED_CREDENTIALS mode)
 * □ External volume configured
 *
 * EXTERNAL TABLES (ICEBERG_DEMO_EXT):
 * □ Database exists
 * □ Tables created and queryable
 * □ Included in ICEBERG_BCDR_DB_FG failover group
 *
 * CATALOG LINKED DATABASE (ICEBERG_DEMO_CLD):
 * □ Database exists (created independently, NOT replicated)
 * □ CLD link status shows success (no errors)
 * □ Tables synced from Glue catalog
 * □ Data matches External Tables
 *
 * FAILOVER GROUPS:
 * □ ICEBERG_BCDR_ACCOUNT_FG (roles, integrations)
 * □ ICEBERG_BCDR_VOLUME_FG (external volumes)
 * □ ICEBERG_BCDR_DB_FG (ICEBERG_DEMO_EXT only - CLD NOT included)
 * □ Replication running (if secondary configured)
 *
 * AWS REQUIREMENTS FOR CLD:
 * □ S3 bucket with Iceberg tables
 * □ Glue database with table definitions
 * □ IAM role trusts BOTH Snowflake accounts' IAM users + Lake Formation
 * □ IAM role has lakeformation:GetDataAccess permission
 * □ Lake Formation: External engine access ENABLED
 * □ Lake Formation: Data permissions granted to IAM role
 *
 * SECONDARY ACCOUNT SPECIFIC:
 * □ Replica failover groups created
 * □ CLD created independently (script 30)
 * □ IAM trust policy includes secondary's Snowflake IAM user ARN
 ******************************************************************************/
