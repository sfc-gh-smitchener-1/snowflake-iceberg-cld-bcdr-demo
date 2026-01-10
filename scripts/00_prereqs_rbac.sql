/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 00_prereqs_rbac.sql
 * Purpose: Create role hierarchy, warehouse, and grants for the demo
 * 
 * Role Hierarchy:
 *   ACCOUNTADMIN
 *       └── ICEBERG_ADMIN (creates account objects: integrations, external volumes)
 *               └── ICEBERG_ENGINEER (creates database objects: schemas, tables)
 *                       └── ICEBERG_ANALYST (read-only access to data)
 *
 * Run as: ACCOUNTADMIN
 ******************************************************************************/

-- ============================================================================
-- SECTION 1: Create Custom Roles
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create the admin role for account-level object management
CREATE ROLE IF NOT EXISTS ICEBERG_ADMIN
    COMMENT = 'Admin role for Iceberg demo - manages account-level objects (integrations, external volumes)';

-- Create the engineer role for database object management
CREATE ROLE IF NOT EXISTS ICEBERG_ENGINEER
    COMMENT = 'Engineer role for Iceberg demo - creates and manages database objects (schemas, tables)';

-- Create the analyst role for read-only data access
CREATE ROLE IF NOT EXISTS ICEBERG_ANALYST
    COMMENT = 'Analyst role for Iceberg demo - read-only access to Iceberg tables';

-- ============================================================================
-- SECTION 2: Establish Role Hierarchy
-- ============================================================================

-- ICEBERG_ADMIN inherits from SYSADMIN (or directly from ACCOUNTADMIN for demo)
GRANT ROLE ICEBERG_ADMIN TO ROLE ACCOUNTADMIN;

-- ICEBERG_ENGINEER inherits from ICEBERG_ADMIN
GRANT ROLE ICEBERG_ENGINEER TO ROLE ICEBERG_ADMIN;

-- ICEBERG_ANALYST inherits from ICEBERG_ENGINEER
GRANT ROLE ICEBERG_ANALYST TO ROLE ICEBERG_ENGINEER;

-- Grant roles to your user (replace YOUR_USERNAME with actual username)
-- GRANT ROLE ICEBERG_ADMIN TO USER YOUR_USERNAME;
-- GRANT ROLE ICEBERG_ENGINEER TO USER YOUR_USERNAME;
-- GRANT ROLE ICEBERG_ANALYST TO USER YOUR_USERNAME;

-- ============================================================================
-- SECTION 3: Create Warehouse
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS ICEBERG_DEMO_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Iceberg CLD BCDR demo';

-- ============================================================================
-- SECTION 4: Grant Warehouse Privileges
-- ============================================================================

-- All roles can use the warehouse
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE ICEBERG_ADMIN;
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE ICEBERG_ANALYST;

-- Admin can modify warehouse settings
GRANT MODIFY ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 5: Grant Account-Level Privileges to ICEBERG_ADMIN
-- ============================================================================

-- Required for creating integrations
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE ICEBERG_ADMIN;

-- Required for creating external volumes
GRANT CREATE EXTERNAL VOLUME ON ACCOUNT TO ROLE ICEBERG_ADMIN;

-- Required for creating databases
GRANT CREATE DATABASE ON ACCOUNT TO ROLE ICEBERG_ADMIN;

-- Required for managing failover groups
GRANT CREATE FAILOVER GROUP ON ACCOUNT TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 6: Create Databases (structure only - content created later)
-- ============================================================================

-- Database for External Tables approach
CREATE DATABASE IF NOT EXISTS ICEBERG_DEMO_EXT
    COMMENT = 'Iceberg demo database using External Tables approach';

-- Database for Catalog Linked Database approach  
CREATE DATABASE IF NOT EXISTS ICEBERG_DEMO_CLD
    COMMENT = 'Iceberg demo database using Catalog Linked Database approach';

-- ============================================================================
-- SECTION 7: Grant Database Privileges
-- ============================================================================

-- ICEBERG_ADMIN gets ownership of databases
GRANT OWNERSHIP ON DATABASE ICEBERG_DEMO_EXT TO ROLE ICEBERG_ADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ADMIN COPY CURRENT GRANTS;

-- ICEBERG_ENGINEER can create objects in databases
GRANT USAGE ON DATABASE ICEBERG_DEMO_EXT TO ROLE ICEBERG_ENGINEER;
GRANT USAGE ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ENGINEER;
GRANT CREATE SCHEMA ON DATABASE ICEBERG_DEMO_EXT TO ROLE ICEBERG_ENGINEER;
GRANT CREATE SCHEMA ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ENGINEER;

-- ICEBERG_ANALYST gets read access to databases
GRANT USAGE ON DATABASE ICEBERG_DEMO_EXT TO ROLE ICEBERG_ANALYST;
GRANT USAGE ON DATABASE ICEBERG_DEMO_CLD TO ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 8: Create Schemas
-- ============================================================================

USE ROLE ICEBERG_ENGINEER;
USE WAREHOUSE ICEBERG_DEMO_WH;

-- Create ADVERTISING schema in External Tables database
CREATE SCHEMA IF NOT EXISTS ICEBERG_DEMO_EXT.ADVERTISING
    COMMENT = 'Schema for advertising data accessed via External Tables';

-- Note: ADVERTISING schema in CLD will be auto-created from Glue catalog
-- or created manually in script 11_catalog_linked_database.sql

-- ============================================================================
-- SECTION 9: Grant Schema Privileges
-- ============================================================================

USE ROLE ICEBERG_ADMIN;

-- Grant schema privileges to ICEBERG_ENGINEER
GRANT USAGE ON SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ENGINEER;
GRANT CREATE TABLE ON SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ENGINEER;
GRANT CREATE VIEW ON SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ENGINEER;
GRANT CREATE ICEBERG TABLE ON SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ENGINEER;

-- Grant schema privileges to ICEBERG_ANALYST (read-only)
GRANT USAGE ON SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ANALYST;

-- Future grants for tables created in the schema
GRANT SELECT ON FUTURE TABLES IN SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ANALYST;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA ICEBERG_DEMO_EXT.ADVERTISING TO ROLE ICEBERG_ANALYST;

-- ============================================================================
-- SECTION 10: Verification Queries
-- ============================================================================

-- Show created roles
SHOW ROLES LIKE 'ICEBERG%';

-- Show role grants
SHOW GRANTS TO ROLE ICEBERG_ADMIN;
SHOW GRANTS TO ROLE ICEBERG_ENGINEER;
SHOW GRANTS TO ROLE ICEBERG_ANALYST;

-- Show warehouse
SHOW WAREHOUSES LIKE 'ICEBERG_DEMO_WH';

-- Show databases
SHOW DATABASES LIKE 'ICEBERG_DEMO%';

-- Show schemas
SHOW SCHEMAS IN DATABASE ICEBERG_DEMO_EXT;

/*******************************************************************************
 * NEXT STEPS:
 * 1. Run 01_aws_integrations.sql as ICEBERG_ADMIN to create integrations
 * 2. Complete AWS trust relationship setup
 * 3. Run 02_generate_iceberg_data.py to create sample data
 ******************************************************************************/

