-- ============================================================================
-- ICEBERG BCDR DEMO: Primary Sync Task and Stored Procedure
-- ============================================================================
-- This script creates:
-- 1. TASK_WH warehouse (XS Gen 2)
-- 2. Monitoring/logging table
-- 3. Python stored procedure for syncing EXT/CLD to PROD
-- 4. Task that runs every 5 minutes
-- ============================================================================

-- ============================================================================
-- SECTION 1: Create Task Warehouse
-- ============================================================================

USE ROLE ICEBERG_ADMIN;

-- Create XS Gen 2 warehouse for task processing
CREATE WAREHOUSE IF NOT EXISTS TASK_WH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'  -- Gen 2
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'XS Gen 2 warehouse for BCDR sync tasks';

-- Grant usage to roles
GRANT USAGE ON WAREHOUSE TASK_WH TO ROLE ICEBERG_ENGINEER;
GRANT OPERATE ON WAREHOUSE TASK_WH TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 2: Create Monitoring Schema and Log Table
-- ============================================================================

USE WAREHOUSE TASK_WH;

-- Create monitoring schema in PROD database
CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.MONITORING
    COMMENT = 'Schema for BCDR sync monitoring and logging';

-- Create sync log table
CREATE TABLE IF NOT EXISTS ICEBERG_PROD.MONITORING.SYNC_LOG (
    sync_id NUMBER AUTOINCREMENT,
    sync_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    sync_type VARCHAR(50),           -- 'FULL_SYNC', 'INCREMENTAL', 'VIEW_UPDATE', 'MATVIEW_REFRESH'
    source_database VARCHAR(100),
    source_schema VARCHAR(100),
    object_type VARCHAR(50),         -- 'TABLE', 'VIEW', 'MATERIALIZED_VIEW'
    object_name VARCHAR(255),
    action VARCHAR(50),              -- 'CREATED', 'UPDATED', 'DROPPED', 'REFRESHED', 'NO_CHANGE'
    status VARCHAR(20),              -- 'SUCCESS', 'FAILED', 'SKIPPED'
    row_count NUMBER,
    error_message VARCHAR(4000),
    execution_time_ms NUMBER,
    PRIMARY KEY (sync_id)
);

-- Create heartbeat log table
CREATE TABLE IF NOT EXISTS ICEBERG_PROD.MONITORING.HEARTBEAT_LOG (
    heartbeat_id NUMBER AUTOINCREMENT,
    heartbeat_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    account_type VARCHAR(20),        -- 'PRIMARY', 'SECONDARY'
    status VARCHAR(20),              -- 'SUCCESS', 'FAILED'
    details VARCHAR(4000),
    error_msg VARCHAR(4000),
    PRIMARY KEY (heartbeat_id)
);

-- Create object inventory table (tracks what's in each database)
CREATE TABLE IF NOT EXISTS ICEBERG_PROD.MONITORING.OBJECT_INVENTORY (
    inventory_id NUMBER AUTOINCREMENT,
    snapshot_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    object_type VARCHAR(50),
    object_name VARCHAR(255),
    column_count NUMBER,
    row_count NUMBER,
    last_modified TIMESTAMP_NTZ,
    PRIMARY KEY (inventory_id)
);

-- ============================================================================
-- SECTION 3: Python Stored Procedure for Sync
-- ============================================================================

CREATE OR REPLACE PROCEDURE ICEBERG_PROD.MONITORING.SYNC_PROD_DATABASE()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from datetime import datetime
import json

def log_sync(session, sync_type, source_db, source_schema, obj_type, obj_name, action, status, row_count=None, error_msg=None, exec_time=None):
    """Log sync activity to the monitoring table"""
    try:
        session.sql(f"""
            INSERT INTO ICEBERG_PROD.MONITORING.SYNC_LOG 
            (sync_type, source_database, source_schema, object_type, object_name, action, status, row_count, error_message, execution_time_ms)
            VALUES ('{sync_type}', '{source_db}', '{source_schema}', '{obj_type}', '{obj_name}', '{action}', '{status}', 
                    {row_count if row_count else 'NULL'}, 
                    {f"'{error_msg}'" if error_msg else 'NULL'}, 
                    {exec_time if exec_time else 'NULL'})
        """).collect()
    except Exception as e:
        pass  # Don't fail on logging errors

def get_tables_from_cld(session):
    """Get list of tables from CLD database"""
    try:
        result = session.sql("""
            SELECT table_name, table_schema 
            FROM ICEBERG_DEMO_CLD.INFORMATION_SCHEMA.TABLES 
            WHERE table_schema != 'INFORMATION_SCHEMA'
            AND table_type = 'BASE TABLE'
        """).collect()
        return [(row['TABLE_NAME'], row['TABLE_SCHEMA']) for row in result]
    except:
        return []

def get_tables_from_ext(session):
    """Get list of external tables from EXT database"""
    try:
        result = session.sql("""
            SELECT table_name, table_schema 
            FROM ICEBERG_DEMO_EXT.INFORMATION_SCHEMA.TABLES 
            WHERE table_schema != 'INFORMATION_SCHEMA'
        """).collect()
        return [(row['TABLE_NAME'], row['TABLE_SCHEMA']) for row in result]
    except:
        return []

def get_views_from_ext(session):
    """Get list of views from EXT database"""
    try:
        result = session.sql("""
            SELECT table_name, table_schema, view_definition
            FROM ICEBERG_DEMO_EXT.INFORMATION_SCHEMA.VIEWS 
            WHERE table_schema != 'INFORMATION_SCHEMA'
        """).collect()
        return [(row['TABLE_NAME'], row['TABLE_SCHEMA'], row['VIEW_DEFINITION']) for row in result]
    except:
        return []

def get_matviews_from_ext(session):
    """Get materialized views from EXT database"""
    try:
        result = session.sql("""
            SHOW MATERIALIZED VIEWS IN DATABASE ICEBERG_DEMO_EXT
        """).collect()
        return [(row['name'], row['schema_name'], row['text']) for row in result]
    except:
        return []

def get_existing_prod_views(session):
    """Get existing views in PROD database"""
    try:
        result = session.sql("""
            SELECT table_name, table_schema
            FROM ICEBERG_PROD.INFORMATION_SCHEMA.VIEWS 
            WHERE table_schema != 'INFORMATION_SCHEMA'
        """).collect()
        return {(row['TABLE_NAME'], row['TABLE_SCHEMA']) for row in result}
    except:
        return set()

def ensure_schema_exists(session, schema_name):
    """Ensure schema exists in PROD database"""
    try:
        session.sql(f"""
            CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.{schema_name}
        """).collect()
        return True
    except:
        return False

def create_base_view_from_cld(session, table_name, source_schema):
    """Create a SELECT * view in PROD from CLD table"""
    start_time = datetime.now()
    try:
        # Ensure target schema exists
        ensure_schema_exists(session, source_schema)
        
        # Create view
        view_sql = f"""
            CREATE OR REPLACE VIEW ICEBERG_PROD.{source_schema}.{table_name}
            COMMENT = 'Auto-synced view from CLD: ICEBERG_DEMO_CLD.{source_schema}.{table_name}'
            AS SELECT * FROM ICEBERG_DEMO_CLD.{source_schema}.{table_name}
        """
        session.sql(view_sql).collect()
        
        # Get row count
        count_result = session.sql(f"SELECT COUNT(*) as cnt FROM ICEBERG_PROD.{source_schema}.{table_name}").collect()
        row_count = count_result[0]['CNT'] if count_result else 0
        
        exec_time = int((datetime.now() - start_time).total_seconds() * 1000)
        log_sync(session, 'FULL_SYNC', 'ICEBERG_DEMO_CLD', source_schema, 'VIEW', table_name, 'CREATED', 'SUCCESS', row_count, None, exec_time)
        return True, row_count
    except Exception as e:
        exec_time = int((datetime.now() - start_time).total_seconds() * 1000)
        log_sync(session, 'FULL_SYNC', 'ICEBERG_DEMO_CLD', source_schema, 'VIEW', table_name, 'FAILED', 'FAILED', None, str(e)[:500], exec_time)
        return False, str(e)

def extract_select_statement(definition):
    """Extract SELECT statement from view/materialized view definition"""
    if not definition:
        return None
    
    # Find the SELECT keyword (case-insensitive)
    upper_def = definition.upper()
    select_idx = upper_def.find('SELECT')
    
    if select_idx == -1:
        return None
    
    # Return from SELECT onwards
    return definition[select_idx:]

def convert_ext_view_to_cld(session, view_name, source_schema, view_definition):
    """Convert EXT view to use CLD tables instead"""
    start_time = datetime.now()
    try:
        # Ensure target schema exists
        ensure_schema_exists(session, source_schema)
        
        # Extract just the SELECT portion
        select_sql = extract_select_statement(view_definition)
        if not select_sql:
            raise ValueError("Could not extract SELECT statement from view definition")
        
        # Replace EXT references with CLD references
        converted_sql = select_sql
        converted_sql = converted_sql.replace('ICEBERG_DEMO_EXT.ADVERTISING.', 'ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.')
        converted_sql = converted_sql.replace('"ICEBERG_DEMO_EXT"."ADVERTISING".', '"ICEBERG_DEMO_CLD"."ICEBERG_ADVERTISING_DB".')
        # Also handle unquoted references
        converted_sql = converted_sql.replace('EXT_CAMPAIGNS', 'CAMPAIGNS')
        converted_sql = converted_sql.replace('EXT_IMPRESSIONS', 'IMPRESSIONS')
        converted_sql = converted_sql.replace('EXT_CLICKS', 'CLICKS')
        converted_sql = converted_sql.replace('EXT_CONVERSIONS', 'CONVERSIONS')
        
        # Create the view with converted definition
        view_sql = f"""
            CREATE OR REPLACE VIEW ICEBERG_PROD.{source_schema}.{view_name}
            AS {converted_sql}
        """
        session.sql(view_sql).collect()
        
        exec_time = int((datetime.now() - start_time).total_seconds() * 1000)
        log_sync(session, 'VIEW_CONVERT', 'ICEBERG_DEMO_EXT', source_schema, 'VIEW', view_name, 'CREATED', 'SUCCESS', None, None, exec_time)
        return True, None
    except Exception as e:
        exec_time = int((datetime.now() - start_time).total_seconds() * 1000)
        log_sync(session, 'VIEW_CONVERT', 'ICEBERG_DEMO_EXT', source_schema, 'VIEW', view_name, 'FAILED', 'FAILED', None, str(e)[:500], exec_time)
        return False, str(e)

def sync_matview_as_view(session, mv_name, source_schema, mv_definition):
    """Convert materialized view to regular view using CLD source"""
    start_time = datetime.now()
    try:
        ensure_schema_exists(session, source_schema)
        
        # Extract just the SELECT portion from the full DDL
        select_sql = extract_select_statement(mv_definition)
        if not select_sql:
            raise ValueError("Could not extract SELECT statement from materialized view definition")
        
        # Replace EXT references with CLD references
        converted_sql = select_sql
        converted_sql = converted_sql.replace('ICEBERG_DEMO_EXT.ADVERTISING.', 'ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.')
        converted_sql = converted_sql.replace('"ICEBERG_DEMO_EXT"."ADVERTISING".', '"ICEBERG_DEMO_CLD"."ICEBERG_ADVERTISING_DB".')
        # Also handle unquoted EXT_ prefixed table references
        converted_sql = converted_sql.replace('EXT_CAMPAIGNS', 'CAMPAIGNS')
        converted_sql = converted_sql.replace('EXT_IMPRESSIONS', 'IMPRESSIONS')
        converted_sql = converted_sql.replace('EXT_CLICKS', 'CLICKS')
        converted_sql = converted_sql.replace('EXT_CONVERSIONS', 'CONVERSIONS')
        
        view_sql = f"""
            CREATE OR REPLACE VIEW ICEBERG_PROD.{source_schema}.{mv_name}
            AS {converted_sql}
        """
        session.sql(view_sql).collect()
        
        exec_time = int((datetime.now() - start_time).total_seconds() * 1000)
        log_sync(session, 'MATVIEW_SYNC', 'ICEBERG_DEMO_EXT', source_schema, 'VIEW', mv_name, 'CREATED', 'SUCCESS', None, None, exec_time)
        return True, None
    except Exception as e:
        exec_time = int((datetime.now() - start_time).total_seconds() * 1000)
        log_sync(session, 'MATVIEW_SYNC', 'ICEBERG_DEMO_EXT', source_schema, 'VIEW', mv_name, 'FAILED', 'FAILED', None, str(e)[:500], exec_time)
        return False, str(e)

def create_aggregation_views(session):
    """Create aggregation views using CLD tables with correct lowercase column names"""
    views_created = 0
    errors = []
    
    # Ensure ADVERTISING schema exists in PROD
    session.sql("CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.ADVERTISING").collect()
    
    # Define aggregation views with correct lowercase column names for CLD
    # Column names from generate script: campaign_name, budget_usd, geo_region, revenue_usd
    agg_views = {
        'MV_CAMPAIGNS_SUMMARY': """
            SELECT 
                c."campaign_id",
                c."campaign_name",
                c."channel",
                c."status",
                c."budget_usd",
                c."start_date",
                c."end_date",
                COUNT(DISTINCT i."impression_id") as total_impressions,
                COUNT(DISTINCT cl."click_id") as total_clicks,
                COUNT(DISTINCT cv."conversion_id") as total_conversions,
                COALESCE(SUM(cv."revenue_usd"), 0) as total_revenue,
                CASE WHEN COUNT(DISTINCT i."impression_id") > 0 
                     THEN ROUND(COUNT(DISTINCT cl."click_id")::FLOAT / COUNT(DISTINCT i."impression_id") * 100, 2)
                     ELSE 0 END as ctr_percent,
                CASE WHEN COUNT(DISTINCT cl."click_id") > 0
                     THEN ROUND(COUNT(DISTINCT cv."conversion_id")::FLOAT / COUNT(DISTINCT cl."click_id") * 100, 2)
                     ELSE 0 END as conversion_rate_percent
            FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS c
            LEFT JOIN ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS i ON c."campaign_id" = i."campaign_id"
            LEFT JOIN ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS cl ON c."campaign_id" = cl."campaign_id"
            LEFT JOIN ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS cv ON c."campaign_id" = cv."campaign_id"
            GROUP BY c."campaign_id", c."campaign_name", c."channel", c."status", c."budget_usd", c."start_date", c."end_date"
        """,
        'MV_IMPRESSIONS_DAILY': """
            SELECT 
                DATE_TRUNC('DAY', TO_TIMESTAMP(i."timestamp")) as impression_date,
                i."campaign_id",
                c."campaign_name",
                i."device_type",
                i."geo_region",
                COUNT(*) as impression_count
            FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS i
            JOIN ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS c ON i."campaign_id" = c."campaign_id"
            GROUP BY DATE_TRUNC('DAY', TO_TIMESTAMP(i."timestamp")), i."campaign_id", c."campaign_name", i."device_type", i."geo_region"
        """,
        'MV_CLICKS_DAILY': """
            SELECT 
                DATE_TRUNC('DAY', TO_TIMESTAMP(cl."timestamp")) as click_date,
                cl."campaign_id",
                c."campaign_name",
                COUNT(*) as click_count
            FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS cl
            JOIN ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS c ON cl."campaign_id" = c."campaign_id"
            GROUP BY DATE_TRUNC('DAY', TO_TIMESTAMP(cl."timestamp")), cl."campaign_id", c."campaign_name"
        """,
        'MV_CONVERSIONS_DAILY': """
            SELECT 
                DATE_TRUNC('DAY', TO_TIMESTAMP(cv."timestamp")) as conversion_date,
                cv."campaign_id",
                c."campaign_name",
                COUNT(*) as conversion_count,
                SUM(cv."revenue_usd") as total_revenue
            FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS cv
            JOIN ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS c ON cv."campaign_id" = c."campaign_id"
            GROUP BY DATE_TRUNC('DAY', TO_TIMESTAMP(cv."timestamp")), cv."campaign_id", c."campaign_name"
        """,
        'MV_CAMPAIGN_PERFORMANCE': """
            SELECT 
                c."campaign_id",
                c."campaign_name",
                c."channel",
                c."status",
                c."budget_usd",
                c."daily_budget_usd",
                c."start_date",
                c."end_date",
                COALESCE(imp.impression_count, 0) as impression_count,
                COALESCE(clk.click_count, 0) as click_count,
                COALESCE(conv.conversion_count, 0) as conversion_count,
                COALESCE(conv.total_revenue, 0) as total_revenue,
                CASE WHEN COALESCE(imp.impression_count, 0) > 0 
                     THEN ROUND(COALESCE(clk.click_count, 0)::FLOAT / imp.impression_count * 100, 2)
                     ELSE 0 END as ctr_percent,
                CASE WHEN COALESCE(clk.click_count, 0) > 0
                     THEN ROUND(COALESCE(conv.conversion_count, 0)::FLOAT / clk.click_count * 100, 2)
                     ELSE 0 END as conversion_rate
            FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS c
            LEFT JOIN (
                SELECT "campaign_id", COUNT(*) as impression_count
                FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS
                GROUP BY "campaign_id"
            ) imp ON c."campaign_id" = imp."campaign_id"
            LEFT JOIN (
                SELECT "campaign_id", COUNT(*) as click_count
                FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS
                GROUP BY "campaign_id"
            ) clk ON c."campaign_id" = clk."campaign_id"
            LEFT JOIN (
                SELECT "campaign_id", COUNT(*) as conversion_count, COALESCE(SUM("revenue_usd"), 0) as total_revenue
                FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS
                GROUP BY "campaign_id"
            ) conv ON c."campaign_id" = conv."campaign_id"
        """
    }
    
    for view_name, view_sql in agg_views.items():
        try:
            full_sql = f"CREATE OR REPLACE VIEW ICEBERG_PROD.ADVERTISING.{view_name} AS {view_sql}"
            session.sql(full_sql).collect()
            log_sync(session, 'AGG_VIEW', 'ICEBERG_DEMO_CLD', 'ADVERTISING', 'VIEW', view_name, 'CREATED', 'SUCCESS', None, None, None)
            views_created += 1
        except Exception as e:
            log_sync(session, 'AGG_VIEW', 'ICEBERG_DEMO_CLD', 'ADVERTISING', 'VIEW', view_name, 'FAILED', 'FAILED', None, str(e)[:500], None)
            errors.append(f"AGG_VIEW:{view_name}: {str(e)[:200]}")
    
    return views_created, errors

def main(session: snowpark.Session) -> dict:
    """Main sync procedure"""
    start_time = datetime.now()
    results = {
        'sync_timestamp': start_time.isoformat(),
        'cld_tables_synced': 0,
        'agg_views_synced': 0,
        'errors': [],
        'status': 'SUCCESS'
    }
    
    try:
        # 1. Sync CLD tables as base views (primary sync)
        cld_tables = get_tables_from_cld(session)
        for table_name, schema_name in cld_tables:
            success, result = create_base_view_from_cld(session, table_name, schema_name)
            if success:
                results['cld_tables_synced'] += 1
            else:
                results['errors'].append(f"CLD:{schema_name}.{table_name}: {result}")
        
        # 2. Create aggregation views using CLD with correct lowercase column names
        agg_created, agg_errors = create_aggregation_views(session)
        results['agg_views_synced'] = agg_created
        results['errors'].extend(agg_errors)
        
        # 3. Log heartbeat
        session.sql(f"""
            INSERT INTO ICEBERG_PROD.MONITORING.HEARTBEAT_LOG (account_type, status, details)
            VALUES ('PRIMARY', 'SUCCESS', 'Synced {results['cld_tables_synced']} CLD base tables and {results['agg_views_synced']} aggregation views to PROD.')
        """).collect()
        
        # 5. Apply grants for replicated objects
        session.sql("GRANT SELECT ON ALL VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ANALYST").collect()
        session.sql("GRANT SELECT ON ALL VIEWS IN SCHEMA ICEBERG_PROD.ADVERTISING TO ROLE ICEBERG_ENGINEER").collect()
        
        # Grant on any new schemas that were created
        session.sql("GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ANALYST").collect()
        session.sql("GRANT USAGE ON ALL SCHEMAS IN DATABASE ICEBERG_PROD TO ROLE ICEBERG_ENGINEER").collect()
        
    except Exception as e:
        results['status'] = 'FAILED'
        results['errors'].append(str(e))
        
        # Log failure
        session.sql(f"""
            INSERT INTO ICEBERG_PROD.MONITORING.HEARTBEAT_LOG (account_type, status, error_msg)
            VALUES ('PRIMARY', 'FAILED', '{str(e)[:500]}')
        """).collect()
    
    results['execution_time_seconds'] = (datetime.now() - start_time).total_seconds()
    return results
$$;

-- ============================================================================
-- SECTION 4: Create Primary Sync Task
-- ============================================================================

-- Create the sync task
CREATE OR REPLACE TASK ICEBERG_PROD.MONITORING.ICEBERG_SYNC_TASK
    WAREHOUSE = 'TASK_WH'
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Syncs EXT/CLD changes to PROD database every 5 minutes'
AS
    CALL ICEBERG_PROD.MONITORING.SYNC_PROD_DATABASE();

-- Grant EXECUTE TASK to admin role
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 5: Manual Execution and Testing
-- ============================================================================

-- Test the stored procedure manually first
CALL ICEBERG_PROD.MONITORING.SYNC_PROD_DATABASE();

-- View sync results
SELECT * FROM ICEBERG_PROD.MONITORING.SYNC_LOG 
ORDER BY sync_timestamp DESC 
LIMIT 20;

-- View heartbeat log
SELECT * FROM ICEBERG_PROD.MONITORING.HEARTBEAT_LOG 
ORDER BY heartbeat_timestamp DESC 
LIMIT 10;

-- ============================================================================
-- SECTION 6: Start the Task
-- ============================================================================

-- Resume (start) the task
ALTER TASK ICEBERG_PROD.MONITORING.ICEBERG_SYNC_TASK RESUME;

-- Verify task is running
SHOW TASKS IN SCHEMA ICEBERG_PROD.MONITORING;

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'ICEBERG_SYNC_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- ============================================================================
-- SECTION 7: Monitoring Queries
-- ============================================================================

-- Summary of sync activity by type
SELECT 
    sync_type,
    action,
    status,
    COUNT(*) as count,
    AVG(execution_time_ms) as avg_exec_time_ms
FROM ICEBERG_PROD.MONITORING.SYNC_LOG
WHERE sync_timestamp > DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY sync_type, action, status
ORDER BY sync_type, action;

-- Recent errors
SELECT 
    sync_timestamp,
    sync_type,
    source_database,
    object_name,
    error_message
FROM ICEBERG_PROD.MONITORING.SYNC_LOG
WHERE status = 'FAILED'
AND sync_timestamp > DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY sync_timestamp DESC;

-- Objects inventory comparison
SELECT 
    database_name,
    schema_name,
    object_type,
    COUNT(*) as object_count
FROM ICEBERG_PROD.MONITORING.OBJECT_INVENTORY
WHERE snapshot_timestamp = (SELECT MAX(snapshot_timestamp) FROM ICEBERG_PROD.MONITORING.OBJECT_INVENTORY)
GROUP BY database_name, schema_name, object_type
ORDER BY database_name, schema_name;

-- ============================================================================
-- SECTION 8: Task Management Commands
-- ============================================================================

-- Suspend the task (for maintenance)
-- ALTER TASK ICEBERG_PROD.MONITORING.ICEBERG_SYNC_TASK SUSPEND;

-- Resume the task
-- ALTER TASK ICEBERG_PROD.MONITORING.ICEBERG_SYNC_TASK RESUME;

-- Execute task immediately (manual trigger)
-- EXECUTE TASK ICEBERG_PROD.MONITORING.ICEBERG_SYNC_TASK;

-- Drop and recreate if needed
-- DROP TASK IF EXISTS ICEBERG_PROD.MONITORING.ICEBERG_SYNC_TASK;

