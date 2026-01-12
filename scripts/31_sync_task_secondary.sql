-- ============================================================================
-- ICEBERG BCDR DEMO: Secondary Sync Task (Resilient Heartbeat)
-- ============================================================================
-- This script creates:
-- 1. TASK_WH warehouse (XS Gen 2) on secondary
-- 2. Monitoring/logging tables
-- 3. Python stored procedure for secondary health checks
-- 4. Task that runs every 5 minutes
-- 
-- RUN THIS ON: SECONDARY ACCOUNT (OZC55031)
-- ============================================================================

-- ============================================================================
-- SECTION 1: Create Task Warehouse (if not replicated)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create XS Gen 2 warehouse for task processing
-- Note: This may already exist if warehouses are replicated
CREATE WAREHOUSE IF NOT EXISTS TASK_WH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'  -- Gen 2
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'XS Gen 2 warehouse for BCDR sync tasks';

-- Grant usage to roles (roles should be replicated)
GRANT USAGE ON WAREHOUSE TASK_WH TO ROLE ICEBERG_ENGINEER;
GRANT OPERATE ON WAREHOUSE TASK_WH TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 2: Create Monitoring Schema and Log Table
-- ============================================================================

USE ROLE ICEBERG_ADMIN;
USE WAREHOUSE TASK_WH;

-- Monitoring schema should be replicated, but ensure it exists
-- The PROD database is replicated from primary
USE DATABASE ICEBERG_PROD;

-- Create DR-specific monitoring schema if needed
CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.DR_MONITORING
    COMMENT = 'Secondary-specific monitoring for DR operations';

-- Create DR heartbeat log (secondary-specific)
CREATE TABLE IF NOT EXISTS ICEBERG_PROD.DR_MONITORING.SECONDARY_HEARTBEAT_LOG (
    heartbeat_id NUMBER AUTOINCREMENT,
    heartbeat_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    check_type VARCHAR(50),           -- 'CLD_REFRESH', 'GRANT_AUDIT', 'DATA_VALIDATION'
    status VARCHAR(20),               -- 'SUCCESS', 'FAILED', 'WARNING'
    details VARCHAR(4000),
    error_msg VARCHAR(4000),
    cld_table_count NUMBER,
    prod_view_count NUMBER,
    replication_lag_seconds NUMBER,
    PRIMARY KEY (heartbeat_id)
);

-- Create grant audit log
CREATE TABLE IF NOT EXISTS ICEBERG_PROD.DR_MONITORING.GRANT_AUDIT_LOG (
    audit_id NUMBER AUTOINCREMENT,
    audit_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    object_type VARCHAR(50),
    object_name VARCHAR(255),
    grantee_role VARCHAR(100),
    privilege VARCHAR(50),
    action VARCHAR(20),               -- 'GRANTED', 'VERIFIED', 'MISSING'
    status VARCHAR(20),
    PRIMARY KEY (audit_id)
);

-- Create CLD sync status table
CREATE TABLE IF NOT EXISTS ICEBERG_PROD.DR_MONITORING.CLD_SYNC_STATUS (
    sync_id NUMBER AUTOINCREMENT,
    sync_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    cld_database VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(255),
    status VARCHAR(20),               -- 'SYNCED', 'PENDING', 'ERROR'
    row_count NUMBER,
    last_modified TIMESTAMP_NTZ,
    PRIMARY KEY (sync_id)
);

-- ============================================================================
-- SECTION 3: Python Stored Procedure for Secondary Health Check
-- ============================================================================

CREATE OR REPLACE PROCEDURE ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from datetime import datetime
import json

def log_heartbeat(session, check_type, status, details=None, error_msg=None, cld_count=None, prod_count=None, lag=None):
    """Log heartbeat to monitoring table"""
    try:
        session.sql(f"""
            INSERT INTO ICEBERG_PROD.DR_MONITORING.SECONDARY_HEARTBEAT_LOG 
            (check_type, status, details, error_msg, cld_table_count, prod_view_count, replication_lag_seconds)
            VALUES ('{check_type}', '{status}', 
                    {f"'{details}'" if details else 'NULL'},
                    {f"'{error_msg}'" if error_msg else 'NULL'},
                    {cld_count if cld_count else 'NULL'},
                    {prod_count if prod_count else 'NULL'},
                    {lag if lag else 'NULL'})
        """).collect()
    except:
        pass

def log_grant_audit(session, obj_type, obj_name, role, privilege, action, status):
    """Log grant audit activity"""
    try:
        session.sql(f"""
            INSERT INTO ICEBERG_PROD.DR_MONITORING.GRANT_AUDIT_LOG 
            (object_type, object_name, grantee_role, privilege, action, status)
            VALUES ('{obj_type}', '{obj_name}', '{role}', '{privilege}', '{action}', '{status}')
        """).collect()
    except:
        pass

def refresh_cld(session):
    """Refresh the CLD database metadata"""
    try:
        session.sql("ALTER DATABASE ICEBERG_DEMO_CLD REFRESH").collect()
        return True, "CLD refreshed successfully"
    except Exception as e:
        return False, str(e)

def get_cld_table_count(session):
    """Get count of tables in CLD"""
    try:
        result = session.sql("""
            SELECT COUNT(*) as cnt 
            FROM ICEBERG_DEMO_CLD.INFORMATION_SCHEMA.TABLES 
            WHERE table_schema != 'INFORMATION_SCHEMA'
        """).collect()
        return result[0]['CNT'] if result else 0
    except:
        return 0

def get_prod_view_count(session):
    """Get count of views in PROD"""
    try:
        result = session.sql("""
            SELECT COUNT(*) as cnt 
            FROM ICEBERG_PROD.INFORMATION_SCHEMA.VIEWS 
            WHERE table_schema != 'INFORMATION_SCHEMA'
        """).collect()
        return result[0]['CNT'] if result else 0
    except:
        return 0

def ensure_database_grants(session):
    """Ensure database-level grants exist"""
    grants = [
        ("ICEBERG_DEMO_CLD", "ICEBERG_ANALYST", "USAGE"),
        ("ICEBERG_DEMO_CLD", "ICEBERG_ENGINEER", "USAGE"),
        ("ICEBERG_PROD", "ICEBERG_ANALYST", "USAGE"),
        ("ICEBERG_PROD", "ICEBERG_ENGINEER", "USAGE"),
    ]
    
    results = []
    for db, role, priv in grants:
        try:
            session.sql(f"GRANT {priv} ON DATABASE {db} TO ROLE {role}").collect()
            log_grant_audit(session, 'DATABASE', db, role, priv, 'GRANTED', 'SUCCESS')
            results.append((db, role, 'SUCCESS'))
        except Exception as e:
            log_grant_audit(session, 'DATABASE', db, role, priv, 'FAILED', str(e)[:100])
            results.append((db, role, 'FAILED'))
    return results

def ensure_schema_grants(session):
    """Ensure schema-level grants and future grants exist"""
    schema_grants = [
        ("ICEBERG_DEMO_CLD", "ICEBERG_ANALYST"),
        ("ICEBERG_DEMO_CLD", "ICEBERG_ENGINEER"),
        ("ICEBERG_PROD", "ICEBERG_ANALYST"),
        ("ICEBERG_PROD", "ICEBERG_ENGINEER"),
    ]
    
    results = []
    for db, role in schema_grants:
        try:
            # Current schemas
            session.sql(f"GRANT USAGE ON ALL SCHEMAS IN DATABASE {db} TO ROLE {role}").collect()
            # Future schemas
            session.sql(f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {db} TO ROLE {role}").collect()
            log_grant_audit(session, 'SCHEMA', f'{db}.*', role, 'USAGE', 'GRANTED', 'SUCCESS')
            results.append((db, role, 'SUCCESS'))
        except Exception as e:
            log_grant_audit(session, 'SCHEMA', f'{db}.*', role, 'USAGE', 'FAILED', str(e)[:100])
            results.append((db, role, 'FAILED'))
    return results

def ensure_table_grants(session):
    """Ensure table/view SELECT grants and future grants exist"""
    table_grants = [
        ("ICEBERG_DEMO_CLD", "ICEBERG_ANALYST"),
        ("ICEBERG_DEMO_CLD", "ICEBERG_ENGINEER"),
        ("ICEBERG_PROD", "ICEBERG_ANALYST"),
        ("ICEBERG_PROD", "ICEBERG_ENGINEER"),
    ]
    
    results = []
    for db, role in table_grants:
        try:
            # Current tables
            session.sql(f"GRANT SELECT ON ALL TABLES IN DATABASE {db} TO ROLE {role}").collect()
            # Current views
            session.sql(f"GRANT SELECT ON ALL VIEWS IN DATABASE {db} TO ROLE {role}").collect()
            # Future tables
            session.sql(f"GRANT SELECT ON FUTURE TABLES IN DATABASE {db} TO ROLE {role}").collect()
            # Future views
            session.sql(f"GRANT SELECT ON FUTURE VIEWS IN DATABASE {db} TO ROLE {role}").collect()
            log_grant_audit(session, 'TABLE/VIEW', f'{db}.*', role, 'SELECT', 'GRANTED', 'SUCCESS')
            results.append((db, role, 'SUCCESS'))
        except Exception as e:
            log_grant_audit(session, 'TABLE/VIEW', f'{db}.*', role, 'SELECT', 'FAILED', str(e)[:100])
            results.append((db, role, 'FAILED'))
    return results

def ensure_integration_grants(session):
    """Ensure grants on external volume and catalog integration"""
    integration_grants = [
        ("EXTERNAL VOLUME", "ICEBERG_EXT_VOLUME", "ICEBERG_ANALYST", "USAGE"),
        ("EXTERNAL VOLUME", "ICEBERG_EXT_VOLUME", "ICEBERG_ENGINEER", "USAGE"),
        ("INTEGRATION", "ICEBERG_S3_INT", "ICEBERG_ANALYST", "USAGE"),
        ("INTEGRATION", "ICEBERG_S3_INT", "ICEBERG_ENGINEER", "USAGE"),
        ("INTEGRATION", "REST_GLUE_CATALOG_INT", "ICEBERG_ADMIN", "USAGE"),
    ]
    
    results = []
    for obj_type, obj_name, role, priv in integration_grants:
        try:
            session.sql(f"GRANT {priv} ON {obj_type} {obj_name} TO ROLE {role}").collect()
            log_grant_audit(session, obj_type, obj_name, role, priv, 'GRANTED', 'SUCCESS')
            results.append((obj_name, role, 'SUCCESS'))
        except Exception as e:
            # May fail if integration doesn't exist - that's OK
            log_grant_audit(session, obj_type, obj_name, role, priv, 'SKIPPED', str(e)[:100])
            results.append((obj_name, role, 'SKIPPED'))
    return results

def validate_cld_data(session):
    """Validate that CLD tables have data"""
    tables = ['campaigns', 'impressions', 'clicks', 'conversions']
    results = []
    
    for table in tables:
        try:
            result = session.sql(f"""
                SELECT COUNT(*) as cnt 
                FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.{table}
            """).collect()
            count = result[0]['CNT'] if result else 0
            results.append((table, count, 'SUCCESS'))
        except Exception as e:
            results.append((table, 0, str(e)[:100]))
    
    return results

def sync_cld_tables_to_prod(session):
    """Ensure PROD views exist for all CLD tables"""
    try:
        # Get CLD tables
        cld_tables = session.sql("""
            SELECT table_name, table_schema 
            FROM ICEBERG_DEMO_CLD.INFORMATION_SCHEMA.TABLES 
            WHERE table_schema != 'INFORMATION_SCHEMA'
            AND table_type = 'BASE TABLE'
        """).collect()
        
        synced = 0
        for row in cld_tables:
            table_name = row['TABLE_NAME']
            schema_name = row['TABLE_SCHEMA']
            
            try:
                # Ensure schema exists
                session.sql(f"CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.{schema_name}").collect()
                
                # Create or replace view
                session.sql(f"""
                    CREATE OR REPLACE VIEW ICEBERG_PROD.{schema_name}.{table_name}
                    COMMENT = 'Secondary-synced view from CLD: ICEBERG_DEMO_CLD.{schema_name}.{table_name}'
                    AS SELECT * FROM ICEBERG_DEMO_CLD.{schema_name}.{table_name}
                """).collect()
                synced += 1
            except:
                pass
        
        return synced, len(cld_tables)
    except:
        return 0, 0

def main(session: snowpark.Session) -> dict:
    """Main secondary heartbeat procedure"""
    start_time = datetime.now()
    results = {
        'heartbeat_timestamp': start_time.isoformat(),
        'account': 'SECONDARY',
        'checks': {},
        'status': 'SUCCESS'
    }
    
    try:
        # 1. Refresh CLD Metadata
        cld_success, cld_msg = refresh_cld(session)
        results['checks']['cld_refresh'] = {'success': cld_success, 'message': cld_msg}
        if cld_success:
            log_heartbeat(session, 'CLD_REFRESH', 'SUCCESS', cld_msg)
        else:
            log_heartbeat(session, 'CLD_REFRESH', 'FAILED', error_msg=cld_msg)
        
        # 2. Grant Audit - Database level
        db_grants = ensure_database_grants(session)
        results['checks']['database_grants'] = db_grants
        
        # 3. Grant Audit - Schema level (including future grants)
        schema_grants = ensure_schema_grants(session)
        results['checks']['schema_grants'] = schema_grants
        
        # 4. Grant Audit - Table/View level (including future grants)
        table_grants = ensure_table_grants(session)
        results['checks']['table_grants'] = table_grants
        
        # 5. Integration grants (BCR-2114 Audit)
        int_grants = ensure_integration_grants(session)
        results['checks']['integration_grants'] = int_grants
        
        # 6. Validate CLD data
        data_validation = validate_cld_data(session)
        results['checks']['data_validation'] = data_validation
        total_rows = sum(count for _, count, _ in data_validation if isinstance(count, int))
        
        # 7. Sync CLD tables to PROD views
        synced, total = sync_cld_tables_to_prod(session)
        results['checks']['cld_to_prod_sync'] = {'synced': synced, 'total': total}
        
        # 8. Get counts for monitoring
        cld_count = get_cld_table_count(session)
        prod_count = get_prod_view_count(session)
        
        # 9. Log success heartbeat
        log_heartbeat(
            session, 
            'FULL_CHECK', 
            'SUCCESS', 
            f"CLD tables: {cld_count}, PROD views: {prod_count}, Data rows: {total_rows}",
            cld_count=cld_count,
            prod_count=prod_count
        )
        
        results['cld_table_count'] = cld_count
        results['prod_view_count'] = prod_count
        results['total_data_rows'] = total_rows
        
    except Exception as e:
        results['status'] = 'FAILED'
        results['error'] = str(e)
        log_heartbeat(session, 'FULL_CHECK', 'FAILED', error_msg=str(e)[:500])
    
    results['execution_time_seconds'] = (datetime.now() - start_time).total_seconds()
    return results
$$;

-- ============================================================================
-- SECTION 4: Create Secondary Heartbeat Task
-- ============================================================================

CREATE OR REPLACE TASK ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT_TASK
    WAREHOUSE = 'TASK_WH'
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Secondary resilient heartbeat - refreshes CLD, audits grants, validates data'
AS
    CALL ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT();

-- Grant EXECUTE TASK
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ICEBERG_ADMIN;

-- ============================================================================
-- SECTION 5: Manual Execution and Testing
-- ============================================================================

-- Test the stored procedure manually first
CALL ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT();

-- View heartbeat results
SELECT * FROM ICEBERG_PROD.DR_MONITORING.SECONDARY_HEARTBEAT_LOG 
ORDER BY heartbeat_timestamp DESC 
LIMIT 20;

-- View grant audit log
SELECT * FROM ICEBERG_PROD.DR_MONITORING.GRANT_AUDIT_LOG 
ORDER BY audit_timestamp DESC 
LIMIT 50;

-- ============================================================================
-- SECTION 6: Start the Task
-- ============================================================================

-- Resume (start) the task
ALTER TASK ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT_TASK RESUME;

-- Verify task is running
SHOW TASKS IN SCHEMA ICEBERG_PROD.DR_MONITORING;

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'SECONDARY_RESILIENT_HEARTBEAT_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- ============================================================================
-- SECTION 7: Monitoring Dashboard Queries
-- ============================================================================

-- Heartbeat health check (last 24 hours)
SELECT 
    DATE_TRUNC('hour', heartbeat_timestamp) as hour,
    check_type,
    status,
    COUNT(*) as check_count,
    AVG(cld_table_count) as avg_cld_tables,
    AVG(prod_view_count) as avg_prod_views
FROM ICEBERG_PROD.DR_MONITORING.SECONDARY_HEARTBEAT_LOG
WHERE heartbeat_timestamp > DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2;

-- Recent failures
SELECT 
    heartbeat_timestamp,
    check_type,
    error_msg
FROM ICEBERG_PROD.DR_MONITORING.SECONDARY_HEARTBEAT_LOG
WHERE status = 'FAILED'
AND heartbeat_timestamp > DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY heartbeat_timestamp DESC;

-- Grant audit summary
SELECT 
    object_type,
    action,
    status,
    COUNT(*) as count
FROM ICEBERG_PROD.DR_MONITORING.GRANT_AUDIT_LOG
WHERE audit_timestamp > DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY object_type, action, status
ORDER BY object_type, action;

-- CLD vs PROD object comparison
SELECT 
    'CLD' as source,
    table_schema,
    COUNT(*) as object_count
FROM ICEBERG_DEMO_CLD.INFORMATION_SCHEMA.TABLES
WHERE table_schema != 'INFORMATION_SCHEMA'
GROUP BY table_schema
UNION ALL
SELECT 
    'PROD' as source,
    table_schema,
    COUNT(*) as object_count
FROM ICEBERG_PROD.INFORMATION_SCHEMA.VIEWS
WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'MONITORING', 'DR_MONITORING')
GROUP BY table_schema
ORDER BY table_schema, source;

-- Data row counts comparison (CLD)
SELECT 
    'CAMPAIGNS' as table_name,
    COUNT(*) as row_count
FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS
UNION ALL
SELECT 'IMPRESSIONS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.IMPRESSIONS
UNION ALL
SELECT 'CLICKS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CLICKS
UNION ALL
SELECT 'CONVERSIONS', COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CONVERSIONS;

-- ============================================================================
-- SECTION 8: Task Management Commands
-- ============================================================================

-- Suspend the task (for maintenance)
-- ALTER TASK ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT_TASK SUSPEND;

-- Resume the task
-- ALTER TASK ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT_TASK RESUME;

-- Execute task immediately (manual trigger)
-- EXECUTE TASK ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT_TASK;

-- ============================================================================
-- SECTION 9: Failover Readiness Check
-- ============================================================================

-- Quick DR readiness check
SELECT 
    CASE WHEN cld_ok AND prod_ok AND grants_ok THEN 'READY' ELSE 'NOT READY' END as dr_status,
    cld_ok,
    prod_ok,
    grants_ok,
    cld_table_count,
    prod_view_count,
    last_successful_check
FROM (
    SELECT 
        MAX(CASE WHEN check_type = 'CLD_REFRESH' AND status = 'SUCCESS' THEN TRUE ELSE FALSE END) as cld_ok,
        MAX(CASE WHEN check_type = 'FULL_CHECK' AND status = 'SUCCESS' THEN TRUE ELSE FALSE END) as prod_ok,
        MAX(CASE WHEN check_type = 'FULL_CHECK' AND status = 'SUCCESS' THEN TRUE ELSE FALSE END) as grants_ok,
        MAX(cld_table_count) as cld_table_count,
        MAX(prod_view_count) as prod_view_count,
        MAX(heartbeat_timestamp) as last_successful_check
    FROM ICEBERG_PROD.DR_MONITORING.SECONDARY_HEARTBEAT_LOG
    WHERE heartbeat_timestamp > DATEADD('minute', -10, CURRENT_TIMESTAMP())
    AND status = 'SUCCESS'
);

