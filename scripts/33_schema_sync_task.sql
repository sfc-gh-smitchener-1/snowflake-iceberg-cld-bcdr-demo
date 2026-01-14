/*******************************************************************************
 * ICEBERG CLD BCDR DEMO
 * Script: 33_schema_sync_task.sql
 * Purpose: Daily schema drift detection and optional sync between accounts
 *
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  SCHEMA DRIFT DETECTION                                                   ║
 * ║                                                                           ║
 * ║  Since ICEBERG_PROD is now independent on both accounts, we need a       ║
 * ║  mechanism to detect when schema definitions drift between them.          ║
 * ║                                                                           ║
 * ║  This task runs DAILY and:                                                ║
 * ║  1. Compares object counts between primary and secondary                  ║
 * ║  2. Logs any drift detected                                               ║
 * ║  3. Optionally syncs missing objects from primary                         ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 *
 * ARCHITECTURE:
 * - PRIMARY publishes schema metadata to a shared location (S3 or stage)
 * - SECONDARY reads the metadata and compares with local objects
 * - Drift is logged; manual review recommended before auto-sync
 *
 * Prerequisites:
 *   - Migration complete (script 32)
 *   - Both accounts have independent ICEBERG_PROD databases
 *   - Shared stage or storage location accessible from both accounts
 *
 ******************************************************************************/

-- ============================================================================
-- PART A: RUN ON PRIMARY ACCOUNT
-- ============================================================================

/*******************************************************************************
 * STEP 1: Create schema metadata export procedure on PRIMARY
 ******************************************************************************/

USE ROLE ICEBERG_ADMIN;
USE DATABASE ICEBERG_PROD;
USE WAREHOUSE ICEBERG_DEMO_WH;

-- Create schema for sync metadata
CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.SCHEMA_SYNC
    COMMENT = 'Schema synchronization metadata for BCDR';

-- Create table to store current schema state
CREATE OR REPLACE TABLE ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA (
    export_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    account_name VARCHAR(100),
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    object_type VARCHAR(50),        -- 'VIEW', 'TABLE', 'PROCEDURE', 'TASK', 'STREAM'
    object_name VARCHAR(255),
    object_definition VARCHAR(100000),
    object_comment VARCHAR(4000),
    created_at TIMESTAMP_NTZ,
    last_altered TIMESTAMP_NTZ,
    object_hash VARCHAR(64)         -- SHA256 of definition for comparison
);

-- Procedure to export current schema state
CREATE OR REPLACE PROCEDURE ICEBERG_PROD.SCHEMA_SYNC.EXPORT_SCHEMA_METADATA()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Clear previous export
    DELETE FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
    WHERE account_name = CURRENT_ACCOUNT();
    
    -- Export views
    INSERT INTO ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
        (account_name, database_name, schema_name, object_type, object_name, 
         object_definition, object_comment, created_at, last_altered, object_hash)
    SELECT 
        CURRENT_ACCOUNT(),
        table_catalog,
        table_schema,
        'VIEW',
        table_name,
        view_definition,
        comment,
        created,
        last_altered,
        SHA2(view_definition, 256)
    FROM ICEBERG_PROD.INFORMATION_SCHEMA.VIEWS
    WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'SCHEMA_SYNC');
    
    -- Export procedures
    INSERT INTO ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
        (account_name, database_name, schema_name, object_type, object_name, 
         object_definition, object_comment, created_at, last_altered, object_hash)
    SELECT 
        CURRENT_ACCOUNT(),
        procedure_catalog,
        procedure_schema,
        'PROCEDURE',
        procedure_name,
        procedure_definition,
        comment,
        created,
        last_altered,
        SHA2(procedure_definition, 256)
    FROM ICEBERG_PROD.INFORMATION_SCHEMA.PROCEDURES
    WHERE procedure_schema NOT IN ('INFORMATION_SCHEMA', 'SCHEMA_SYNC');
    
    -- Export tasks
    INSERT INTO ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
        (account_name, database_name, schema_name, object_type, object_name, 
         object_definition, object_comment, created_at, last_altered, object_hash)
    SELECT 
        CURRENT_ACCOUNT(),
        database_name,
        schema_name,
        'TASK',
        name,
        definition,
        NULL,
        created_on,
        NULL,
        SHA2(definition, 256)
    FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
        TASK_NAME => 'ICEBERG_PROD.DR_MONITORING.SECONDARY_RESILIENT_HEARTBEAT_TASK',
        RECURSIVE => TRUE
    ));
    
    RETURN 'Schema metadata exported for account: ' || CURRENT_ACCOUNT();
END;
$$;

-- Create a task to export schema metadata daily (runs at 2 AM)
CREATE OR REPLACE TASK ICEBERG_PROD.SCHEMA_SYNC.EXPORT_SCHEMA_METADATA_TASK
    WAREHOUSE = 'ICEBERG_DEMO_WH'
    SCHEDULE = 'USING CRON 0 2 * * * America/Los_Angeles'
    COMMENT = 'Daily export of schema metadata for BCDR sync'
AS
    CALL ICEBERG_PROD.SCHEMA_SYNC.EXPORT_SCHEMA_METADATA();

-- Resume the task
ALTER TASK ICEBERG_PROD.SCHEMA_SYNC.EXPORT_SCHEMA_METADATA_TASK RESUME;

-- Manual run for initial export
CALL ICEBERG_PROD.SCHEMA_SYNC.EXPORT_SCHEMA_METADATA();

-- View exported metadata
SELECT * FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA ORDER BY object_type, object_name;


-- ============================================================================
-- PART B: RUN ON SECONDARY ACCOUNT
-- ============================================================================

/*******************************************************************************
 * STEP 2: Create schema sync infrastructure on SECONDARY
 ******************************************************************************/

USE ROLE ICEBERG_ADMIN;
USE DATABASE ICEBERG_PROD;
USE WAREHOUSE ICEBERG_DEMO_WH;

-- Create schema for sync metadata (if not exists from migration)
CREATE SCHEMA IF NOT EXISTS ICEBERG_PROD.SCHEMA_SYNC
    COMMENT = 'Schema synchronization metadata for BCDR';

-- Create local schema metadata table
CREATE OR REPLACE TABLE ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA (
    export_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    account_name VARCHAR(100),
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    object_type VARCHAR(50),
    object_name VARCHAR(255),
    object_definition VARCHAR(100000),
    object_comment VARCHAR(4000),
    created_at TIMESTAMP_NTZ,
    last_altered TIMESTAMP_NTZ,
    object_hash VARCHAR(64)
);

-- Create table to store PRIMARY's schema metadata (copied manually or via shared storage)
CREATE OR REPLACE TABLE ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA (
    export_timestamp TIMESTAMP_NTZ,
    account_name VARCHAR(100),
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    object_type VARCHAR(50),
    object_name VARCHAR(255),
    object_definition VARCHAR(100000),
    object_comment VARCHAR(4000),
    created_at TIMESTAMP_NTZ,
    last_altered TIMESTAMP_NTZ,
    object_hash VARCHAR(64),
    sync_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create drift log table
CREATE TABLE IF NOT EXISTS ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_DRIFT_LOG (
    drift_id NUMBER AUTOINCREMENT,
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    object_type VARCHAR(50),
    schema_name VARCHAR(100),
    object_name VARCHAR(255),
    drift_type VARCHAR(50),
    primary_hash VARCHAR(64),
    secondary_hash VARCHAR(64),
    primary_definition VARCHAR(100000),
    secondary_definition VARCHAR(100000),
    status VARCHAR(20) DEFAULT 'DETECTED',
    resolved_at TIMESTAMP_NTZ,
    resolved_by VARCHAR(100),
    PRIMARY KEY (drift_id)
);

/*******************************************************************************
 * STEP 3: Create Python procedure for schema drift detection
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE ICEBERG_PROD.SCHEMA_SYNC.DETECT_SCHEMA_DRIFT()
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

def export_local_schema(session):
    """Export current secondary schema metadata"""
    try:
        # Clear previous export
        session.sql("""
            DELETE FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
            WHERE account_name = CURRENT_ACCOUNT()
        """).collect()
        
        # Export views
        session.sql("""
            INSERT INTO ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
                (account_name, database_name, schema_name, object_type, object_name, 
                 object_definition, object_comment, created_at, last_altered, object_hash)
            SELECT 
                CURRENT_ACCOUNT(),
                table_catalog,
                table_schema,
                'VIEW',
                table_name,
                view_definition,
                comment,
                created,
                last_altered,
                SHA2(view_definition, 256)
            FROM ICEBERG_PROD.INFORMATION_SCHEMA.VIEWS
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'SCHEMA_SYNC')
        """).collect()
        
        # Export procedures
        session.sql("""
            INSERT INTO ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
                (account_name, database_name, schema_name, object_type, object_name, 
                 object_definition, object_comment, created_at, last_altered, object_hash)
            SELECT 
                CURRENT_ACCOUNT(),
                procedure_catalog,
                procedure_schema,
                'PROCEDURE',
                procedure_name,
                procedure_definition,
                comment,
                created,
                last_altered,
                SHA2(procedure_definition, 256)
            FROM ICEBERG_PROD.INFORMATION_SCHEMA.PROCEDURES
            WHERE procedure_schema NOT IN ('INFORMATION_SCHEMA', 'SCHEMA_SYNC')
        """).collect()
        
        return True
    except Exception as e:
        return False

def detect_drift(session):
    """Compare primary and secondary schema metadata"""
    drift_results = []
    
    try:
        # Find objects missing on secondary (exist on primary but not secondary)
        missing_on_secondary = session.sql("""
            SELECT 
                p.object_type,
                p.schema_name,
                p.object_name,
                p.object_definition,
                p.object_hash
            FROM ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA p
            LEFT JOIN ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA s
                ON p.object_type = s.object_type 
                AND p.schema_name = s.schema_name 
                AND p.object_name = s.object_name
            WHERE s.object_name IS NULL
        """).collect()
        
        for row in missing_on_secondary:
            drift_results.append({
                'object_type': row['OBJECT_TYPE'],
                'schema_name': row['SCHEMA_NAME'],
                'object_name': row['OBJECT_NAME'],
                'drift_type': 'MISSING_ON_SECONDARY',
                'primary_hash': row['OBJECT_HASH'],
                'secondary_hash': None,
                'primary_definition': row['OBJECT_DEFINITION'][:5000] if row['OBJECT_DEFINITION'] else None
            })
            
            # Log drift
            session.sql(f"""
                INSERT INTO ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_DRIFT_LOG
                    (object_type, schema_name, object_name, drift_type, primary_hash, primary_definition)
                VALUES (
                    '{row['OBJECT_TYPE']}',
                    '{row['SCHEMA_NAME']}',
                    '{row['OBJECT_NAME']}',
                    'MISSING_ON_SECONDARY',
                    '{row['OBJECT_HASH']}',
                    $${row['OBJECT_DEFINITION'][:5000] if row['OBJECT_DEFINITION'] else ''}$$
                )
            """).collect()
        
        # Find objects with different definitions
        definition_mismatch = session.sql("""
            SELECT 
                p.object_type,
                p.schema_name,
                p.object_name,
                p.object_hash AS primary_hash,
                s.object_hash AS secondary_hash,
                p.object_definition AS primary_definition,
                s.object_definition AS secondary_definition
            FROM ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA p
            JOIN ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA s
                ON p.object_type = s.object_type 
                AND p.schema_name = s.schema_name 
                AND p.object_name = s.object_name
            WHERE p.object_hash != s.object_hash
        """).collect()
        
        for row in definition_mismatch:
            drift_results.append({
                'object_type': row['OBJECT_TYPE'],
                'schema_name': row['SCHEMA_NAME'],
                'object_name': row['OBJECT_NAME'],
                'drift_type': 'DEFINITION_MISMATCH',
                'primary_hash': row['PRIMARY_HASH'],
                'secondary_hash': row['SECONDARY_HASH']
            })
            
            # Log drift
            session.sql(f"""
                INSERT INTO ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_DRIFT_LOG
                    (object_type, schema_name, object_name, drift_type, 
                     primary_hash, secondary_hash, primary_definition, secondary_definition)
                VALUES (
                    '{row['OBJECT_TYPE']}',
                    '{row['SCHEMA_NAME']}',
                    '{row['OBJECT_NAME']}',
                    'DEFINITION_MISMATCH',
                    '{row['PRIMARY_HASH']}',
                    '{row['SECONDARY_HASH']}',
                    $${row['PRIMARY_DEFINITION'][:5000] if row['PRIMARY_DEFINITION'] else ''}$$,
                    $${row['SECONDARY_DEFINITION'][:5000] if row['SECONDARY_DEFINITION'] else ''}$$
                )
            """).collect()
        
        # Find objects missing on primary (exist on secondary but not primary)
        missing_on_primary = session.sql("""
            SELECT 
                s.object_type,
                s.schema_name,
                s.object_name,
                s.object_hash
            FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA s
            LEFT JOIN ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA p
                ON p.object_type = s.object_type 
                AND p.schema_name = s.schema_name 
                AND p.object_name = s.object_name
            WHERE p.object_name IS NULL
            AND s.schema_name NOT IN ('SCHEMA_SYNC', 'DR_MONITORING')
        """).collect()
        
        for row in missing_on_primary:
            drift_results.append({
                'object_type': row['OBJECT_TYPE'],
                'schema_name': row['SCHEMA_NAME'],
                'object_name': row['OBJECT_NAME'],
                'drift_type': 'MISSING_ON_PRIMARY',
                'secondary_hash': row['OBJECT_HASH']
            })
        
        return drift_results
    except Exception as e:
        return [{'error': str(e)}]

def sync_missing_objects(session, drift_results):
    """Optionally sync missing objects from primary to secondary"""
    synced = []
    
    for drift in drift_results:
        if drift.get('drift_type') == 'MISSING_ON_SECONDARY' and drift.get('primary_definition'):
            obj_type = drift['object_type']
            schema = drift['schema_name']
            name = drift['object_name']
            definition = drift['primary_definition']
            
            try:
                if obj_type == 'VIEW':
                    # Extract just the SELECT part and recreate the view
                    # Note: This is simplified; real implementation would need to parse DDL
                    session.sql(f"""
                        CREATE OR REPLACE VIEW ICEBERG_PROD.{schema}.{name} AS {definition}
                    """).collect()
                    synced.append(f"{obj_type}: {schema}.{name}")
                    
                    # Mark as resolved
                    session.sql(f"""
                        UPDATE ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_DRIFT_LOG
                        SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP()
                        WHERE object_name = '{name}' 
                        AND schema_name = '{schema}'
                        AND status = 'DETECTED'
                    """).collect()
            except Exception as e:
                # Log sync failure
                pass
    
    return synced

def main(session: snowpark.Session) -> dict:
    """Main schema drift detection procedure"""
    start_time = datetime.now()
    results = {
        'check_timestamp': start_time.isoformat(),
        'account': 'SECONDARY',
        'status': 'SUCCESS'
    }
    
    try:
        # Export local schema
        export_local_schema(session)
        
        # Check if we have primary metadata to compare
        primary_count = session.sql("""
            SELECT COUNT(*) as cnt FROM ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA
        """).collect()[0]['CNT']
        
        if primary_count == 0:
            results['warning'] = 'No primary schema metadata found. Please sync PRIMARY_SCHEMA_METADATA table.'
            results['drift_detected'] = False
            return results
        
        # Detect drift
        drift_results = detect_drift(session)
        
        results['drift_detected'] = len(drift_results) > 0
        results['drift_count'] = len(drift_results)
        results['drift_summary'] = {
            'missing_on_secondary': len([d for d in drift_results if d.get('drift_type') == 'MISSING_ON_SECONDARY']),
            'missing_on_primary': len([d for d in drift_results if d.get('drift_type') == 'MISSING_ON_PRIMARY']),
            'definition_mismatch': len([d for d in drift_results if d.get('drift_type') == 'DEFINITION_MISMATCH'])
        }
        
        # Get object counts for comparison
        local_counts = session.sql("""
            SELECT object_type, COUNT(*) as cnt 
            FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
            GROUP BY object_type
        """).collect()
        
        primary_counts = session.sql("""
            SELECT object_type, COUNT(*) as cnt 
            FROM ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA 
            GROUP BY object_type
        """).collect()
        
        results['object_counts'] = {
            'secondary': {row['OBJECT_TYPE']: row['CNT'] for row in local_counts},
            'primary': {row['OBJECT_TYPE']: row['CNT'] for row in primary_counts}
        }
        
    except Exception as e:
        results['status'] = 'FAILED'
        results['error'] = str(e)
    
    results['execution_time_seconds'] = (datetime.now() - start_time).total_seconds()
    return results
$$;

/*******************************************************************************
 * STEP 4: Create daily schema sync task
 ******************************************************************************/

CREATE OR REPLACE TASK ICEBERG_PROD.SCHEMA_SYNC.DETECT_SCHEMA_DRIFT_TASK
    WAREHOUSE = 'ICEBERG_DEMO_WH'
    SCHEDULE = 'USING CRON 0 3 * * * America/Los_Angeles'  -- 3 AM daily
    COMMENT = 'Daily schema drift detection between primary and secondary'
AS
    CALL ICEBERG_PROD.SCHEMA_SYNC.DETECT_SCHEMA_DRIFT();

-- Resume the task
ALTER TASK ICEBERG_PROD.SCHEMA_SYNC.DETECT_SCHEMA_DRIFT_TASK RESUME;

/*******************************************************************************
 * STEP 5: Manual sync of primary metadata
 * 
 * Since cross-account queries aren't directly possible, you have two options:
 * 
 * OPTION A: Manual export/import via stage
 * OPTION B: Use Snowflake data sharing (if available)
 * OPTION C: Export to S3 and import
 * 
 * Below is Option A - manual copy
 ******************************************************************************/

-- ON PRIMARY: Export to a stage
/*
CREATE OR REPLACE STAGE ICEBERG_PROD.SCHEMA_SYNC.METADATA_STAGE
    DIRECTORY = (ENABLE = TRUE);

COPY INTO @ICEBERG_PROD.SCHEMA_SYNC.METADATA_STAGE/schema_metadata.csv
FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA
FILE_FORMAT = (TYPE = CSV HEADER = TRUE);
*/

-- ON SECONDARY: Import from stage (after copying file)
/*
COPY INTO ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA
FROM @ICEBERG_PROD.SCHEMA_SYNC.METADATA_STAGE/schema_metadata.csv
FILE_FORMAT = (TYPE = CSV HEADER = TRUE);
*/

/*******************************************************************************
 * STEP 6: Monitoring queries
 ******************************************************************************/

-- View recent drift detections
SELECT * FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_DRIFT_LOG
ORDER BY check_timestamp DESC
LIMIT 50;

-- Summary of unresolved drift
SELECT 
    drift_type,
    object_type,
    COUNT(*) as count
FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_DRIFT_LOG
WHERE status = 'DETECTED'
GROUP BY drift_type, object_type
ORDER BY count DESC;

-- Object count comparison
SELECT 
    COALESCE(p.object_type, s.object_type) AS object_type,
    COALESCE(p.cnt, 0) AS primary_count,
    COALESCE(s.cnt, 0) AS secondary_count,
    COALESCE(p.cnt, 0) - COALESCE(s.cnt, 0) AS difference
FROM (
    SELECT object_type, COUNT(*) as cnt 
    FROM ICEBERG_PROD.SCHEMA_SYNC.PRIMARY_SCHEMA_METADATA 
    GROUP BY object_type
) p
FULL OUTER JOIN (
    SELECT object_type, COUNT(*) as cnt 
    FROM ICEBERG_PROD.SCHEMA_SYNC.SCHEMA_METADATA 
    GROUP BY object_type
) s ON p.object_type = s.object_type
ORDER BY object_type;

-- Manual run of drift detection
CALL ICEBERG_PROD.SCHEMA_SYNC.DETECT_SCHEMA_DRIFT();

/*******************************************************************************
 * SCHEMA SYNC SUMMARY:
 * 
 * DAILY WORKFLOW:
 * 1. PRIMARY (2 AM): EXPORT_SCHEMA_METADATA_TASK runs, exports current state
 * 2. (Manual/Automated): Copy PRIMARY_SCHEMA_METADATA to SECONDARY
 * 3. SECONDARY (3 AM): DETECT_SCHEMA_DRIFT_TASK runs, compares and logs drift
 * 
 * DRIFT TYPES:
 * - MISSING_ON_SECONDARY: Object exists on primary but not secondary
 * - MISSING_ON_PRIMARY: Object exists on secondary but not primary
 * - DEFINITION_MISMATCH: Object exists on both but definitions differ
 * 
 * RESOLUTION:
 * - Review drift logs daily
 * - Manually sync critical objects
 * - Mark resolved after verification
 * 
 ******************************************************************************/
