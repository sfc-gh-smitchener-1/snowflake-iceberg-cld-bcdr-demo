# Iceberg CLD BCDR Demo - Architecture Guide

## Overview

This document provides detailed architecture information for the Iceberg Catalog Linked Database (CLD) Business Continuity and Disaster Recovery (BCDR) demonstration.

## Key Architecture Principle

**Catalog Linked Databases (CLD) cannot be replicated via Snowflake failover groups.**

Each Snowflake account must create its own CLD independently, but both CLDs point to the **same** AWS Glue catalog and S3 storage. This shared-storage architecture means:

- No data duplication between accounts
- Instant failover (CLD is already connected to the data)
- Consistent data view across all accounts

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                  AWS ACCOUNT                                      │
│                                                                                   │
│   ┌─────────────────────────────────────────────────────────────────────────┐    │
│   │                         Amazon S3                                        │    │
│   │                                                                          │    │
│   │   s3://<bucket>/iceberg/warehouse/                                       │    │
│   │   ├── campaigns/                                                         │    │
│   │   │   ├── data/                   ← Parquet files                       │    │
│   │   │   └── metadata/               ← Iceberg metadata                    │    │
│   │   ├── impressions/                                                       │    │
│   │   ├── clicks/                                                            │    │
│   │   └── conversions/                                                       │    │
│   │                                                                          │    │
│   └─────────────────────────────────────────────────────────────────────────┘    │
│                                        │                                          │
│                                        │ Table Metadata                           │
│                                        ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────────────┐    │
│   │                      AWS Glue Data Catalog                               │    │
│   │                                                                          │    │
│   │   Database: iceberg_advertising_db                                       │    │
│   │   ├── campaigns      (Iceberg table)                                    │    │
│   │   ├── impressions    (Iceberg table)                                    │    │
│   │   ├── clicks         (Iceberg table)                                    │    │
│   │   └── conversions    (Iceberg table)                                    │    │
│   │                                                                          │    │
│   │   REST API: https://glue.<region>.amazonaws.com/iceberg                 │    │
│   │                                                                          │    │
│   └─────────────────────────────────────────────────────────────────────────┘    │
│                                        │                                          │
│                                        │ Credential Vending                       │
│                                        ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────────────┐    │
│   │                      AWS Lake Formation                                  │    │
│   │                                                                          │    │
│   │   • S3 Location Registration                                            │    │
│   │   • Database/Table Permissions                                          │    │
│   │   • External Engine Access: ENABLED ← Critical for CLD!                 │    │
│   │   • Credential Vending to BOTH Snowflake accounts                       │    │
│   │                                                                          │    │
│   └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                   │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
              ┌──────────────────────────┴──────────────────────────┐
              │                                                      │
              ▼                                                      ▼
┌─────────────────────────────────────────┐  ┌─────────────────────────────────────────┐
│       SNOWFLAKE PRIMARY ACCOUNT         │  │      SNOWFLAKE SECONDARY ACCOUNT        │
│                                         │  │                                         │
│ ┌─────────────────────────────────────┐ │  │ ┌─────────────────────────────────────┐ │
│ │         Account Objects             │ │  │ │     Replicated Account Objects      │ │
│ │                                     │ │  │ │                                     │ │
│ │ • AWS_ICEBERG_STORAGE_INT          │─┼──┼─▶ • AWS_ICEBERG_STORAGE_INT          │ │
│ │ • REST_GLUE_CATALOG_INT            │ │  │ │ • REST_GLUE_CATALOG_INT            │ │
│ │ • ICEBERG_EXT_VOLUME               │ │  │ │ • ICEBERG_EXT_VOLUME               │ │
│ │ • ICEBERG_ADMIN/ENGINEER/ANALYST   │ │  │ │ • ICEBERG_ADMIN/ENGINEER/ANALYST   │ │
│ └─────────────────────────────────────┘ │  │ └─────────────────────────────────────┘ │
│                                         │  │                                         │
│ ┌─────────────────────────────────────┐ │  │ ┌─────────────────────────────────────┐ │
│ │   ICEBERG_DEMO_EXT (External Tbls)  │ │  │ │   ICEBERG_DEMO_EXT (Replica)        │ │
│ │   ✓ REPLICATED via Failover Group  │─┼──┼─▶   ✓ REPLICATED via Failover Group  │ │
│ │                                     │ │  │ │                                     │ │
│ │   Schema: ADVERTISING               │ │  │ │   Schema: ADVERTISING               │ │
│ │   • EXT_CAMPAIGNS                   │ │  │ │   • EXT_CAMPAIGNS                   │ │
│ │   • EXT_IMPRESSIONS                 │ │  │ │   • EXT_IMPRESSIONS                 │ │
│ │   • EXT_CLICKS                      │ │  │ │   • EXT_CLICKS                      │ │
│ │   • EXT_CONVERSIONS                 │ │  │ │   • EXT_CONVERSIONS                 │ │
│ └─────────────────────────────────────┘ │  │ └─────────────────────────────────────┘ │
│                                         │  │                                         │
│ ┌─────────────────────────────────────┐ │  │ ┌─────────────────────────────────────┐ │
│ │   ICEBERG_DEMO_CLD                  │ │  │ │   ICEBERG_DEMO_CLD                  │ │
│ │   ✗ NOT REPLICATED                  │ │  │ │   ✗ NOT REPLICATED                  │ │
│ │   (created independently)           │ │  │ │   (created independently)           │ │
│ │                                     │ │  │ │                                     │ │
│ │   Schema: ICEBERG_ADVERTISING_DB    │ │  │ │   Schema: ICEBERG_ADVERTISING_DB    │ │
│ │   • CAMPAIGNS                       │─┼──┼──│   • CAMPAIGNS                       │ │
│ │   • IMPRESSIONS                     │ │  │ │   • IMPRESSIONS                     │ │
│ │   • CLICKS                          │ │  │ │   • CLICKS                          │ │
│ │   • CONVERSIONS                     │ │  │ │   • CONVERSIONS                     │ │
│ └────────────────┬────────────────────┘ │  │ └────────────────┬────────────────────┘ │
│                  │                      │  │                  │                      │
│ ┌────────────────┴────────────────────┐ │  │ ┌────────────────┴────────────────────┐ │
│ │        Failover Groups              │ │  │ │     Replica Failover Groups        │ │
│ │                                     │ │  │ │                                     │ │
│ │ • ICEBERG_BCDR_ACCOUNT_FG ─────────┼─┼──┼─▶ • ICEBERG_BCDR_ACCOUNT_FG         │ │
│ │ • ICEBERG_BCDR_VOLUME_FG ──────────┼─┼──┼─▶ • ICEBERG_BCDR_VOLUME_FG          │ │
│ │ • ICEBERG_BCDR_DB_FG ──────────────┼─┼──┼─▶ • ICEBERG_BCDR_DB_FG              │ │
│ │   (EXT only, NOT CLD)              │ │  │ │   (EXT only, NOT CLD)              │ │
│ └─────────────────────────────────────┘ │  │ └─────────────────────────────────────┘ │
│                                         │  │                                         │
│         [PRIMARY - Read/Write]          │  │       [SECONDARY - Read Only*]         │
└─────────────────────────────────────────┘  └─────────────────────────────────────────┘
              │                                                      │
              │     BOTH CLDs connect to SAME Glue catalog          │
              └──────────────────────────┬──────────────────────────┘
                                         │
                                         ▼
                              ┌────────────────────┐
                              │    AWS GLUE        │
                              │    CATALOG         │
                              │                    │
                              │  Same tables       │
                              │  Same data         │
                              │  Same metadata     │
                              └────────────────────┘
```

## Component Details

### AWS Components

#### Amazon S3
- **Purpose**: Stores the actual Iceberg table data (Parquet files) and metadata
- **Location**: `s3://<bucket>/iceberg/warehouse/`
- **Key Feature**: Single storage location accessed by BOTH Snowflake accounts
- **Access Method**: IAM role with trust relationship to both Snowflake accounts

#### AWS Glue Data Catalog
- **Purpose**: Central metastore for Iceberg table definitions
- **Database**: `iceberg_advertising_db`
- **Key Feature**: Provides table discovery and schema management
- **REST API**: `https://glue.<region>.amazonaws.com/iceberg`
- **Integration**: Both Snowflake accounts connect via REST Catalog Integration

#### AWS Lake Formation
- **Purpose**: Provides credential vending for Snowflake CLD on BOTH accounts
- **Key Settings**:
  - S3 location registered as data lake location
  - IAM role registered as data lake administrator
  - Database and table permissions granted
  - **External engine access ENABLED** (Application integration settings)
- **Multi-Account**: Must vend credentials to both primary and secondary Snowflake accounts

### Snowflake Components

#### Integrations (Replicated via Failover Groups)

| Integration | Type | Replication |
|-------------|------|-------------|
| `AWS_ICEBERG_STORAGE_INT` | Storage | ✓ Replicated |
| `REST_GLUE_CATALOG_INT` | Catalog (ICEBERG_REST) | ✓ Replicated |
| `ICEBERG_EXT_VOLUME` | External Volume | ✓ Replicated |

#### Databases

| Database | Replication | Notes |
|----------|-------------|-------|
| `ICEBERG_DEMO_EXT` | ✓ Replicated via failover group | External Tables approach |
| `ICEBERG_DEMO_CLD` | ✗ **NOT REPLICATED** | Created independently on each account |

#### Role Hierarchy (Replicated)

```
ACCOUNTADMIN
    └── ICEBERG_ADMIN
            │   • CREATE INTEGRATION
            │   • CREATE EXTERNAL VOLUME
            │   • CREATE DATABASE
            │   • CREATE FAILOVER GROUP
            │
            └── ICEBERG_ENGINEER
                    │   • CREATE SCHEMA
                    │   • CREATE TABLE
                    │   • CREATE VIEW
                    │
                    └── ICEBERG_ANALYST
                            │   • SELECT on tables
                            │   • USAGE on warehouse
```

### Failover Groups

| Failover Group | Object Types | Contents |
|----------------|--------------|----------|
| `ICEBERG_BCDR_ACCOUNT_FG` | ROLES, INTEGRATIONS | ICEBERG_ADMIN, ICEBERG_ENGINEER, ICEBERG_ANALYST, AWS_ICEBERG_STORAGE_INT, REST_GLUE_CATALOG_INT |
| `ICEBERG_BCDR_VOLUME_FG` | EXTERNAL VOLUMES | ICEBERG_EXT_VOLUME |
| `ICEBERG_BCDR_DB_FG` | DATABASES | **ICEBERG_DEMO_EXT only** (CLD not supported) |

## CLD BCDR Strategy

Since CLDs cannot be replicated, we use a **shared catalog** strategy:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CLD BCDR ARCHITECTURE                                                      │
│                                                                             │
│  PRIMARY ACCOUNT                    SECONDARY ACCOUNT                       │
│  ┌──────────────────────────┐      ┌──────────────────────────┐            │
│  │ ICEBERG_DEMO_CLD         │      │ ICEBERG_DEMO_CLD         │            │
│  │ (created via script 11)  │      │ (created via script 30)  │            │
│  │                          │      │                          │            │
│  │ Uses:                    │      │ Uses:                    │            │
│  │ REST_GLUE_CATALOG_INT    │      │ REST_GLUE_CATALOG_INT    │            │
│  │ (local)                  │      │ (replicated)             │            │
│  └────────────┬─────────────┘      └────────────┬─────────────┘            │
│               │                                 │                          │
│               │    ┌───────────────────────────┘                          │
│               │    │                                                       │
│               └────┴──────────────┐                                        │
│                                   ▼                                        │
│                   ┌───────────────────────────┐                            │
│                   │    AWS GLUE CATALOG       │                            │
│                   │    iceberg_advertising_db │                            │
│                   └─────────────┬─────────────┘                            │
│                                 │                                          │
│                   ┌─────────────▼─────────────┐                            │
│                   │        AMAZON S3          │                            │
│                   │    s3://bucket/iceberg/   │                            │
│                   └───────────────────────────┘                            │
│                                                                             │
│  RESULT: Both CLDs see the SAME data at all times!                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Failover Behavior

| Component | During Failover | Action Required |
|-----------|-----------------|-----------------|
| External Tables DB | Promote failover group | `ALTER FAILOVER GROUP ... PRIMARY` |
| CLD | Already working | None - already connected to Glue |
| Integrations | Promote failover group | `ALTER FAILOVER GROUP ... PRIMARY` |
| External Volume | Promote failover group | `ALTER FAILOVER GROUP ... PRIMARY` |

**Key Insight**: The CLD on secondary is **already operational** because it's independently connected to the same Glue catalog. During failover, applications simply redirect to the secondary account - the CLD is ready immediately.

## Data Flow

### Normal Operations (Primary Active)

```
1. Application → Snowflake Primary
2. Primary CLD → Glue REST API (metadata)
3. Primary → Lake Formation (credential vending)
4. Primary → S3 (data)
5. Failover Groups → Secondary (replicate EXT DB, integrations, roles)
6. Secondary CLD → Same Glue catalog (independent connection)
```

### During Failover

```
1. Detect primary failure
2. Promote failover groups on secondary:
   - ALTER FAILOVER GROUP ICEBERG_BCDR_ACCOUNT_FG PRIMARY;
   - ALTER FAILOVER GROUP ICEBERG_BCDR_VOLUME_FG PRIMARY;
   - ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG PRIMARY;
3. Application → Snowflake Secondary
4. Secondary CLD → Same Glue Catalog (already connected)
5. Secondary → Same S3 data
6. ✓ Failover complete - no data movement required
```

## Security Model

### IAM Trust Relationship

For CLD to work on BOTH accounts, the IAM role must trust:
- Primary account's Snowflake IAM user
- Secondary account's Snowflake IAM user (different ARN!)
- Lake Formation service principal

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "<PRIMARY_SNOWFLAKE_IAM_USER_ARN>",
          "<SECONDARY_SNOWFLAKE_IAM_USER_ARN>"
        ]
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lakeformation.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Important**: Each Snowflake account has a different IAM user ARN. Get these from:
```sql
-- On PRIMARY:
DESCRIBE INTEGRATION AWS_ICEBERG_STORAGE_INT;

-- On SECONDARY:
DESCRIBE INTEGRATION AWS_ICEBERG_STORAGE_INT;
```

### Required IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:GetObjectVersion", "s3:PutObject",
        "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable",
        "glue:GetTables", "glue:GetTableVersion", "glue:GetTableVersions",
        "glue:GetPartitions", "glue:BatchGetPartition"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/*",
        "arn:aws:glue:<region>:<account>:table/*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "lakeformation:GetDataAccess",
      "Resource": "*"
    }
  ]
}
```

## Comparison: External Tables vs CLD

| Aspect | External Tables | Catalog Linked Database |
|--------|-----------------|------------------------|
| Table Discovery | Manual creation | Automatic sync |
| Schema Updates | Manual ALTER | Automatic on refresh |
| Naming | Custom (EXT_ prefix) | Matches Glue |
| Catalog Integration | GLUE type | ICEBERG_REST type |
| Credential Model | Direct IAM | Lake Formation vending |
| Lake Formation | Not required | **Required** |
| **Failover Group** | **✓ Can replicate** | **✗ Cannot replicate** |
| BCDR Strategy | Replicate database | Create independently |
| Failover Speed | After promotion | Instant (already connected) |

## Recovery Objectives

### Recovery Point Objective (RPO)

| Component | RPO | Reason |
|-----------|-----|--------|
| Iceberg Data | 0 | Shared S3 storage |
| Glue Metadata | 0 | Shared catalog |
| External Tables DB | ~10 min | Failover group replication |
| CLD | 0 | Independent, same catalog |
| Roles/Integrations | ~10 min | Failover group replication |

### Recovery Time Objective (RTO)

| Scenario | RTO | Steps |
|----------|-----|-------|
| Planned Failover | < 5 min | Promote failover groups |
| Unplanned Failover | < 10 min | Detect + Promote |
| CLD Access | Instant | Already connected |

## Best Practices

1. **Test Failover Regularly**: Schedule quarterly DR drills
2. **Monitor Replication**: Alert on lag > 30 minutes
3. **Verify Secondary CLD**: Regularly test queries on secondary CLD
4. **Keep Trust Policy Updated**: When integrations change, update IAM trust policy for both accounts
5. **Document Runbooks**: Create step-by-step failover procedures
6. **Dual Validation**: Test both EXT and CLD after changes
7. **Lake Formation Audit**: Review permissions quarterly
