# Iceberg CLD BCDR Demo

Business Continuity and Disaster Recovery (BCDR) demonstration using Apache Iceberg tables managed by AWS Glue Catalog, integrated with Snowflake via both External Tables and Catalog Linked Database (CLD) approaches, with cross-account failover capabilities.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS ACCOUNT                                     │
│  ┌─────────────────────┐         ┌─────────────────────────────────┐        │
│  │   AWS Glue Catalog  │◄───────►│        Amazon S3 Bucket         │        │
│  │  (Iceberg Tables)   │         │  s3://<bucket>/iceberg/         │        │
│  └─────────┬───────────┘         └─────────────────────────────────┘        │
│            │                                                                 │
│  ┌─────────▼───────────┐                                                    │
│  │   Lake Formation    │  ← Credential Vending for External Engines         │
│  └─────────────────────┘                                                    │
└─────────────┼───────────────────────────────────────────────────────────────┘
              │
              │ Both accounts connect to SAME Glue catalog
              │
┌─────────────┴───────────────────────────────────────────────────────────────┐
│                                                                              │
│   ┌─────────────────────────────┐      ┌─────────────────────────────┐     │
│   │    SNOWFLAKE PRIMARY        │      │    SNOWFLAKE SECONDARY      │     │
│   │                             │      │                             │     │
│   │  ┌───────────────────────┐  │      │  ┌───────────────────────┐  │     │
│   │  │ ICEBERG_DEMO_EXT      │  │      │  │ ICEBERG_DEMO_EXT      │  │     │
│   │  │ (replicated via FG)   │──┼──────┼─►│ (replica)             │  │     │
│   │  └───────────────────────┘  │      │  └───────────────────────┘  │     │
│   │                             │      │                             │     │
│   │  ┌───────────────────────┐  │      │  ┌───────────────────────┐  │     │
│   │  │ ICEBERG_DEMO_CLD      │  │      │  │ ICEBERG_DEMO_CLD      │  │     │
│   │  │ (independent)         │──┼──┐   │  │ (independent)         │  │     │
│   │  └───────────────────────┘  │  │   │  └───────────────────────┘  │     │
│   │                             │  │   │              │              │     │
│   └─────────────────────────────┘  │   └──────────────┼──────────────┘     │
│                                    │                  │                     │
│                                    │   ┌──────────────┘                     │
│                                    │   │                                    │
│                                    └───┴─► Both CLDs point to SAME         │
│                                            Glue catalog (not replicated)   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Key Concept: CLD Cannot Be Replicated

**Important:** Catalog Linked Databases (CLD) **cannot** be included in Snowflake failover groups. Instead:

1. **External Tables** (`ICEBERG_DEMO_EXT`) → Replicated via failover groups
2. **CLD** (`ICEBERG_DEMO_CLD`) → Created independently on each account, both pointing to the same Glue catalog

This architecture actually provides benefits:
- Zero data duplication (both CLDs read from the same S3 location)
- Instant failover for CLD (already connected to the data)
- Consistent data view across accounts

## What You Will Build

1. **AWS Glue-managed Iceberg Tables**: Sample advertising data (campaigns, impressions, clicks, conversions) stored in S3 as Iceberg tables
2. **Snowflake Role Hierarchy**:
   - `ICEBERG_ADMIN`: Creates account-level objects (integrations, external volumes)
   - `ICEBERG_ENGINEER`: Creates and manages database objects (schemas, tables)
   - `ICEBERG_ANALYST`: Read-only access to query data
3. **Dual Access Patterns**:
   - **External Tables**: Traditional approach using `ICEBERG_DEMO_EXT` database (replicated)
   - **Catalog Linked Database (CLD)**: Modern approach using `ICEBERG_DEMO_CLD` database (independent per account)
4. **BCDR Configuration**: Failover groups for account objects, external volumes, and External Tables database
5. **Secondary Account Setup**: Scripts to configure the secondary account and create its own CLD

## Contents

| File | Description |
|------|-------------|
| `scripts/00_prereqs_rbac.sql` | Role hierarchy, warehouse, and grants |
| `scripts/01_aws_integrations.sql` | Storage integration, catalog integration, external volume |
| `scripts/02_generate_iceberg_data.py` | Python script to generate sample advertising data |
| `scripts/03_load_iceberg_aws.py` | Load data into S3 (legacy approach) |
| `scripts/04_create_glue_iceberg_tables.py` | Create Iceberg tables in Glue using PyIceberg |
| `scripts/10_external_tables.sql` | Create external Iceberg tables (traditional approach) |
| `scripts/11_catalog_linked_database.sql` | Create Catalog Linked Database (modern approach) |
| `scripts/20_failover_groups_primary.sql` | Configure failover groups on primary account |
| `scripts/21_failover_groups_secondary.sql` | Configure secondary account for replication |
| `scripts/30_cld_secondary_setup.sql` | **Create CLD on secondary** (required - CLD not replicated) |
| `scripts/90_validation_queries.sql` | Queries to validate the setup |
| `scripts/99_cleanup.sql` | Cleanup scripts |
| `docs/ARCHITECTURE.md` | Detailed architecture documentation |
| `docs/LAKE_FORMATION_SETUP.md` | **Critical AWS Lake Formation setup guide** |
| `docs/TALK_TRACK.md` | Demo talk track and key points |
| `data/` | Generated sample data (CSV format) |

## Prerequisites

### Snowflake
- Two Snowflake accounts in the same organization (for BCDR replication)
- `ACCOUNTADMIN` role access on both accounts
- Enterprise Edition or higher (required for failover groups)

### AWS
- AWS Account with:
  - S3 bucket for Iceberg data storage
  - AWS Glue Data Catalog access
  - **AWS Lake Formation configured** (required for CLD)
  - IAM role with appropriate permissions for Snowflake integration
- AWS CLI configured locally

### Local Environment
- Python 3.10+
- AWS CLI configured with appropriate credentials
- SnowSQL CLI (optional, for command-line execution)

## Quick Start

### Step 1: Configure Environment Variables

Copy `env.example` to `.env` and update with your values:

```bash
cp env.example .env
```

### Step 2: Install Python Dependencies

```bash
cd Iceberg_CLD_BCDR_Demo
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 3: Set Up AWS (One-Time)

#### 3a. Generate Sample Data

```bash
python scripts/02_generate_iceberg_data.py --output-dir ./data
```

#### 3b. Create Iceberg Tables in Glue

```bash
python scripts/04_create_glue_iceberg_tables.py \
    --data-dir ./data \
    --bucket $AWS_S3_BUCKET \
    --database iceberg_advertising_db \
    --region $AWS_REGION
```

#### 3c. Configure Lake Formation (CRITICAL!)

See [docs/LAKE_FORMATION_SETUP.md](docs/LAKE_FORMATION_SETUP.md) for detailed instructions.

Key steps:
1. Register S3 location in Lake Formation
2. Grant database and table permissions to IAM role
3. **Enable "Allow external engines to access data in Amazon S3 locations with full table access"**
4. Add Lake Formation service to IAM role trust policy

### Step 4: Set Up Snowflake Primary Account

Run scripts in Snowsight:

```sql
-- 1. Create roles, warehouse, and grants (scripts/00_prereqs_rbac.sql)
-- 2. Create AWS integrations (scripts/01_aws_integrations.sql)
-- 3. Configure AWS IAM trust policy with values from DESCRIBE INTEGRATION
-- 4. Create External Tables database (scripts/10_external_tables.sql)
-- 5. Create Catalog Linked Database (scripts/11_catalog_linked_database.sql)
-- 6. Validate setup (scripts/90_validation_queries.sql)
```

### Step 5: Configure BCDR (Failover Groups)

```sql
-- On PRIMARY account: Create failover groups
-- (scripts/20_failover_groups_primary.sql)
-- Note: Only External Tables DB is replicated, NOT CLD

-- On SECONDARY account: Enable replication
-- (scripts/21_failover_groups_secondary.sql)
```

### Step 6: Create CLD on Secondary (REQUIRED!)

```sql
-- On SECONDARY account: Create CLD pointing to same Glue catalog
-- (scripts/30_cld_secondary_setup.sql)
-- This step is REQUIRED because CLDs cannot be replicated via failover groups
```

## Data Model

### Advertising Schema

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `CAMPAIGNS` | Advertising campaign definitions | campaign_id, name, channel, budget, start_date, end_date |
| `IMPRESSIONS` | Ad impression events | impression_id, campaign_id, timestamp, device_type, geo |
| `CLICKS` | Click-through events | click_id, impression_id, campaign_id, timestamp |
| `CONVERSIONS` | Conversion/purchase events | conversion_id, click_id, campaign_id, revenue, timestamp |

### Naming Conventions

| Object Type | External Tables Pattern | CLD Pattern |
|-------------|------------------------|-------------|
| Database | `ICEBERG_DEMO_EXT` | `ICEBERG_DEMO_CLD` |
| Schema | `ADVERTISING` | `ICEBERG_ADVERTISING_DB` (mirrors Glue) |
| Tables | `EXT_CAMPAIGNS`, `EXT_IMPRESSIONS`, etc. | `CAMPAIGNS`, `IMPRESSIONS`, etc. |

## Key Concepts

### External Tables vs Catalog Linked Database

| Feature | External Tables | Catalog Linked Database |
|---------|----------------|------------------------|
| Table Creation | Manual DDL in Snowflake | Auto-synced from Glue Catalog |
| Schema Evolution | Manual updates required | Automatic sync on refresh |
| Metadata Sync | Point-in-time | Continuous or on-demand |
| Catalog Integration | GLUE type | ICEBERG_REST type |
| Credential Model | Direct IAM role | Lake Formation vended credentials |
| **Failover Group** | **Can be replicated** | **Cannot be replicated** |
| BCDR Strategy | Replicate via failover group | Create independently on each account |

### Failover Groups Configuration

| Failover Group | Contents | Notes |
|----------------|----------|-------|
| `ICEBERG_BCDR_ACCOUNT_FG` | Roles, Integrations | Includes REST_GLUE_CATALOG_INT |
| `ICEBERG_BCDR_VOLUME_FG` | External Volumes | ICEBERG_EXT_VOLUME |
| `ICEBERG_BCDR_DB_FG` | Databases | **Only ICEBERG_DEMO_EXT** (not CLD!) |

### CLD BCDR Strategy

Since CLDs cannot be replicated, each account creates its own CLD:

```
PRIMARY ACCOUNT                    SECONDARY ACCOUNT
┌──────────────────┐              ┌──────────────────┐
│ ICEBERG_DEMO_CLD │              │ ICEBERG_DEMO_CLD │
│ (script 11)      │              │ (script 30)      │
└────────┬─────────┘              └────────┬─────────┘
         │                                 │
         │   Uses replicated integration   │
         │   REST_GLUE_CATALOG_INT         │
         └────────────┬────────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │  AWS GLUE     │  ← Same catalog
              │  CATALOG      │  ← Same data
              └───────────────┘
```

**Failover behavior for CLD:**
- CLD on secondary is already working (connected to same Glue)
- No "promotion" needed for CLD during failover
- Applications just switch to secondary account
- Data immediately available

## Troubleshooting

### CLD-Specific Issues

**"Forbidden: null" error**
```
SQL Execution Error: Resource on the REST endpoint... Forbidden: null
```
**Solution**: Enable "Allow external engines to access data in Amazon S3 locations with full table access" in Lake Formation → Application integration settings.

**"credential vending not enabled" error**
```
Catalog integration did not have credential vending enabled
```
**Solution**: Use `CATALOG_SOURCE = ICEBERG_REST` with `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS`.

**CLD on secondary not working**
```
sts:AssumeRole not authorized
```
**Solution**: Add the SECONDARY account's Snowflake IAM user ARN to the AWS IAM role trust policy. Each Snowflake account has a different IAM user ARN.

### Validation Queries

```sql
-- Check CLD link status
SELECT SYSTEM$CATALOG_LINK_STATUS('ICEBERG_DEMO_CLD');

-- Compare row counts between EXT and CLD
SELECT 'EXT' as source, COUNT(*) FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CAMPAIGNS
UNION ALL
SELECT 'CLD' as source, COUNT(*) FROM ICEBERG_DEMO_CLD.ICEBERG_ADVERTISING_DB.CAMPAIGNS;

-- Check failover group status
SHOW FAILOVER GROUPS;
```

## Cleanup

```sql
-- Run cleanup script to remove all demo objects
-- (scripts/99_cleanup.sql)
```

## References

- [Snowflake Iceberg Tables Documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [Catalog Linked Databases](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)
- [Failover Groups](https://docs.snowflake.com/en/user-guide/account-failover-groups)
- [AWS Glue Integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-glue)
- [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
