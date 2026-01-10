# Iceberg CLD BCDR Demo - Talk Track

## Demo Overview

This document provides a guided talk track for presenting the Iceberg CLD BCDR demonstration. Use this to walk through the demo with customers, partners, or internal teams.

---

## Introduction (2 minutes)

> "Today I'm going to show you how Snowflake integrates with Apache Iceberg tables managed by AWS Glue, and more importantly, how we achieve business continuity and disaster recovery across multiple Snowflake accounts - all while pointing to the same data."

### Key Messages

1. **Open Table Format**: Iceberg enables true data lakehouse architecture
2. **Dual Access Patterns**: External Tables for control, CLD for automation
3. **Zero-Copy DR**: Failover without data movement
4. **Shared Data Layer**: Multiple accounts, one source of truth

---

## Part 1: The Business Challenge (3 minutes)

> "Let's start with why this matters. Many organizations face these challenges:"

### Pain Points to Highlight

- **Data Silos**: Data locked in proprietary formats
- **Multi-Engine Requirements**: Need Spark, Snowflake, Athena on same data
- **Disaster Recovery**: Traditional DR requires data duplication
- **Cost Concerns**: Storing data multiple times is expensive

> "Iceberg solves the format problem. But how do you get enterprise-grade DR without duplicating petabytes of data? That's what we'll demonstrate today."

---

## Part 2: Architecture Overview (5 minutes)

### Show the Architecture Diagram

> "Here's the architecture we've built. Notice the key design principle: **one copy of data, multiple access points**."

### Walk Through Components

1. **AWS Glue Catalog**
   > "Glue serves as our central metastore. It knows about all our Iceberg tables - their schemas, partitions, and where the data lives in S3."

2. **Amazon S3**
   > "This is where the actual data lives. Parquet files organized by Iceberg's specifications. There's only ONE copy here."

3. **Primary Snowflake Account**
   > "Our primary account connects to Glue via catalog integration. We have two ways to access the tables - External Tables and Catalog Linked Database."

4. **Secondary Snowflake Account**
   > "The secondary account also connects to the SAME Glue catalog and SAME S3 storage. During normal operations, it's in read-only mode, receiving metadata replication from primary."

---

## Part 3: Live Demo - Setup (10 minutes)

### Show Role Hierarchy

```sql
SHOW ROLES LIKE 'ICEBERG%';
```

> "We've implemented a three-tier role model: Admin for account objects, Engineer for database objects, and Analyst for read-only access. This follows Snowflake security best practices."

### Show Integrations

```sql
SHOW INTEGRATIONS;
DESCRIBE INTEGRATION GLUE_CATALOG_INT;
```

> "The catalog integration is the bridge to Glue. Notice it references our Glue database and uses an IAM role for authentication - no credentials stored in Snowflake."

### Show External Volume

```sql
DESCRIBE EXTERNAL VOLUME ICEBERG_EXT_VOLUME;
```

> "The external volume tells Snowflake where to find the Iceberg data in S3."

---

## Part 4: External Tables Approach (5 minutes)

> "Let's look at the first access pattern - External Tables."

### Query External Tables

```sql
USE ROLE ICEBERG_ANALYST;
USE DATABASE ICEBERG_DEMO_EXT;

SELECT COUNT(*) FROM ADVERTISING.EXT_CAMPAIGNS;
SELECT * FROM ADVERTISING.EXT_CAMPAIGNS LIMIT 5;
```

> "These tables have the `EXT_` prefix - that's our naming convention to distinguish them. The DDL was created manually, giving us full control over the table definitions."

### Show Performance View

```sql
SELECT * FROM ADVERTISING.V_CAMPAIGN_PERFORMANCE LIMIT 10;
```

> "We can build views and analytics just like native Snowflake tables. The data is in S3, but the query experience is identical."

---

## Part 5: Catalog Linked Database (5 minutes)

> "Now let's see the modern approach - Catalog Linked Database, or CLD."

### Show CLD Structure

```sql
USE DATABASE ICEBERG_DEMO_CLD;
SHOW TABLES;
```

> "Notice the tables here use the SAME names as in Glue - no prefix. That's because the CLD automatically syncs from the catalog. Add a table in Glue, it appears here automatically."

### Compare Data

```sql
-- Same data, different access pattern
SELECT COUNT(*) FROM ICEBERG_DEMO_EXT.ADVERTISING.EXT_CAMPAIGNS;
SELECT COUNT(*) FROM ICEBERG_DEMO_CLD.PUBLIC.CAMPAIGNS;
```

> "Identical counts - because it's the SAME data. One in S3, accessed two ways in Snowflake."

---

## Part 6: BCDR Configuration (5 minutes)

> "Now let's talk about disaster recovery."

### Show Failover Groups

```sql
SHOW FAILOVER GROUPS;
```

> "We have two failover groups: one for account objects like integrations and roles, another for databases. Both replicate to our secondary account every 10 minutes."

### Check Replication Status

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_HISTORY())
WHERE REPLICATION_GROUP_NAME LIKE 'ICEBERG_BCDR%'
ORDER BY START_TIME DESC
LIMIT 5;
```

> "Replication is continuous and automatic. You can see the history of refreshes here."

---

## Part 7: Failover Scenario (5 minutes)

> "Let's simulate a disaster scenario."

### On Secondary Account

```sql
-- This is on the SECONDARY account
USE ROLE ICEBERG_ANALYST;
USE WAREHOUSE ICEBERG_DEMO_WH_SECONDARY;
USE DATABASE ICEBERG_DEMO_CLD;

-- Query works immediately
SELECT COUNT(*) FROM PUBLIC.CAMPAIGNS;
```

> "The secondary account is already receiving replication. It can query the data right now in read-only mode."

### Simulate Failover (Describe, Don't Execute)

> "If primary fails, we promote the secondary with a simple command:"

```sql
-- DON'T RUN THIS - just show
-- ALTER FAILOVER GROUP ICEBERG_BCDR_DB_FG PRIMARY;
```

> "That single command promotes secondary to primary. Applications switch connection strings, and they're back in business."

---

## Part 8: The Key Insight (3 minutes)

> "Here's what makes this powerful:"

### Draw on Whiteboard or Show Diagram

```
Traditional DR:
  Primary DB ──copy──► Secondary DB ──copy──► DR Site
  (3 copies of data, sync delays, cost)

Iceberg CLD DR:
  Primary SF ──┐
               │──► Same S3/Glue ◄──── Secondary SF
  (1 copy of data, instant failover, low cost)
```

### Key Points

1. **Zero Data Movement**: During failover, we don't move a single byte of data
2. **Instant Recovery**: RTO measured in minutes, not hours
3. **Cost Efficient**: One copy of data, even with full DR
4. **Multi-Engine**: Same data accessible from Spark, Athena, or any Iceberg-compatible tool

---

## Part 9: Comparison Summary (2 minutes)

> "Let me summarize the two approaches we showed:"

| Feature | External Tables | Catalog Linked Database |
|---------|-----------------|------------------------|
| Setup | Manual DDL | Automatic sync |
| Schema Changes | Manual updates | Auto-refresh |
| Control | Full control | Catalog-driven |
| Best For | Stable schemas | Dynamic environments |

> "Most customers use CLD for active development and EXT for production workloads where schema stability is critical."

---

## Part 10: Q&A Prompts

### Common Questions and Answers

**Q: What's the replication lag?**
> "Configurable from 1 minute to 1 hour. We use 10 minutes for this demo, but production workloads often use 1-5 minutes."

**Q: Can I write to Iceberg tables from Snowflake?**
> "Yes, with the appropriate configuration. Snowflake can be the writer of record for Iceberg tables, with other engines reading."

**Q: What about costs?**
> "You pay for Snowflake compute when querying, S3 storage once (not duplicated), and Glue catalog API calls. Often cheaper than traditional approaches because data isn't duplicated."

**Q: Does this work with other catalogs?**
> "Yes, Snowflake supports AWS Glue, Snowflake's own catalog, and other Iceberg catalogs. The pattern applies broadly."

---

## Closing (2 minutes)

> "To summarize what we've demonstrated today:"

1. ✅ Iceberg tables managed by AWS Glue
2. ✅ Dual access patterns: External Tables and CLD
3. ✅ Role-based access control aligned to your organization
4. ✅ Zero-copy disaster recovery with failover groups
5. ✅ Secondary account ready for instant promotion

> "The result is a data lakehouse architecture with enterprise-grade reliability, without the traditional cost and complexity of DR."

### Call to Action

> "Next steps could include:
> - A deeper dive into your specific use case
> - Proof of concept with your data
> - Architecture review for your current DR setup"

---

## Demo Reset

After the demo, reset the environment:

```sql
-- Refresh any altered data
USE ROLE ICEBERG_ADMIN;
ALTER DATABASE ICEBERG_DEMO_CLD REFRESH;

-- Verify everything is in sync
-- Run 90_validation_queries.sql
```

## Tips for Presenters

1. **Practice the flow** - Know the scripts well enough to recover from errors
2. **Have backups** - Pre-run queries in case of connectivity issues
3. **Know the data** - Be ready to explore ad-hoc questions
4. **Time management** - The full demo is 40-45 minutes; have a short version ready
5. **Adapt to audience** - Engineers want SQL; executives want architecture diagrams

