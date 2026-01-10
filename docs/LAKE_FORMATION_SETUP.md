# AWS Lake Formation Setup for Snowflake CLD

This guide covers the required AWS Lake Formation configuration for Snowflake Catalog Linked Database (CLD) to work with AWS Glue-managed Iceberg tables.

## Overview

Snowflake CLD with AWS Glue requires Lake Formation to **vend credentials** for S3 access. This is different from standard Snowflake integrations that use direct IAM role assumption.

## Prerequisites

- AWS Account with Lake Formation enabled
- S3 bucket with Iceberg tables
- Glue Data Catalog with Iceberg table definitions
- IAM role for Snowflake access

## Step-by-Step Configuration

### Step 1: Register S3 Location in Lake Formation

Lake Formation must know about the S3 locations containing your Iceberg data.

**Console:**
1. Go to **AWS Console → Lake Formation → Data lake locations**
2. Click **Register location**
3. Enter:
   - **Amazon S3 path**: `s3://<your-bucket>/iceberg/warehouse/`
   - **IAM role**: Select your Snowflake IAM role
4. Click **Register location**

**CLI:**
```bash
aws lakeformation register-resource \
    --resource-arn arn:aws:s3:::<your-bucket>/iceberg/warehouse \
    --role-arn arn:aws:iam::<account-id>:role/<your-role> \
    --region <your-region>
```

### Step 2: Add IAM Role as Data Lake Administrator

**Console:**
1. Go to **Lake Formation → Administrative roles and tasks**
2. Under **Data lake administrators**, click **Grant**
3. Select **IAM users and roles**
4. Add your Snowflake IAM role
5. Click **Grant**

**CLI:**
```bash
aws lakeformation put-data-lake-settings \
    --data-lake-settings '{
        "DataLakeAdmins": [
            {"DataLakePrincipalIdentifier": "arn:aws:iam::<account-id>:role/<your-role>"}
        ]
    }' \
    --region <your-region>
```

### Step 3: Grant Database and Table Permissions

**Console:**
1. Go to **Lake Formation → Data permissions → Grant**
2. **Principals**: Select your IAM role
3. **LF-Tags or catalog resources**: Choose **Named Data Catalog resources**
4. **Databases**: Select `iceberg_advertising_db`
5. **Tables**: All tables (or specific tables)
6. **Table permissions**: Select `Select`, `Describe`
7. **Grantable permissions**: Check `Select`, `Describe`
8. Click **Grant**

**CLI:**
```bash
# Grant database permissions
aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::<account-id>:role/<your-role> \
    --resource '{"Database":{"Name":"iceberg_advertising_db"}}' \
    --permissions "DESCRIBE" \
    --region <your-region>

# Grant table permissions
aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::<account-id>:role/<your-role> \
    --resource '{"Table":{"DatabaseName":"iceberg_advertising_db","TableWildcard":{}}}' \
    --permissions "SELECT" "DESCRIBE" \
    --region <your-region>
```

### Step 4: Grant Data Location Access

**CLI:**
```bash
aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::<account-id>:role/<your-role> \
    --resource '{"DataLocation":{"ResourceArn":"arn:aws:s3:::<your-bucket>/iceberg/warehouse"}}' \
    --permissions "DATA_LOCATION_ACCESS" \
    --region <your-region>
```

### Step 5: Enable External Engine Access (CRITICAL!)

This is the most commonly missed step and causes "Forbidden: null" errors.

**Console:**
1. Go to **Lake Formation → Application integration settings**
2. Check the box: **"Allow external engines to access data in Amazon S3 locations with full table access"**
3. Click **Save**

**CLI:**
```bash
aws lakeformation put-data-lake-settings \
    --data-lake-settings '{
        "AllowExternalDataFiltering": true,
        "AllowFullTableExternalDataAccess": true,
        "ExternalDataFilteringAllowList": [
            {"DataLakePrincipalIdentifier": "arn:aws:iam::<account-id>:role/<your-role>"}
        ]
    }' \
    --region <your-region>
```

## IAM Role Configuration

### Trust Policy

The IAM role must trust both Snowflake and Lake Formation:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<SNOWFLAKE_IAM_USER_ARN>"
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

Get the Snowflake IAM user ARN from:
```sql
DESCRIBE INTEGRATION AWS_ICEBERG_STORAGE_INT;
-- Look for STORAGE_AWS_IAM_USER_ARN
```

### Permissions Policy

The role needs S3, Glue, and Lake Formation permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::<your-bucket>",
                "arn:aws:s3:::<your-bucket>/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetTableVersion",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:BatchGetPartition"
            ],
            "Resource": [
                "arn:aws:glue:<region>:<account-id>:catalog",
                "arn:aws:glue:<region>:<account-id>:database/*",
                "arn:aws:glue:<region>:<account-id>:table/*/*"
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

## Troubleshooting

### Error: "Forbidden: null"

**Cause:** Lake Formation Application integration settings not configured.

**Fix:** Enable "Allow external engines to access data in Amazon S3 locations with full table access" in Lake Formation → Application integration settings.

### Error: "Unable to assume role"

**Cause:** IAM role trust policy doesn't include Lake Formation service.

**Fix:** Add this to the trust policy:
```json
{
    "Effect": "Allow",
    "Principal": {"Service": "lakeformation.amazonaws.com"},
    "Action": "sts:AssumeRole"
}
```

### Error: "lakeformation:GetDataAccess not authorized"

**Cause:** IAM role missing Lake Formation permission.

**Fix:** Add `lakeformation:GetDataAccess` to the role's permissions policy.

### Error: "Table is not initialized"

**Cause:** Tables synced but can't read metadata.

**Fix:** 
1. Verify S3 location is registered in Lake Formation
2. Verify data permissions are granted
3. Try `ALTER DATABASE <db> REFRESH;`

### Error: "Credential vending not enabled"

**Cause:** Using wrong catalog integration type.

**Fix:** Use `CATALOG_SOURCE = ICEBERG_REST` with `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS`, not `CATALOG_SOURCE = GLUE`.

## Verification Checklist

- [ ] S3 location registered in Lake Formation
- [ ] IAM role added as Data Lake Administrator
- [ ] Database permissions granted (Describe)
- [ ] Table permissions granted (Select, Describe)
- [ ] Data location access granted
- [ ] **Application integration settings enabled for external engines**
- [ ] IAM trust policy includes Lake Formation service
- [ ] IAM permissions policy includes `lakeformation:GetDataAccess`
- [ ] REST catalog integration uses `VENDED_CREDENTIALS` mode

## References

- [Snowflake Catalog Linked Database](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)
- [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html)
- [Glue Iceberg REST API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)

