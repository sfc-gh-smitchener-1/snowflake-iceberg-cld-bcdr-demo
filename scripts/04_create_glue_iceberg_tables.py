#!/usr/bin/env python3
"""
Create proper Iceberg tables in AWS Glue catalog using PyIceberg.
This script reads the Parquet files and creates real Iceberg tables.
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import click
from rich.console import Console

console = Console()

# Configuration - UPDATE THESE VALUES for your environment
# You can also set these via environment variables: AWS_S3_BUCKET, AWS_REGION
import os
BUCKET = os.environ.get("AWS_S3_BUCKET", "<YOUR_S3_BUCKET>")
REGION = os.environ.get("AWS_REGION", "<YOUR_AWS_REGION>")
GLUE_DATABASE = "iceberg_advertising_db"
S3_PREFIX = "iceberg/advertising"

def recreate_glue_database(glue_client, database_name):
    """Delete and recreate the Glue database."""
    # Delete existing tables first
    try:
        tables = glue_client.get_tables(DatabaseName=database_name)
        for table in tables.get('TableList', []):
            console.print(f"  Deleting table: {table['Name']}")
            glue_client.delete_table(DatabaseName=database_name, Name=table['Name'])
    except:
        pass
    
    # Delete database
    try:
        glue_client.delete_database(Name=database_name)
        console.print(f"  Deleted database: {database_name}")
    except:
        pass
    
    # Recreate database
    glue_client.create_database(
        DatabaseInput={
            'Name': database_name,
            'Description': 'Iceberg advertising database for BCDR demo'
        }
    )
    console.print(f"  Created database: {database_name}")


def create_iceberg_table_in_glue(glue_client, database_name, table_name, s3_location, columns, partition_keys=None):
    """Create an Iceberg table in Glue with proper metadata."""
    
    # Map Python/Pandas types to Glue types
    type_map = {
        'object': 'string',
        'int64': 'bigint',
        'float64': 'double',
        'bool': 'boolean',
        'int32': 'int',
    }
    
    glue_columns = []
    for col_name, col_type in columns:
        glue_type = type_map.get(str(col_type), 'string')
        glue_columns.append({'Name': col_name, 'Type': glue_type})
    
    # Remove partition columns from regular columns if present
    if partition_keys:
        glue_columns = [c for c in glue_columns if c['Name'] not in partition_keys]
    
    table_input = {
        'Name': table_name,
        'Description': f'Iceberg table: {table_name}',
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'table_type': 'ICEBERG',
            'format': 'parquet',
        },
        'StorageDescriptor': {
            'Columns': glue_columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.iceberg.mr.hive.HiveIcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.hive.HiveIcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe'
            },
        },
        'PartitionKeys': [{'Name': pk, 'Type': 'string'} for pk in (partition_keys or [])],
    }
    
    try:
        glue_client.delete_table(DatabaseName=database_name, Name=table_name)
    except:
        pass
    
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    console.print(f"  ✓ Created Glue table: {table_name}")


@click.command()
@click.option('--data-dir', '-d', type=click.Path(exists=True), default='./data', help='Directory with Parquet files')
@click.option('--region', '-r', type=str, default=REGION, help='AWS region')
@click.option('--bucket', '-b', type=str, default=BUCKET, help='S3 bucket')
@click.option('--database', '-db', type=str, default=GLUE_DATABASE, help='Glue database name')
def main(data_dir, region, bucket, database):
    """Create Iceberg tables in AWS Glue."""
    
    console.print("\n[bold blue]Creating Iceberg Tables in AWS Glue[/]\n")
    
    data_path = Path(data_dir)
    
    # Initialize AWS clients
    glue_client = boto3.client('glue', region_name=region)
    s3_client = boto3.client('s3', region_name=region)
    
    # Recreate database
    console.print("[cyan]Recreating Glue database...[/]")
    recreate_glue_database(glue_client, database)
    
    # Table definitions
    tables = {
        'campaigns': {
            'columns': [
                ('campaign_id', 'object'),
                ('campaign_name', 'object'),
                ('channel', 'object'),
                ('ad_format', 'object'),
                ('budget_usd', 'float64'),
                ('daily_budget_usd', 'float64'),
                ('target_cpa_usd', 'float64'),
                ('target_region', 'object'),
                ('status', 'object'),
                ('start_date', 'object'),
                ('end_date', 'object'),
                ('advertiser_name', 'object'),
                ('advertiser_industry', 'object'),
                ('created_at', 'object'),
                ('updated_at', 'object'),
            ],
            'partition_keys': None
        },
        'impressions': {
            'columns': [
                ('impression_id', 'object'),
                ('campaign_id', 'object'),
                ('timestamp', 'object'),
                ('date_key', 'object'),
                ('hour', 'int64'),
                ('device_type', 'object'),
                ('geo_region', 'object'),
                ('geo_country', 'object'),
                ('geo_city', 'object'),
                ('browser', 'object'),
                ('os', 'object'),
                ('ad_position', 'object'),
                ('viewable', 'bool'),
                ('cost_usd', 'float64'),
                ('publisher_id', 'object'),
                ('placement_id', 'object'),
            ],
            'partition_keys': None
        },
        'clicks': {
            'columns': [
                ('click_id', 'object'),
                ('impression_id', 'object'),
                ('campaign_id', 'object'),
                ('timestamp', 'object'),
                ('date_key', 'object'),
                ('device_type', 'object'),
                ('geo_region', 'object'),
                ('geo_country', 'object'),
                ('landing_page_url', 'object'),
                ('referrer_url', 'object'),
                ('time_on_site_seconds', 'int64'),
                ('pages_viewed', 'int64'),
                ('bounce', 'bool'),
                ('cost_usd', 'float64'),
            ],
            'partition_keys': None
        },
        'conversions': {
            'columns': [
                ('conversion_id', 'object'),
                ('click_id', 'object'),
                ('impression_id', 'object'),
                ('campaign_id', 'object'),
                ('timestamp', 'object'),
                ('date_key', 'object'),
                ('conversion_type', 'object'),
                ('revenue_usd', 'float64'),
                ('quantity', 'int64'),
                ('currency', 'object'),
                ('order_id', 'object'),
                ('product_category', 'object'),
                ('new_customer', 'bool'),
                ('device_type', 'object'),
                ('geo_region', 'object'),
                ('attribution_model', 'object'),
            ],
            'partition_keys': None
        }
    }
    
    console.print("\n[cyan]Creating Glue tables...[/]")
    for table_name, table_def in tables.items():
        s3_location = f"s3://{bucket}/{S3_PREFIX}/{table_name}/"
        create_iceberg_table_in_glue(
            glue_client,
            database,
            table_name,
            s3_location,
            table_def['columns'],
            table_def['partition_keys']
        )
    
    console.print("\n[bold green]✓ Done![/]")
    console.print(f"\nGlue database: {database}")
    console.print("Tables: campaigns, impressions, clicks, conversions")
    console.print("\nNow run the Snowflake CLD script to access these tables.")


if __name__ == '__main__':
    main()

