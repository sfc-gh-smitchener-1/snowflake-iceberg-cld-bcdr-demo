#!/usr/bin/env python3
"""
ICEBERG CLD BCDR DEMO
Script: 03_load_iceberg_aws.py
Purpose: Load generated advertising data into Iceberg tables in AWS (S3 + Glue)

This script:
1. Creates an AWS Glue database if it doesn't exist
2. Uploads Parquet data to S3
3. Creates Iceberg tables in Glue Data Catalog
4. Validates the data is accessible

Prerequisites:
- AWS CLI configured with appropriate credentials
- S3 bucket created
- IAM permissions for S3 and Glue

Usage:
    python 03_load_iceberg_aws.py \
        --data-dir ./data \
        --bucket <YOUR_S3_BUCKET> \
        --glue-database iceberg_advertising_db \
        --region <YOUR_AWS_REGION>

Environment Variables (alternative to CLI args):
    AWS_S3_BUCKET, AWS_REGION, AWS_GLUE_DATABASE
"""

import os
import sys
from pathlib import Path
from typing import Optional

import click
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()

# Schema definitions for Iceberg tables
SCHEMAS = {
    'campaigns': {
        'columns': [
            ('campaign_id', 'string'),
            ('campaign_name', 'string'),
            ('channel', 'string'),
            ('ad_format', 'string'),
            ('budget_usd', 'double'),
            ('daily_budget_usd', 'double'),
            ('target_cpa_usd', 'double'),
            ('target_region', 'string'),
            ('status', 'string'),
            ('start_date', 'string'),
            ('end_date', 'string'),
            ('advertiser_name', 'string'),
            ('advertiser_industry', 'string'),
            ('created_at', 'string'),
            ('updated_at', 'string'),
        ],
        'partition_keys': []
    },
    'impressions': {
        'columns': [
            ('impression_id', 'string'),
            ('campaign_id', 'string'),
            ('timestamp', 'string'),
            ('date_key', 'string'),
            ('hour', 'int'),
            ('device_type', 'string'),
            ('geo_region', 'string'),
            ('geo_country', 'string'),
            ('geo_city', 'string'),
            ('browser', 'string'),
            ('os', 'string'),
            ('ad_position', 'string'),
            ('viewable', 'boolean'),
            ('cost_usd', 'double'),
            ('publisher_id', 'string'),
            ('placement_id', 'string'),
        ],
        'partition_keys': ['date_key']
    },
    'clicks': {
        'columns': [
            ('click_id', 'string'),
            ('impression_id', 'string'),
            ('campaign_id', 'string'),
            ('timestamp', 'string'),
            ('date_key', 'string'),
            ('device_type', 'string'),
            ('geo_region', 'string'),
            ('geo_country', 'string'),
            ('landing_page_url', 'string'),
            ('referrer_url', 'string'),
            ('time_on_site_seconds', 'int'),
            ('pages_viewed', 'int'),
            ('bounce', 'boolean'),
            ('cost_usd', 'double'),
        ],
        'partition_keys': ['date_key']
    },
    'conversions': {
        'columns': [
            ('conversion_id', 'string'),
            ('click_id', 'string'),
            ('impression_id', 'string'),
            ('campaign_id', 'string'),
            ('timestamp', 'string'),
            ('date_key', 'string'),
            ('conversion_type', 'string'),
            ('revenue_usd', 'double'),
            ('quantity', 'int'),
            ('currency', 'string'),
            ('order_id', 'string'),
            ('product_category', 'string'),
            ('new_customer', 'boolean'),
            ('device_type', 'string'),
            ('geo_region', 'string'),
            ('attribution_model', 'string'),
        ],
        'partition_keys': ['date_key']
    }
}


def get_glue_column_type(pandas_type: str) -> str:
    """Map pandas/python types to Glue/Iceberg types."""
    type_mapping = {
        'string': 'string',
        'int': 'int',
        'double': 'double',
        'boolean': 'boolean',
        'timestamp': 'timestamp',
    }
    return type_mapping.get(pandas_type, 'string')


def create_glue_database(glue_client, database_name: str, description: str = None):
    """Create Glue database if it doesn't exist."""
    try:
        glue_client.get_database(Name=database_name)
        console.print(f"[yellow]Database '{database_name}' already exists[/]")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        pass
    
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': description or f'Iceberg advertising database for BCDR demo'
            }
        )
        console.print(f"[green]✓ Created database '{database_name}'[/]")
        return True
    except ClientError as e:
        console.print(f"[red]Error creating database: {e}[/]")
        return False


def upload_to_s3(s3_client, local_path: Path, bucket: str, s3_prefix: str) -> bool:
    """Upload a file to S3."""
    s3_key = f"{s3_prefix}/{local_path.name}"
    try:
        s3_client.upload_file(str(local_path), bucket, s3_key)
        return True
    except ClientError as e:
        console.print(f"[red]Error uploading {local_path.name}: {e}[/]")
        return False


def convert_csv_to_parquet(csv_path: Path, parquet_path: Path, table_name: str) -> bool:
    """Convert CSV to Parquet with proper schema."""
    try:
        df = pd.read_csv(csv_path)
        
        # Ensure proper types
        schema_def = SCHEMAS.get(table_name, {})
        for col_name, col_type in schema_def.get('columns', []):
            if col_name in df.columns:
                if col_type == 'boolean':
                    df[col_name] = df[col_name].astype(bool)
                elif col_type == 'int':
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
                elif col_type == 'double':
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0)
        
        df.to_parquet(parquet_path, index=False, engine='pyarrow')
        return True
    except Exception as e:
        console.print(f"[red]Error converting {csv_path.name}: {e}[/]")
        return False


def create_iceberg_table(
    glue_client,
    database_name: str,
    table_name: str,
    s3_location: str,
    schema_def: dict
) -> bool:
    """Create an Iceberg table in Glue Data Catalog."""
    
    # Build column definitions
    columns = [
        {'Name': col_name, 'Type': get_glue_column_type(col_type)}
        for col_name, col_type in schema_def['columns']
        if col_name not in schema_def.get('partition_keys', [])
    ]
    
    # Build partition keys
    partition_keys = [
        {'Name': pk, 'Type': 'string'}
        for pk in schema_def.get('partition_keys', [])
    ]
    
    table_input = {
        'Name': table_name,
        'Description': f'Iceberg table for {table_name} advertising data',
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'table_type': 'ICEBERG',
            'metadata_location': f'{s3_location}/metadata/00000-00000000-0000-0000-0000-000000000000.metadata.json',
            'format': 'parquet',
        },
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.iceberg.mr.hive.HiveIcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.hive.HiveIcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe'
            }
        }
    }
    
    if partition_keys:
        table_input['PartitionKeys'] = partition_keys
    
    try:
        # Delete if exists
        try:
            glue_client.delete_table(DatabaseName=database_name, Name=table_name)
        except glue_client.exceptions.EntityNotFoundException:
            pass
        
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        return True
    except ClientError as e:
        console.print(f"[red]Error creating table {table_name}: {e}[/]")
        return False


@click.command()
@click.option(
    '--data-dir', '-d',
    type=click.Path(exists=True),
    required=True,
    help='Directory containing generated CSV files'
)
@click.option(
    '--bucket', '-b',
    type=str,
    required=True,
    help='S3 bucket name for Iceberg data'
)
@click.option(
    '--glue-database', '-g',
    type=str,
    default='iceberg_advertising_db',
    help='Glue database name'
)
@click.option(
    '--region', '-r',
    type=str,
    default=os.environ.get('AWS_REGION'),
    help='AWS region (or set AWS_REGION env var)'
)
@click.option(
    '--s3-prefix', '-p',
    type=str,
    default='iceberg/advertising',
    help='S3 prefix for data storage'
)
@click.option(
    '--profile',
    type=str,
    default=None,
    help='AWS profile name (optional)'
)
def main(
    data_dir: str,
    bucket: str,
    glue_database: str,
    region: str,
    s3_prefix: str,
    profile: Optional[str]
):
    """Load advertising data into Iceberg tables in AWS."""
    
    console.print("\n[bold blue]═══════════════════════════════════════════════════════════════[/]")
    console.print("[bold blue]       ICEBERG CLD BCDR DEMO - AWS Data Loader[/]")
    console.print("[bold blue]═══════════════════════════════════════════════════════════════[/]\n")
    
    data_path = Path(data_dir)
    
    console.print(f"[cyan]Configuration:[/]")
    console.print(f"  • Data directory: {data_path.absolute()}")
    console.print(f"  • S3 bucket: s3://{bucket}/{s3_prefix}/")
    console.print(f"  • Glue database: {glue_database}")
    console.print(f"  • Region: {region}\n")
    
    # Initialize AWS clients
    session_kwargs = {'region_name': region}
    if profile:
        session_kwargs['profile_name'] = profile
    
    session = boto3.Session(**session_kwargs)
    s3_client = session.client('s3')
    glue_client = session.client('glue')
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        # Create Glue database
        task = progress.add_task("[cyan]Creating Glue database...", total=None)
        if create_glue_database(glue_client, glue_database):
            progress.update(task, completed=True, description="[green]✓ Glue database ready")
        else:
            progress.update(task, completed=True, description="[red]✗ Failed to create database")
            return
        
        # Process each table
        tables = ['campaigns', 'impressions', 'clicks', 'conversions']
        
        for table_name in tables:
            csv_file = data_path / f'{table_name}.csv'
            
            if not csv_file.exists():
                console.print(f"[yellow]⚠ Skipping {table_name}: CSV file not found[/]")
                continue
            
            # Convert to Parquet
            task = progress.add_task(f"[cyan]Converting {table_name} to Parquet...", total=None)
            parquet_file = data_path / f'{table_name}.parquet'
            if convert_csv_to_parquet(csv_file, parquet_file, table_name):
                progress.update(task, completed=True, description=f"[green]✓ Converted {table_name}")
            else:
                progress.update(task, completed=True, description=f"[red]✗ Failed to convert {table_name}")
                continue
            
            # Upload to S3
            task = progress.add_task(f"[cyan]Uploading {table_name} to S3...", total=None)
            s3_table_prefix = f"{s3_prefix}/{table_name}/data"
            if upload_to_s3(s3_client, parquet_file, bucket, s3_table_prefix):
                progress.update(task, completed=True, description=f"[green]✓ Uploaded {table_name}")
            else:
                progress.update(task, completed=True, description=f"[red]✗ Failed to upload {table_name}")
                continue
            
            # Create Glue table
            task = progress.add_task(f"[cyan]Creating Glue table {table_name}...", total=None)
            s3_location = f"s3://{bucket}/{s3_prefix}/{table_name}"
            if create_iceberg_table(glue_client, glue_database, table_name, s3_location, SCHEMAS[table_name]):
                progress.update(task, completed=True, description=f"[green]✓ Created table {table_name}")
            else:
                progress.update(task, completed=True, description=f"[red]✗ Failed to create table {table_name}")
    
    # Summary
    console.print("\n[bold green]═══════════════════════════════════════════════════════════════[/]")
    console.print("[bold green]                    Loading Complete![/]")
    console.print("[bold green]═══════════════════════════════════════════════════════════════[/]\n")
    
    # Show table info
    table = Table(title="Iceberg Tables in Glue Catalog")
    table.add_column("Table", style="cyan")
    table.add_column("S3 Location", style="green")
    table.add_column("Partition Keys", style="yellow")
    
    for table_name in tables:
        partition_keys = ', '.join(SCHEMAS[table_name].get('partition_keys', [])) or 'None'
        table.add_row(
            table_name,
            f"s3://{bucket}/{s3_prefix}/{table_name}/",
            partition_keys
        )
    
    console.print(table)
    
    console.print(f"\n[cyan]Glue Catalog:[/] {glue_database}")
    console.print(f"[cyan]S3 Base Path:[/] s3://{bucket}/{s3_prefix}/")
    
    console.print("\n[yellow]Next steps:[/]")
    console.print("  1. Verify AWS IAM trust policy is configured")
    console.print("  2. Run 10_external_tables.sql in Snowflake")
    console.print("  3. Run 11_catalog_linked_database.sql in Snowflake\n")


if __name__ == '__main__':
    main()

