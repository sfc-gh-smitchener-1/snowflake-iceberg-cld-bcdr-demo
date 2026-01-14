#!/usr/bin/env python3
"""
Append new campaigns to the existing Iceberg table using PyIceberg.
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, NestedField
from datetime import datetime, timedelta
import random
from faker import Faker
from rich.console import Console
import click

console = Console()
fake = Faker()

# Configuration - UPDATE THESE VALUES for your environment
# You can also set these via environment variables: AWS_S3_BUCKET, AWS_REGION
import os
BUCKET = os.environ.get("AWS_S3_BUCKET", "<YOUR_S3_BUCKET>")
REGION = os.environ.get("AWS_REGION", "<YOUR_AWS_REGION>")
GLUE_DATABASE = "iceberg_advertising_db"
WAREHOUSE_PATH = f"s3://{BUCKET}/iceberg/warehouse"

# Data generation constants
CHANNELS = ['display', 'search', 'social', 'video', 'native', 'email', 'affiliate']
AD_FORMATS = ['banner', 'interstitial', 'video_preroll', 'video_midroll', 'native_feed', 'carousel']
GEO_REGIONS = ['US-EAST', 'US-WEST', 'US-CENTRAL', 'CANADA', 'UK', 'EU-WEST', 'EU-CENTRAL', 'APAC']
CAMPAIGN_STATUS = ['active', 'paused', 'completed', 'draft']


def generate_new_campaigns(num_campaigns: int, start_id: int = 1000) -> pd.DataFrame:
    """Generate new campaign data with unique IDs."""
    campaigns = []
    now = datetime.now()
    
    for i in range(num_campaigns):
        campaign_id = f'CMP-NEW-{str(start_id + i).zfill(4)}'
        campaign_start = fake.date_between(start_date='-30d', end_date='today')
        campaign_end = campaign_start + timedelta(days=random.randint(7, 90))
        
        campaign = {
            'campaign_id': campaign_id,
            'campaign_name': f'NEW: {fake.catch_phrase()} Campaign',
            'channel': random.choice(CHANNELS),
            'ad_format': random.choice(AD_FORMATS),
            'budget_usd': round(random.uniform(1000, 500000), 2),
            'daily_budget_usd': round(random.uniform(100, 10000), 2),
            'target_cpa_usd': round(random.uniform(5, 100), 2),
            'target_region': random.choice(GEO_REGIONS),
            'status': random.choice(CAMPAIGN_STATUS),
            'start_date': campaign_start.isoformat(),
            'end_date': campaign_end.isoformat(),
            'advertiser_name': fake.company(),
            'advertiser_industry': fake.bs(),
            'created_at': now.isoformat(),
            'updated_at': now.isoformat()
        }
        campaigns.append(campaign)
    
    return pd.DataFrame(campaigns)


@click.command()
@click.option('--num-campaigns', '-n', type=int, default=10, help='Number of campaigns to add')
@click.option('--region', '-r', type=str, default=REGION, help='AWS region')
@click.option('--bucket', '-b', type=str, default=BUCKET, help='S3 bucket')
def main(num_campaigns, region, bucket):
    """Append new campaigns to the Iceberg table."""
    
    console.print(f"\n[bold blue]Appending {num_campaigns} New Campaigns to Iceberg Table[/]\n")
    
    # Generate new campaigns
    console.print("[cyan]Generating new campaign data...[/]")
    df = generate_new_campaigns(num_campaigns)
    console.print(f"  Generated {len(df)} campaigns")
    
    # Show preview
    console.print("\n[cyan]Preview of new campaigns:[/]")
    for _, row in df.head(3).iterrows():
        console.print(f"  {row['campaign_id']}: {row['campaign_name']}")
    
    # Connect to Glue catalog using PyIceberg with warehouse config
    console.print("\n[cyan]Connecting to Glue catalog...[/]")
    try:
        catalog = GlueCatalog(
            name="glue",
            **{
                "region_name": region,
                "s3.region": region,
                "warehouse": WAREHOUSE_PATH,
            }
        )
        console.print("  ✓ Connected to Glue catalog")
        
        # List tables to verify connection
        tables = catalog.list_tables(GLUE_DATABASE)
        console.print(f"  Found tables: {[t[1] for t in tables]}")
        
    except Exception as e:
        console.print(f"[red]Failed to connect to catalog: {e}[/]")
        raise
    
    # Load the campaigns table
    try:
        console.print("\n[cyan]Loading campaigns table...[/]")
        table = catalog.load_table(f"{GLUE_DATABASE}.campaigns")
        console.print(f"  ✓ Loaded table: {GLUE_DATABASE}.campaigns")
        console.print(f"  Location: {table.location()}")
        console.print(f"  Current snapshots: {len(list(table.snapshots()))}")
        
        # Convert to PyArrow table and append
        console.print("\n[cyan]Appending data to Iceberg table...[/]")
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        table.append(arrow_table)
        
        console.print(f"  ✓ Appended {num_campaigns} campaigns")
        console.print(f"  New snapshots: {len(list(table.snapshots()))}")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")
        console.print("\n[yellow]Trying alternative approach - checking table exists...[/]")
        
        # Try listing namespaces and tables
        try:
            namespaces = catalog.list_namespaces()
            console.print(f"  Namespaces: {namespaces}")
            
            for ns in namespaces:
                tables = catalog.list_tables(ns[0] if isinstance(ns, tuple) else ns)
                console.print(f"  Tables in {ns}: {tables}")
        except Exception as e2:
            console.print(f"[red]  Error listing: {e2}[/]")
        
        raise
    
    console.print("\n[bold green]✓ Done![/]")
    console.print(f"\nAdded {num_campaigns} new campaigns with IDs starting with 'CMP-NEW-'")
    console.print("\nTo verify in Snowflake, run:")
    console.print("  SELECT * FROM ICEBERG_DEMO_CLD.iceberg_advertising_db.campaigns")
    console.print("  WHERE campaign_id LIKE 'CMP-NEW-%';")


if __name__ == '__main__':
    main()
